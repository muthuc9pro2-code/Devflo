"""Deterministic stack-frame -> source-file correlation via one prebuilt index."""
from bisect import bisect_left
import json
import logging
import os
import posixpath
import uuid
from collections import namedtuple
from dataclasses import dataclass, field
from pathlib import Path

from app.core.processing_config import (
    MAX_SOURCE_CONTEXT_FILE_BYTES,
    MAX_SOURCE_FILES,
    MAX_SOURCE_INDEX_MANIFEST_BYTES,
    MAX_SOURCE_PATH_DEPTH,
    MAX_SOURCE_RELATIVE_PATH_BYTES,
    SOURCE_CONTEXT_CACHE_BYTES,
    SOURCE_CONTEXT_LINES,
)

logger = logging.getLogger(__name__)

IGNORED_DIRS = {".git", "node_modules", ".venv", "venv", "dist", "build", "coverage", "__pycache__"}
BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".webp", ".pdf", ".zip", ".tar", ".gz",
    ".bz2", ".7z", ".exe", ".dll", ".so", ".dylib", ".class", ".jar", ".woff", ".woff2",
    ".ttf", ".eot", ".mp4", ".mp3", ".wav", ".bin", ".lock", ".pyc",
}

SourceFile = namedtuple("SourceFile", "relative_path basename extension size")

# v1 persisted generated suffix/stem maps. v2 persists only canonical by_path
# metadata. Derived lookup structures are reconstructed in memory so old or
# corrupt derived maps can never change source-correlation semantics.
_SOURCE_INDEX_MANIFEST_VERSION = 2
_LEGACY_SOURCE_INDEX_MANIFEST_VERSIONS = {None, 1}
_SUFFIX_COMPONENT_SEPARATOR = "\x00"


class SourceIndexLimitError(ValueError):
    """A source tree would amplify into an unreasonably large derived index."""


def _validate_relative_path_for_index(relative_path: str) -> list[str]:
    if not isinstance(relative_path, str) or "\x00" in relative_path:
        raise SourceIndexLimitError("Unsafe source path in index")
    normalized = posixpath.normpath(relative_path)
    if (
        not relative_path
        or normalized in {".", ".."}
        or normalized.startswith("../")
        or normalized.startswith("/")
        or normalized != relative_path
    ):
        raise SourceIndexLimitError("Unsafe source path in index")
    parts = relative_path.split("/")
    if len(parts) > MAX_SOURCE_PATH_DEPTH:
        raise SourceIndexLimitError(
            "Source path depth exceeds the supported index limit"
        )
    if (
        len(relative_path.encode("utf-8", errors="surrogatepass"))
        > MAX_SOURCE_RELATIVE_PATH_BYTES
    ):
        raise SourceIndexLimitError(
            "Source path length exceeds the supported index limit"
        )
    return parts


def _reversed_path_key(parts: list[str]) -> str:
    return _SUFFIX_COMPONENT_SEPARATOR.join(reversed(parts)) + _SUFFIX_COMPONENT_SEPARATOR


def _derived_maps_from_paths(
    by_path: dict[str, SourceFile],
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    """Build bounded lookup structures from canonical source paths.

    Exactly one reversed lookup key is retained per source file. Prefix
    searches over those keys reproduce the old exhaustive proper-suffix index
    without storing every materialized suffix.
    """
    suffix_entries: list[tuple[str, str]] = []
    by_stem: dict[str, list[str]] = {}
    for relative_path, source_file in by_path.items():
        parts = relative_path.split("/")
        suffix_entries.append((_reversed_path_key(parts), relative_path))
        by_stem.setdefault(Path(source_file.basename).stem, []).append(relative_path)
    suffix_entries.sort(key=lambda item: item[0])
    return (
        [key for key, _path in suffix_entries],
        [path for _key, path in suffix_entries],
        by_stem,
    )

@dataclass(slots=True)
class SourceIndex:
    root: Path
    by_path: dict[str, SourceFile] = field(default_factory=dict)
    _suffix_keys: list[str] = field(default_factory=list, repr=False)
    _suffix_paths: list[str] = field(default_factory=list, repr=False)
    by_stem: dict[str, list[str]] = field(default_factory=dict)
    _context_cache: dict[str, list[str] | None] = field(
        default_factory=dict, repr=False
    )
    _context_cache_bytes: int = field(default=0, repr=False)

    def context_lines(self, relative_path: str) -> list[str] | None:
        cached = self._context_cache.get(relative_path, _MISSING)
        if cached is not _MISSING:
            return cached

        lines: list[str] | None
        try:
            path = self.root / relative_path
            size = path.stat().st_size
            if size > MAX_SOURCE_CONTEXT_FILE_BYTES:
                lines = None
            else:
                lines = path.read_text(errors="replace").splitlines()
        except OSError:
            lines = None

        if self._context_cache_bytes < SOURCE_CONTEXT_CACHE_BYTES:
            self._context_cache[relative_path] = lines
            if lines is not None:
                self._context_cache_bytes += sum(
                    len(line.encode("utf-8", errors="replace")) for line in lines
                )
        return lines


_MISSING = object()

def build_index(root: Path) -> SourceIndex:
    by_path: dict[str, SourceFile] = {}
    root_str = str(root)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in IGNORED_DIRS]
        if not filenames:
            continue
        relative_dir = os.path.relpath(dirpath, root_str)
        relative_dir_posix = "" if relative_dir == "." else relative_dir.replace(os.sep, "/")
        for filename in filenames:
            extension = Path(filename).suffix.lower()
            if extension in BINARY_EXTENSIONS:
                continue
            relative_path = f"{relative_dir_posix}/{filename}" if relative_dir_posix else filename
            _validate_relative_path_for_index(relative_path)
            if len(by_path) >= MAX_SOURCE_FILES:
                raise SourceIndexLimitError(
                    "Source file count exceeds the supported index limit"
                )
            full_path = Path(dirpath) / filename
            by_path[relative_path] = SourceFile(
                relative_path, filename, extension, full_path.stat().st_size
            )
    suffix_keys, suffix_paths, by_stem = _derived_maps_from_paths(by_path)
    return SourceIndex(
        root=root,
        by_path=by_path,
        _suffix_keys=suffix_keys,
        _suffix_paths=suffix_paths,
        by_stem=by_stem,
    )


def index_manifest_path(root: Path) -> Path:
    return root.parent / f"{root.name}.index.json"


def save_index_manifest(index: SourceIndex, manifest_path: Path) -> None:
    """Persist only canonical bounded source-file metadata as safe JSON.

    Derived lookup structures are reconstructed from by_path. Each writer gets
    its own source-tree-owned temporary file before atomically replacing the
    sibling manifest.

    Keeping the writer temp inside index.root is important for terminal
    cleanup: if cancellation/failure removes the source tree while a stale
    manifest refresh is in flight, that stale writer cannot recreate the
    sibling manifest after cleanup has already won.

    The .pyc suffix also keeps implementation-owned writer temps invisible to
    concurrent build_index() walks because .pyc is already excluded from the
    source index.
    """
    payload = {
        "version": _SOURCE_INDEX_MANIFEST_VERSION,
        "by_path": {
            relative_path: [source_file.basename, source_file.extension, source_file.size]
            for relative_path, source_file in index.by_path.items()
        },
    }
    encoded = json.dumps(payload).encode("utf-8")
    if len(encoded) > MAX_SOURCE_INDEX_MANIFEST_BYTES:
        raise SourceIndexLimitError(
            "Source index manifest exceeds the supported size limit"
        )
    temp_path = index.root / f".devflo-index-manifest-{os.getpid()}-{uuid.uuid4().hex}.pyc"
    try:
        temp_path.write_bytes(encoded)
        os.replace(temp_path, manifest_path)
    finally:
        temp_path.unlink(missing_ok=True)


def _source_file_from_manifest(relative_path: str, raw_source_file) -> SourceFile:
    if not isinstance(relative_path, str):
        raise TypeError("Source index path keys must be strings")
    _validate_relative_path_for_index(relative_path)
    if not isinstance(raw_source_file, (list, tuple)) or len(raw_source_file) != 3:
        raise TypeError("Source index file metadata has an invalid shape")
    basename, extension, size = raw_source_file
    expected_basename = Path(relative_path).name
    expected_extension = Path(expected_basename).suffix.lower()
    if (
        not isinstance(basename, str)
        or not isinstance(extension, str)
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
        or basename != expected_basename
        or extension != expected_extension
    ):
        raise TypeError("Source index file metadata has an invalid shape")
    return SourceFile(relative_path, basename, extension, size)


def load_index_manifest(manifest_path: Path, root: Path) -> "SourceIndex | None":
    """Load canonical metadata and rebuild bounded lookup structures.

    Unversioned/v1 manifests remain safe to adopt because only their validated
    by_path metadata is trusted; their old suffix/stem maps are ignored.
    Missing, corrupt, oversized, or unsupported manifests remain cache misses.
    """
    try:
        if manifest_path.stat().st_size > MAX_SOURCE_INDEX_MANIFEST_BYTES:
            logger.warning(
                "Source index manifest %s exceeds the supported size limit; "
                "rebuilding",
                manifest_path,
            )
            return None

        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("Source index manifest must be an object")

        version = payload.get("version")
        if (
            version != _SOURCE_INDEX_MANIFEST_VERSION
            and version not in _LEGACY_SOURCE_INDEX_MANIFEST_VERSIONS
        ):
            logger.warning(
                "Source index manifest %s uses an unsupported format version; "
                "rebuilding",
                manifest_path,
            )
            return None

        by_path_payload = payload["by_path"]
        if not isinstance(by_path_payload, dict):
            raise TypeError("Source index by_path mapping has an invalid shape")

        if len(by_path_payload) > MAX_SOURCE_FILES:
            logger.warning(
                "Source index manifest %s contains too many files; rebuilding",
                manifest_path,
            )
            return None

        by_path: dict[str, SourceFile] = {}
        for relative_path, raw_source_file in by_path_payload.items():
            by_path[relative_path] = _source_file_from_manifest(
                relative_path, raw_source_file
            )

        suffix_keys, suffix_paths, by_stem = _derived_maps_from_paths(by_path)
        return SourceIndex(
            root=root,
            by_path=by_path,
            _suffix_keys=suffix_keys,
            _suffix_paths=suffix_paths,
            by_stem=by_stem,
        )
    except (OSError, ValueError, KeyError, TypeError):
        logger.warning(
            "Could not load source index manifest %s; rebuilding",
            manifest_path,
            exc_info=True,
        )
        return None


def _lookup_proper_suffix(
    index: SourceIndex, suffix_parts: list[str]
) -> tuple[str | None, bool]:
    """Return (unique_path, ambiguous) for one requested proper suffix.

    The reversed-key prefix represents all repository paths ending in the
    requested component sequence. A key exactly equal to the prefix represents
    a source file whose COMPLETE relative path equals that suffix; the original
    exhaustive index deliberately did not register a path as its own suffix,
    so that entry is skipped.
    """
    prefix = _reversed_path_key(suffix_parts)
    position = bisect_left(index._suffix_keys, prefix)
    candidate: str | None = None
    while (
        position < len(index._suffix_keys)
        and index._suffix_keys[position].startswith(prefix)
    ):
        key = index._suffix_keys[position]
        if len(key) > len(prefix):
            if candidate is not None:
                return None, True
            candidate = index._suffix_paths[position]
        position += 1
    return candidate, False


def _match_frame(frame, index: SourceIndex, module: str | None) -> dict | None:
    normalized = posixpath.normpath(frame.file.replace("\\", "/")).lstrip("./") if frame.file else None
    if normalized:
        if normalized in index.by_path:
            return _build_match(index, normalized, normalized, frame, "exact")
        parts = normalized.split("/")
        # Preserve the original exhaustive-suffix algorithm exactly:
        # requested proper suffixes are examined longest -> shortest.
        for start in range(1, len(parts)):
            candidate, ambiguous = _lookup_proper_suffix(index, parts[start:])
            if ambiguous:
                # Never fabricate a source location.
                return None
            if candidate is not None:
                method = "basename" if start == len(parts) - 1 else "suffix"
                return _build_match(index, candidate, normalized, frame, method)
    stem = (module or "").rsplit(".", 1)[-1] or None
    candidates = index.by_stem.get(stem) if stem else None
    if candidates and len(candidates) == 1:
        return _build_match(index, candidates[0], normalized or stem, frame, "module")
    return None

def _build_match(index: SourceIndex, relative_path: str, requested_path: str, frame, method: str) -> dict:
    line_number = getattr(frame, "line", None)
    snippet, start, end = _read_context(index, relative_path, line_number)
    return {
        "relative_path": relative_path,
        "requested_path": requested_path,
        "line_number": line_number,
        "function": getattr(frame, "function", None),
        "context_start": start,
        "context_end": end,
        "snippet": snippet,
        "match_method": method,
        "confidence": "high" if method == "exact" else "medium",
    }

def _read_context(index: SourceIndex, relative_path: str, line_number: int | None) -> tuple[str | None, int | None, int | None]:
    if not line_number or line_number < 1:
        return None, None, None
    lines = index.context_lines(relative_path)
    if lines is None:
        return None, None, None
    start = max(line_number - SOURCE_CONTEXT_LINES, 1)
    end = min(line_number + SOURCE_CONTEXT_LINES, len(lines))
    if start > end:
        return None, None, None
    return "\n".join(lines[start - 1 : end]), start, end

def correlate_event(event, index: SourceIndex | None) -> list[dict]:
    if index is None:
        return []
    module = getattr(event, "module", None)
    frames = getattr(event, "stack_frames", None) or []
    return [match for frame in frames if (match := _match_frame(frame, index, module))]
