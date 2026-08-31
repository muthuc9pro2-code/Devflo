"""Deterministic stack-frame -> source-file correlation via one prebuilt index."""
import json
import logging
import os
import posixpath
from collections import namedtuple
from dataclasses import dataclass, field
from pathlib import Path

from app.core.processing_config import (
    MAX_SOURCE_CONTEXT_FILE_BYTES,
    MAX_SOURCE_FILES,
    MAX_SOURCE_INDEX_MANIFEST_BYTES,
    MAX_SOURCE_INDEX_SUFFIXES_PER_FILE,
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


class SourceIndexLimitError(ValueError):
    """A source tree would amplify into an unreasonably large derived index."""


def _validate_relative_path_for_index(relative_path: str) -> list[str]:
    parts = [part for part in relative_path.split("/") if part]
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

@dataclass(slots=True)
class SourceIndex:
    root: Path
    by_path: dict[str, SourceFile] = field(default_factory=dict)
    by_suffix: dict[str, list[str]] = field(default_factory=dict)
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
    index = SourceIndex(root=root)
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
            parts = _validate_relative_path_for_index(relative_path)
            full_path = Path(dirpath) / filename
            index.by_path[relative_path] = SourceFile(relative_path, filename, extension, full_path.stat().st_size)
            index.by_stem.setdefault(Path(filename).stem, []).append(relative_path)
            # Exact repository-relative matching remains available through by_path.
            # Only the generated suffix variants are bounded.
            first_suffix_start = max(1, len(parts) - MAX_SOURCE_INDEX_SUFFIXES_PER_FILE)
            for start in range(first_suffix_start, len(parts)):
                index.by_suffix.setdefault("/".join(parts[start:]), []).append(relative_path)
    return index


def index_manifest_path(root: Path) -> Path:
    return root.parent / f"{root.name}.index.json"


def save_index_manifest(index: SourceIndex, manifest_path: Path) -> None:
    """Persist the expensive-to-build structural index (by_path/by_suffix/
    by_stem - never the bounded per-file context cache, which stays cheap
    to (re)populate lazily from the already-prepared source tree) as plain
    JSON - a safe, deterministic representation, never pickle/unsafe
    deserialization of user-controlled data. Written atomically (temp file
    + os.replace, which is atomic on POSIX) so a concurrent reader can
    never observe a partially-written file."""
    payload = {
        "by_path": {
            relative_path: [source_file.basename, source_file.extension, source_file.size]
            for relative_path, source_file in index.by_path.items()
        },
        "by_suffix": index.by_suffix,
        "by_stem": index.by_stem,
    }
    serialized = json.dumps(payload)
    encoded = serialized.encode("utf-8")
    if len(encoded) > MAX_SOURCE_INDEX_MANIFEST_BYTES:
        raise SourceIndexLimitError(
            "Source index manifest exceeds the supported size limit"
        )
    temp_path = manifest_path.with_name(manifest_path.name + ".tmp")
    temp_path.write_bytes(encoded)
    os.replace(temp_path, manifest_path)


def load_index_manifest(manifest_path: Path, root: Path) -> "SourceIndex | None":
    """Reconstruct a bounded SourceIndex from a persisted manifest.
    Missing/corrupt/legacy-unbounded manifests are cache misses, never fatal:
    callers may rebuild from the already-prepared canonical source tree.
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
        by_path_payload = payload["by_path"]
        by_suffix = payload["by_suffix"]
        by_stem = payload["by_stem"]

        if not all(
            isinstance(mapping, dict)
            for mapping in (by_path_payload, by_suffix, by_stem)
        ):
            raise TypeError("Source index manifest mappings have an invalid shape")

        if len(by_path_payload) > MAX_SOURCE_FILES:
            logger.warning(
                "Source index manifest %s contains too many files; rebuilding",
                manifest_path,
            )
            return None

        if any(not isinstance(paths, list) for paths in by_suffix.values()):
            raise TypeError("Source index suffix lists have an invalid shape")

        suffix_reference_count = sum(len(paths) for paths in by_suffix.values())
        if suffix_reference_count > (
            MAX_SOURCE_FILES * MAX_SOURCE_INDEX_SUFFIXES_PER_FILE
        ):
            logger.warning(
                "Source index manifest %s contains too many suffix "
                "references; rebuilding",
                manifest_path,
            )
            return None

        if any(not isinstance(paths, list) for paths in by_stem.values()):
            raise TypeError("Source index stem lists have an invalid shape")

        stem_reference_count = sum(len(paths) for paths in by_stem.values())
        if stem_reference_count > MAX_SOURCE_FILES:
            logger.warning(
                "Source index manifest %s contains too many stem "
                "references; rebuilding",
                manifest_path,
            )
            return None

        by_path = {}
        for relative_path, raw_source_file in by_path_payload.items():
            if not isinstance(relative_path, str):
                raise TypeError("Source index path keys must be strings")
            _validate_relative_path_for_index(relative_path)
            basename, extension, size = raw_source_file
            if (
                not isinstance(basename, str)
                or not isinstance(extension, str)
                or not isinstance(size, int)
            ):
                raise TypeError("Source index file metadata has an invalid shape")
            by_path[relative_path] = SourceFile(relative_path, basename, extension, size)

        return SourceIndex(
            root=root,
            by_path=by_path,
            by_suffix=by_suffix,
            by_stem=by_stem,
        )
    except (OSError, ValueError, KeyError, TypeError):
        logger.warning(
            "Could not load source index manifest %s; rebuilding",
            manifest_path,
            exc_info=True,
        )
        return None


def _match_frame(frame, index: SourceIndex, module: str | None) -> dict | None:
    normalized = posixpath.normpath(frame.file.replace("\\", "/")).lstrip("./") if frame.file else None
    if normalized:
        if normalized in index.by_path:
            return _build_match(index, normalized, normalized, frame, "exact")
        parts = normalized.split("/")
        for start in range(1, len(parts)):
            candidates = index.by_suffix.get("/".join(parts[start:]))
            if candidates:
                if len(candidates) != 1:
                    return None  # ambiguous: never fabricate a source location
                method = "basename" if start == len(parts) - 1 else "suffix"
                return _build_match(index, candidates[0], normalized, frame, method)
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
