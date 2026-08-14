"""Deterministic stack-frame -> source-file correlation via one prebuilt index."""
import os
import posixpath
from collections import namedtuple
from dataclasses import dataclass, field
from pathlib import Path

from app.core.processing_config import (
    MAX_SOURCE_CONTEXT_FILE_BYTES,
    SOURCE_CONTEXT_CACHE_BYTES,
    SOURCE_CONTEXT_LINES,
)

IGNORED_DIRS = {".git", "node_modules", ".venv", "venv", "dist", "build", "coverage", "__pycache__"}
BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".webp", ".pdf", ".zip", ".tar", ".gz",
    ".bz2", ".7z", ".exe", ".dll", ".so", ".dylib", ".class", ".jar", ".woff", ".woff2",
    ".ttf", ".eot", ".mp4", ".mp3", ".wav", ".bin", ".lock", ".pyc",
}

SourceFile = namedtuple("SourceFile", "relative_path basename extension size")

@dataclass(slots=True)
class SourceIndex:
    root: Path
    by_path: dict[str, SourceFile] = field(default_factory=dict)
    by_suffix: dict[str, list[str]] = field(default_factory=dict)
    by_stem: dict[str, list[str]] = field(default_factory=dict)
    # Many stack frames across many events routinely point at the same hot
    # file; without this, each one re-reads and re-splits it from disk.
    # Bounded by total cached bytes (not entry count) so a handful of large
    # files can't blow memory even though there's no eviction.
    # Keyed by relative_path (str), not an absolute Path object: every
    # correlate_event() call reconstructs `root / relative_path` fresh, and
    # hashing a brand-new Path is surprisingly costly (re-parses and
    # re-normcases the whole string) - a plain string key lets repeat
    # lookups for the same file skip that entirely and skip constructing
    # the Path at all on a cache hit.
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
        # full_path.relative_to(root) was being recomputed via pathlib for
        # every single file, even though every file in this same dirpath
        # shares the identical relative directory prefix - on a directory
        # with many files that was the dominant cost in build_index().
        # Hoisting it to once per directory and joining the (unchanged)
        # per-file components onto it is provably identical - verified
        # directly against the old per-file computation for every path in
        # this repo before relying on it.
        relative_dir = os.path.relpath(dirpath, root_str)
        relative_dir_posix = "" if relative_dir == "." else relative_dir.replace(os.sep, "/")
        for filename in filenames:
            extension = Path(filename).suffix.lower()
            if extension in BINARY_EXTENSIONS:
                continue
            relative_path = f"{relative_dir_posix}/{filename}" if relative_dir_posix else filename
            full_path = Path(dirpath) / filename
            index.by_path[relative_path] = SourceFile(relative_path, filename, extension, full_path.stat().st_size)
            index.by_stem.setdefault(Path(filename).stem, []).append(relative_path)
            parts = relative_path.split("/")
            for start in range(len(parts) - 1):
                index.by_suffix.setdefault("/".join(parts[start + 1 :]), []).append(relative_path)
    return index

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
