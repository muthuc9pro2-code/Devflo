"""Interleaved A/B: old per-file Path.relative_to() vs new per-directory
hoisted os.path.relpath() version of build_index(), alternating within one
process so system load drift cancels out (same technique that caught the
_extract_zip regression - do not trust cProfile alone here either).
"""
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.source_index import (  # noqa: E402
    BINARY_EXTENSIONS,
    IGNORED_DIRS,
    SourceFile,
    SourceIndex,
)

SCRATCH = Path("/tmp/claude-1000/-home-muthu-code-Devflo/3abd434c-c866-4d80-9ffe-ce320c8e1ffa/scratchpad")
ROOT = SCRATCH / "synthetic_extracted"


def old_build_index(root: Path) -> SourceIndex:
    index = SourceIndex(root=root)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in IGNORED_DIRS]
        for filename in filenames:
            extension = Path(filename).suffix.lower()
            if extension in BINARY_EXTENSIONS:
                continue
            full_path = Path(dirpath) / filename
            relative_path = full_path.relative_to(root).as_posix()
            index.by_path[relative_path] = SourceFile(relative_path, filename, extension, full_path.stat().st_size)
            index.by_stem.setdefault(Path(filename).stem, []).append(relative_path)
            parts = relative_path.split("/")
            for start in range(len(parts) - 1):
                index.by_suffix.setdefault("/".join(parts[start + 1:]), []).append(relative_path)
    return index


def new_build_index(root: Path) -> SourceIndex:
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
            full_path = Path(dirpath) / filename
            index.by_path[relative_path] = SourceFile(relative_path, filename, extension, full_path.stat().st_size)
            index.by_stem.setdefault(Path(filename).stem, []).append(relative_path)
            parts = relative_path.split("/")
            for start in range(len(parts) - 1):
                index.by_suffix.setdefault("/".join(parts[start + 1:]), []).append(relative_path)
    return index


results = {"old": [], "new": []}
order = ["old", "new"] * 8
for label in order:
    fn = old_build_index if label == "old" else new_build_index
    t0 = time.perf_counter()
    idx = fn(ROOT)
    dt = time.perf_counter() - t0
    results[label].append(dt)
    print(f"{label}: {dt:.4f}s ({len(idx.by_path)} files)")

for label in ("old", "new"):
    vals = results[label]
    print(f"\n{label}: median={statistics.median(vals):.4f}s all={[f'{v:.4f}' for v in vals]}")

old_med = statistics.median(results["old"])
new_med = statistics.median(results["new"])
print(f"\ndelta: {old_med - new_med:+.4f}s ({(old_med - new_med) / old_med * 100:+.1f}%)")
