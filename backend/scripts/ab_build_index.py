"""Historical interleaved A/B for the Phase-1 build_index optimization.

This benchmark intentionally owns its legacy by_path/by_suffix/by_stem shape
instead of constructing the production SourceIndex. SourceIndex changed after
this benchmark was recorded: bounded reversed lookup keys replaced the
materialized suffix map.

Keeping the historical structure local lets the old per-file
Path.relative_to() and new per-directory os.path.relpath() implementations
remain runnable and directly comparable.
"""
import os
import statistics
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.source_index import (  # noqa: E402
    BINARY_EXTENSIONS,
    IGNORED_DIRS,
    SourceFile,
)

SCRATCH = Path(tempfile.gettempdir()) / "devflo-bench-scratch"
ROOT = SCRATCH / "synthetic_extracted"


@dataclass
class BenchIndex:
    root: Path
    by_path: dict[str, SourceFile] = field(default_factory=dict)
    by_suffix: dict[str, list[str]] = field(default_factory=dict)
    by_stem: dict[str, list[str]] = field(default_factory=dict)


def old_build_index(root: Path) -> BenchIndex:
    index = BenchIndex(root=root)
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


def new_build_index(root: Path) -> BenchIndex:
    index = BenchIndex(root=root)
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


def _assert_equivalent(old: BenchIndex, new: BenchIndex) -> None:
    assert old.by_path == new.by_path
    assert old.by_suffix == new.by_suffix
    assert old.by_stem == new.by_stem


def main() -> None:
    # Semantic gate outside timing.
    _assert_equivalent(old_build_index(ROOT), new_build_index(ROOT))

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
        values = results[label]
        print(f"\n{label}: median={statistics.median(values):.4f}s all={[f'{value:.4f}' for value in values]}")

    old_median = statistics.median(results["old"])
    new_median = statistics.median(results["new"])
    delta = old_median - new_median
    percent = delta / old_median * 100 if old_median else 0.0
    print(f"\ndelta: {delta:+.4f}s ({percent:+.1f}%)")


if __name__ == "__main__":
    main()
