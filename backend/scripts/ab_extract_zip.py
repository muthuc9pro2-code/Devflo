import os
import shutil
import statistics
import sys
import tempfile
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.source_archive import SourceInputError, _safe_members

SCRATCH = Path(tempfile.gettempdir()) / "devflo-bench-scratch"
ZIP_PATH = SCRATCH / "synthetic_source.zip"


def old_extract_zip(path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    with zipfile.ZipFile(path) as zf:
        for member, relative in _safe_members(zf):
            if member.is_dir() or not relative or relative == ".":
                continue
            target = (dest / relative).resolve()
            if root not in target.parents:
                raise SourceInputError(f"Unsafe path in source ZIP: {member.filename}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as source, open(target, "wb") as destination:
                shutil.copyfileobj(source, destination)


def new_extract_zip(path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    with zipfile.ZipFile(path) as zf:
        for member, relative in _safe_members(zf):
            if member.is_dir() or not relative or relative == ".":
                continue
            target = Path(os.path.realpath(dest / relative))
            if root not in target.parents:
                raise SourceInputError(f"Unsafe path in source ZIP: {member.filename}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as source, open(target, "wb") as destination:
                shutil.copyfileobj(source, destination)


results = {"old": [], "new": []}
order = ["old", "new"] * 6
for label in order:
    fn = old_extract_zip if label == "old" else new_extract_zip
    dest = SCRATCH / f"ab_extract_{label}"
    if dest.exists():
        shutil.rmtree(dest)
    t0 = time.perf_counter()
    fn(ZIP_PATH, dest)
    dt = time.perf_counter() - t0
    results[label].append(dt)
    print(f"{label}: {dt:.4f}s")
    shutil.rmtree(dest)

for label in ("old", "new"):
    vals = results[label]
    print(f"\n{label}: median={statistics.median(vals):.4f}s all={[f'{v:.4f}' for v in vals]}")

old_med = statistics.median(results["old"])
new_med = statistics.median(results["new"])
print(f"\ndelta: {old_med - new_med:+.4f}s ({(old_med - new_med) / old_med * 100:+.1f}%)")
