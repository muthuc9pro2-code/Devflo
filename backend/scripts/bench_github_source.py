from __future__ import annotations
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.source_archive import _clone_github
from app.services.source_index import build_index

SCRATCH = Path(tempfile.gettempdir()) / "devflo-bench-scratch"
DEST = SCRATCH / "github_bench_clone"

def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "https://github.com/pallets/flask.git"

    if DEST.exists():
        shutil.rmtree(DEST)

    t0 = time.perf_counter()
    _clone_github(url, DEST)
    clone_s = time.perf_counter() - t0

    total_files = sum(1 for _ in DEST.rglob("*") if _.is_file())
    total_bytes = sum(p.stat().st_size for p in DEST.rglob("*") if p.is_file())

    t0 = time.perf_counter()
    index = build_index(DEST)
    index_s = time.perf_counter() - t0

    print(f"url: {url}")
    print(f"clone (network + git, --depth 1 shallow): {clone_s:.3f}s")
    print(f"checked-out tree: {total_files} files, {total_bytes:,} bytes")
    print(f"build_index (local CPU only, same code path as ZIP source): {index_s:.4f}s")
    print(f"indexed: {len(index.by_path)} files")
    print(f"local-CPU share of total prep time: {index_s / (clone_s + index_s) * 100:.2f}%")

    shutil.rmtree(DEST)

if __name__ == "__main__":
    main()
