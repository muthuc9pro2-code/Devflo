"""Profile source ZIP preparation, extraction, indexing, and stack-frame
correlation independently from diagnostic ingestion.

Usage:
    .venv/bin/python scripts/bench_source.py
"""

from __future__ import annotations

import shutil
import statistics
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.log_praser import ParsedEvent, StackFrame  # noqa: E402
from app.services.source_archive import _extract_zip, validate_source_zip  # noqa: E402
from app.services.source_index import build_index, correlate_event  # noqa: E402

SCRATCH = Path("/tmp/claude-1000/-home-muthu-code-Devflo/3abd434c-c866-4d80-9ffe-ce320c8e1ffa/scratchpad")
REPO_DIR = SCRATCH / "synthetic_repo"
ZIP_PATH = SCRATCH / "synthetic_source.zip"
EXTRACT_DIR = SCRATCH / "synthetic_extracted"

N_DIRS = 40
FILES_PER_DIR = 50
LINES_PER_FILE = 120


def generate_repo(n_dirs: int = N_DIRS, files_per_dir: int = FILES_PER_DIR, lines_per_file: int = LINES_PER_FILE) -> None:
    if REPO_DIR.exists():
        shutil.rmtree(REPO_DIR)
    REPO_DIR.mkdir(parents=True)
    for d in range(n_dirs):
        dir_path = REPO_DIR / f"module_{d}"
        dir_path.mkdir()
        for f in range(files_per_dir):
            file_path = dir_path / f"file_{f}.py"
            lines = [f"# module_{d}/file_{f}.py line {i}\ndef fn_{i}():\n    return {i}\n" for i in range(lines_per_file)]
            file_path.write_text("".join(lines))
    # A couple of "hot" files that many stack frames will reference repeatedly.
    (REPO_DIR / "module_0" / "worker.py").write_text(
        "\n".join(f"def run_{i}():\n    pass" for i in range(500))
    )
    (REPO_DIR / "module_1" / "app.js").write_text(
        "\n".join(f"function handle_{i}() {{ return {i}; }}" for i in range(500))
    )


def zip_repo() -> None:
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in REPO_DIR.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(REPO_DIR))


def bench(label: str, fn, iterations: int = 5) -> list[float]:
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    print(f"{label}: median={statistics.median(times):.4f}s all={[f'{t:.4f}' for t in times]}")
    return times


def main() -> None:
    print("Generating synthetic repo...")
    generate_repo()
    total_files = sum(1 for _ in REPO_DIR.rglob("*") if _.is_file())
    total_bytes = sum(p.stat().st_size for p in REPO_DIR.rglob("*") if p.is_file())
    print(f"repo: {total_files} files, {total_bytes:,} bytes")
    zip_repo()
    print(f"zip: {ZIP_PATH.stat().st_size:,} bytes")

    bench("validate_source_zip", lambda: validate_source_zip(ZIP_PATH))

    def do_extract():
        if EXTRACT_DIR.exists():
            shutil.rmtree(EXTRACT_DIR)
        _extract_zip(ZIP_PATH, EXTRACT_DIR)

    bench("extract_zip", do_extract, iterations=5)

    if EXTRACT_DIR.exists():
        shutil.rmtree(EXTRACT_DIR)
    _extract_zip(ZIP_PATH, EXTRACT_DIR)

    index_holder = {}

    def do_index():
        index_holder["index"] = build_index(EXTRACT_DIR)

    bench("build_index", do_index, iterations=5)
    index = index_holder["index"]
    print(f"index: {len(index.by_path)} files indexed")

    # Correlation: many events, each with a stack frame pointing at ONE of a
    # small set of "hot" files (worst case for repeated re-reads without the
    # context cache), interspersed with frames into many DIFFERENT files.
    hot_events = [
        ParsedEvent(
            line_number=i,
            raw_line=f"error {i}",
            stack_frames=[StackFrame(file="module_0/worker.py", line=(i % 480) + 1, function="run_x")],
        )
        for i in range(20000)
    ]
    unique_events = [
        ParsedEvent(
            line_number=i,
            raw_line=f"error {i}",
            stack_frames=[
                StackFrame(
                    file=f"module_{i % N_DIRS}/file_{i % FILES_PER_DIR}.py",
                    line=(i % lines_per_file_default()) + 1,
                    function="fn_x",
                )
            ],
        )
        for i in range(20000)
    ]

    def correlate_hot():
        for event in hot_events:
            correlate_event(event, index)

    def correlate_unique():
        for event in unique_events:
            correlate_event(event, index)

    bench("correlate_20000_repeated_hot_file", correlate_hot, iterations=3)
    bench("correlate_20000_unique_files", correlate_unique, iterations=3)


def lines_per_file_default() -> int:
    return LINES_PER_FILE


if __name__ == "__main__":
    main()
