"""Peak memory measurement for representative formats at 10 MiB and 50 MiB,
CPU-only (no DB), mirroring what a single _process_artifact() call does:
stream_artifact_events -> create_batches -> per-batch retention filter +
fingerprinting. Each fixture is measured in its own fresh subprocess so
resource.getrusage's peak-RSS high-water-mark isn't contaminated by a
previous fixture's peak in the same run.

Usage:
    .venv/bin/python scripts/bench_memory.py                 # measure all
    .venv/bin/python scripts/bench_memory.py --child <fixture-path> <format>  # internal
"""
from __future__ import annotations

import resource
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"

CASES = [
    ("generic", "generic_10mib.log"),
    ("generic", "generic_50mib.log"),
    ("json", "json_10mib.jsonl"),
    ("json", "json_50mib.jsonl"),
    ("database", "database_10mib.log"),
    ("database", "database_50mib.log"),
    ("container", "container_10mib.log"),
    ("container", "container_50mib.log"),
]


def _child(fixture_path: str, fmt_name: str) -> None:
    from app.services.artifact_detector import ArtifactFormat
    from app.services.diagnostic_adapters import stream_artifact_events
    from app.services.batch_processor import create_batches
    from app.services.exception_fingerprint import build_exception_fingerprint

    _IMPORTANT_LEVELS = frozenset({"WARNING", "WARN", "ERROR", "CRITICAL"})
    fmt = ArtifactFormat(fmt_name)

    records = stream_artifact_events(
        file_path=fixture_path,
        artifact_format=fmt,
        source_file=Path(fixture_path).name,
    )

    important_total = 0
    for batch in create_batches(records):
        important = []
        for record in batch:
            event = record.event
            if event is None:
                continue
            if event.level in _IMPORTANT_LEVELS or (
                event.source_format == "opentelemetry"
                and (event.trace_id is not None or event.span_id is not None)
            ):
                important.append(event)
        cache: dict = {}
        for event in important:
            key = (
                (None, event.level, event.raw_line)
                if event.exception_type is None
                else (event.exception_type, event.exception_message)
            )
            fp = cache.get(key)
            if fp is None:
                fp = build_exception_fingerprint(event)
                cache[key] = fp
            event.fingerprint = fp
        important_total += len(important)

    peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"RESULT important={important_total} peak_rss_kib={peak_kib}")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        _child(sys.argv[2], sys.argv[3])
        return

    print(f"{'format':<10} {'fixture':<22} {'bytes':>12}   {'peak_rss_mib':>12}")
    for fmt_name, filename in CASES:
        path = FIXTURES_DIR / filename
        if not path.exists():
            print(f"{fmt_name:<10} {filename:<22} MISSING, skipped")
            continue
        proc = subprocess.run(
            [sys.executable, __file__, "--child", str(path), fmt_name],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[1]),
        )
        line = next((l for l in proc.stdout.splitlines() if l.startswith("RESULT")), None)
        if line is None:
            print(f"{fmt_name:<10} {filename:<22} FAILED: {proc.stderr[-500:]}")
            continue
        parts = dict(kv.split("=") for kv in line.replace("RESULT ", "").split())
        peak_mib = int(parts["peak_rss_kib"]) / 1024
        size_bytes = path.stat().st_size
        print(f"{fmt_name:<10} {filename:<22} {size_bytes:>12,}   {peak_mib:>10.1f} MiB   important={parts['important']}")


if __name__ == "__main__":
    main()
