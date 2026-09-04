
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.artifact_detector import ArtifactFormat, detect_artifact
from app.services.diagnostic_adapters import stream_artifact_events
from app.services import event_filter

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"

FORMAT_FIXTURES = {
    "generic": "generic_10mib.log",
    "json": "json_10mib.jsonl",
    "stack_trace": "stack_trace_10mib.log",
    "web_server": "web_server_10mib.log",
    "container": "container_10mib.log",
    "database": "database_10mib.log",
    "cloud_gateway": "cloud_gateway_10mib.log",
    "ci_cd": "ci_cd_10mib.log",
    "browser": "browser_10mib.har",
    "message_broker": "message_broker_10mib.log",
    "serverless": "serverless_10mib.log",
    "syslog": "syslog_10mib.log",
    "opentelemetry": "opentelemetry_10mib.json",
}


def run_once(fixture_path: Path, artifact_format: ArtifactFormat) -> dict:
    t0 = time.perf_counter()
    records = list(
        stream_artifact_events(
            file_path=str(fixture_path),
            artifact_format=artifact_format,
            source_file=fixture_path.name,
        )
    )
    parse_elapsed = time.perf_counter() - t0

    events = [r.event for r in records if r.event is not None]
    t1 = time.perf_counter()
    important = event_filter.filter_important_events(events)
    retain_elapsed = time.perf_counter() - t1

    return {
        "records": len(records),
        "normalized": len(events),
        "important": len(important),
        "parse_s": parse_elapsed,
        "retain_s": retain_elapsed,
        "total_s": parse_elapsed + retain_elapsed,
        "bytes": fixture_path.stat().st_size,
    }


def bench(formats: list[str], iterations: int) -> None:
    for name in formats:
        fixture_name = FORMAT_FIXTURES[name]
        fixture_path = FIXTURE_DIR / fixture_name
        if not fixture_path.exists():
            print(f"{name}: SKIP (fixture missing: {fixture_path})")
            continue
        artifact_format = detect_artifact(fixture_path, filename=fixture_name)
        results = [run_once(fixture_path, artifact_format) for _ in range(iterations)]
        total_s = [r["total_s"] for r in results]
        parse_s = [r["parse_s"] for r in results]
        last = results[-1]
        print(
            f"{name:16s} bytes={last['bytes']:>10,} records={last['records']:>7,} "
            f"normalized={last['normalized']:>7,} important={last['important']:>7,} "
            f"parse_median={statistics.median(parse_s):.3f}s "
            f"total_median={statistics.median(total_s):.3f}s "
            f"(n={iterations}, all={[f'{v:.2f}' for v in total_s]})"
        )


def profile_one(name: str) -> None:
    import cProfile
    import pstats

    fixture_name = FORMAT_FIXTURES[name]
    fixture_path = FIXTURE_DIR / fixture_name
    artifact_format = detect_artifact(fixture_path, filename=fixture_name)

    def _work():
        return list(
            stream_artifact_events(
                file_path=str(fixture_path),
                artifact_format=artifact_format,
                source_file=fixture_path.name,
            )
        )

    profiler = cProfile.Profile()
    profiler.enable()
    records = _work()
    profiler.disable()
    normalized = sum(1 for r in records if r.event is not None)
    print(f"{name}: records={len(records)} normalized={normalized}")
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    stats.print_stats(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["bench", "profile"])
    parser.add_argument("--format", type=str, default="")
    parser.add_argument("--iterations", type=int, default=5)
    args = parser.parse_args()

    if args.mode == "bench":
        formats = [args.format] if args.format else list(FORMAT_FIXTURES)
        bench(formats, args.iterations)
    else:
        if not args.format:
            raise SystemExit("--format is required for profile mode")
        profile_one(args.format)
