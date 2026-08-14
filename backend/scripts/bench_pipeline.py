"""Benchmark harness for the ingestion / identity-resolution pipeline.

Runs the REAL production code paths (app.tasks.analysis.process_analysis,
app.services.diagnostic_adapters.stream_artifact_events, ...) against the
frozen 10 MiB fixture at tests/fixtures/bench/devflo_10mib.log and a real
MySQL database (same one the app uses). Not part of the pytest suite -
this is a manual/dev tool, invoked directly:

    .venv/bin/python scripts/bench_pipeline.py full --iterations 5
    .venv/bin/python scripts/bench_pipeline.py cpu --iterations 5
    .venv/bin/python scripts/bench_pipeline.py profile
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text  # noqa: E402

from app.db.database import sessionLocal  # noqa: E402
from app.models import Analysis, AnalysisArtifact, Evidence, User  # noqa: E402

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "tests/fixtures/bench/devflo_10mib.log"
BENCH_USERNAME = "__bench_pipeline__"
BENCH_EMAIL = "bench-pipeline@example.invalid"


def _get_or_create_bench_user(db) -> User:
    user = db.query(User).filter(User.username == BENCH_USERNAME).first()
    if user is not None:
        return user
    user = User(
        username=BENCH_USERNAME,
        email=BENCH_EMAIL,
        hashed_password="x",
        is_verified=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _reset_analysis(db, user_id: int) -> int:
    """Create a fresh analysis+artifact row pointing at the frozen fixture.

    Deletes any prior benchmark analyses so each run starts from a clean
    slate (evidence table has no leftover rows for this analysis id).
    """

    old = db.query(Analysis).filter(Analysis.user_id == user_id).all()
    for analysis in old:
        db.query(Evidence).filter(Evidence.analysis_id == analysis.id).delete()
        db.query(AnalysisArtifact).filter(
            AnalysisArtifact.analysis_id == analysis.id
        ).delete()
        db.delete(analysis)
    db.commit()

    size_bytes = FIXTURE_PATH.stat().st_size

    analysis = Analysis(
        user_id=user_id,
        original_filename="devflo_10mib.log",
        saved_file_path=str(FIXTURE_PATH),
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
    )
    db.add(analysis)
    db.commit()
    db.refresh(analysis)

    artifact = AnalysisArtifact(
        analysis_id=analysis.id,
        position=0,
        original_filename="devflo_10mib.log",
        saved_file_path=str(FIXTURE_PATH),
        content_type="text/plain",
        size_bytes=size_bytes,
        detected_format=None,
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
    )
    db.add(artifact)
    db.commit()
    return analysis.id


class _StageCapture(logging.Handler):
    """Pulls the perf_counter stage timings straight out of the log args
    process_analysis() already emits, instead of re-parsing message text."""

    def __init__(self):
        super().__init__(level=logging.INFO)
        self.stages: dict[str, float] = {}
        self.artifact_lines: list[tuple] = []

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.msg
        args = record.args or ()
        if "ingestion completed in" in msg:
            self.stages["ingestion"] = float(args[-1])
        elif "identity resolution completed in" in msg:
            self.stages["identity"] = float(args[-1])
        elif "timeline processing completed in" in msg:
            self.stages["timeline"] = float(args[-1])
        elif "TOTAL processing time" in msg:
            self.stages["total"] = float(args[-1])
        elif "artifact_position=" in msg:
            self.artifact_lines.append(args)


def _evidence_signature(db, analysis_id: int) -> tuple[int, str]:
    rows = (
        db.execute(
            text(
                """
                SELECT fingerprint, correlation_key, event_type, severity,
                       trace_id, request_id, span_id, parent_span_id,
                       service, module, host, container, pod, endpoint,
                       http_status, source_file, source_format,
                       first_seen, last_seen, occurrence_count,
                       first_line_number, last_line_number,
                       representative_line, resolved_identity,
                       identity_match_type, identity_strength
                FROM evidence
                WHERE analysis_id = :analysis_id
                ORDER BY fingerprint, correlation_key
                """
            ),
            {"analysis_id": analysis_id},
        )
        .mappings()
        .all()
    )
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(dict(row), sort_keys=True, default=str).encode())
    return len(rows), digest.hexdigest()


def run_full(iterations: int) -> None:
    from app.tasks.analysis import process_analysis

    db = sessionLocal()
    user = _get_or_create_bench_user(db)
    db.close()

    logger = logging.getLogger("app.tasks.analysis")
    previous_level = logger.level
    logger.setLevel(logging.INFO)

    per_iteration = []
    for i in range(iterations):
        db = sessionLocal()
        analysis_id = _reset_analysis(db, user.id)
        db.close()

        capture = _StageCapture()
        logger.addHandler(capture)
        wall_start = time.perf_counter()
        try:
            process_analysis(analysis_id)
        finally:
            logger.removeHandler(capture)
        wall_elapsed = time.perf_counter() - wall_start

        db = sessionLocal()
        count, digest = _evidence_signature(db, analysis_id)
        db.close()

        capture.stages["wall_total"] = wall_elapsed
        capture.stages["evidence_count"] = count
        capture.stages["evidence_hash"] = digest
        per_iteration.append(capture.stages)
        print(
            f"iter {i}: ingestion={capture.stages.get('ingestion'):.3f}s "
            f"identity={capture.stages.get('identity'):.3f}s "
            f"timeline={capture.stages.get('timeline'):.3f}s "
            f"total={capture.stages.get('total'):.3f}s "
            f"evidence={count} hash={digest[:12]}"
        )

    logger.setLevel(previous_level)

    def median_of(key):
        values = [it[key] for it in per_iteration if key in it]
        return statistics.median(values) if values else None

    print("\n--- MEDIAN (n=%d) ---" % iterations)
    for key in ("ingestion", "identity", "timeline", "total", "wall_total"):
        value = median_of(key)
        print(f"{key}: {value:.3f}s" if value is not None else f"{key}: n/a")
    counts = {it["evidence_count"] for it in per_iteration}
    hashes = {it["evidence_hash"] for it in per_iteration}
    print(f"evidence_count consistent: {counts if len(counts) > 1 else counts.pop()}")
    print(f"evidence_hash consistent: {len(hashes) == 1} ({next(iter(hashes))[:16]}...)")


def run_cpu_only(iterations: int) -> None:
    """Pure-Python ingestion CPU cost, no DB involved at all.

    Mirrors what _process_artifact / _persist_artifact_batch do before any
    database call, broken into the phases the task asked to see separately.
    """

    from app.services.artifact_detector import ArtifactFormat, detect_artifact
    from app.services.diagnostic_adapters import stream_artifact_events
    from app.services.exception_fingerprint import build_exception_fingerprint
    from app.services.batch_processor import create_batches
    from app.utils.file_reader import stream_text_lines

    _IMPORTANT_LEVELS = frozenset({"WARNING", "WARN", "ERROR", "CRITICAL"})
    fmt = detect_artifact(str(FIXTURE_PATH), filename=FIXTURE_PATH.name)
    print(f"detected format: {fmt.value}")

    results = []
    for i in range(iterations):
        t0 = time.perf_counter()
        raw_lines = 0
        for _line, _end in stream_text_lines(str(FIXTURE_PATH)):
            raw_lines += 1
        t1 = time.perf_counter()

        records = list(
            stream_artifact_events(
                file_path=str(FIXTURE_PATH),
                artifact_format=fmt,
                source_file=FIXTURE_PATH.name,
            )
        )
        t2 = time.perf_counter()

        normalized = sum(1 for r in records if r.event is not None)
        important = []
        for batch in create_batches(iter(records)):
            for record in batch:
                event = record.event
                if event is None:
                    continue
                if event.level in _IMPORTANT_LEVELS or (
                    event.source_format == "opentelemetry"
                    and (event.trace_id is not None or event.span_id is not None)
                ):
                    important.append(event)
        t3 = time.perf_counter()

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
        t4 = time.perf_counter()

        stage = {
            "raw_lines": raw_lines,
            "records": len(records),
            "normalized": normalized,
            "important": len(important),
            "file_read_s": t1 - t0,
            "parse_total_s": t2 - t1,
            "classify_normalize_s": (t2 - t1) - 0.0,
            "retention_filter_s": t3 - t2,
            "fingerprint_s": t4 - t3,
            "cpu_total_s": t4 - t0,
        }
        results.append(stage)
        print(
            f"iter {i}: records={stage['records']} normalized={stage['normalized']} "
            f"important={stage['important']} file_read={stage['file_read_s']:.3f}s "
            f"parse={stage['parse_total_s']:.3f}s retain={stage['retention_filter_s']:.3f}s "
            f"fp={stage['fingerprint_s']:.3f}s cpu_total={stage['cpu_total_s']:.3f}s"
        )

    print("\n--- MEDIAN (n=%d) ---" % iterations)
    for key in (
        "file_read_s",
        "parse_total_s",
        "retention_filter_s",
        "fingerprint_s",
        "cpu_total_s",
    ):
        print(f"{key}: {statistics.median(r[key] for r in results):.3f}s")
    print(f"records: {results[-1]['records']}")
    print(f"normalized: {results[-1]['normalized']}")
    print(f"important: {results[-1]['important']}")


def run_profile() -> None:
    import cProfile
    import pstats

    from app.services.artifact_detector import detect_artifact
    from app.services.diagnostic_adapters import stream_artifact_events

    fmt = detect_artifact(str(FIXTURE_PATH), filename=FIXTURE_PATH.name)

    def _work():
        return list(
            stream_artifact_events(
                file_path=str(FIXTURE_PATH),
                artifact_format=fmt,
                source_file=FIXTURE_PATH.name,
            )
        )

    profiler = cProfile.Profile()
    profiler.enable()
    records = _work()
    profiler.disable()
    print(f"records: {len(records)}")
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    stats.print_stats(25)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["full", "cpu", "profile"])
    parser.add_argument("--iterations", type=int, default=5)
    args = parser.parse_args()

    if args.mode == "full":
        run_full(args.iterations)
    elif args.mode == "cpu":
        run_cpu_only(args.iterations)
    else:
        run_profile()
