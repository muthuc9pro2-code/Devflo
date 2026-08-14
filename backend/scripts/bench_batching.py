"""Per-format batching benchmark: does the current INGESTION_RAW_BATCH_BYTES
(10 MiB) / INGESTION_RAW_BATCH_ITEMS (20000) default actually beat smaller or
larger batches for real DB commit/checkpoint cost? Runs the REAL production
pipeline (app.tasks.analysis.process_analysis) against the real MySQL
database this app uses, only varying the batch size create_batches() is
called with (via monkeypatching app.tasks.analysis.create_batches to a
partial - process_analysis itself is untouched).

Self-cleaning: uses the same __bench_pipeline__ throwaway user/analysis
pattern as scripts/bench_pipeline.py, deletes its own rows before each run
and at the end. Never touches any other user's data.

Usage:
    .venv/bin/python scripts/bench_batching.py
"""
from __future__ import annotations

import functools
import logging
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.database import sessionLocal  # noqa: E402
from app.models import Analysis, AnalysisArtifact, Evidence, User  # noqa: E402
from app.services.batch_processor import create_batches as real_create_batches  # noqa: E402
import app.tasks.analysis as analysis_task  # noqa: E402

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"
BENCH_USERNAME = "__bench_pipeline__"
BENCH_EMAIL = "bench-pipeline@example.invalid"

FORMATS = {
    "generic": FIXTURES_DIR / "generic_10mib.log",
    "database": FIXTURES_DIR / "database_10mib.log",
    "ci_cd": FIXTURES_DIR / "ci_cd_10mib.log",
}

VARIANTS = {
    "small (1MiB/2000)": (1 * 1024 * 1024, 2_000),
    "current (10MiB/20000)": (10 * 1024 * 1024, 20_000),
    "large (50MiB/100000)": (50 * 1024 * 1024, 100_000),
}


def _get_or_create_bench_user(db) -> User:
    user = db.query(User).filter(User.username == BENCH_USERNAME).first()
    if user is not None:
        return user
    user = User(username=BENCH_USERNAME, email=BENCH_EMAIL, hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _reset_analysis(db, user_id: int, fixture_path: Path) -> int:
    old = db.query(Analysis).filter(Analysis.user_id == user_id).all()
    for analysis in old:
        db.query(Evidence).filter(Evidence.analysis_id == analysis.id).delete()
        db.query(AnalysisArtifact).filter(AnalysisArtifact.analysis_id == analysis.id).delete()
        db.delete(analysis)
    db.commit()

    size_bytes = fixture_path.stat().st_size
    analysis = Analysis(
        user_id=user_id,
        original_filename=fixture_path.name,
        saved_file_path=str(fixture_path),
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
        original_filename=fixture_path.name,
        saved_file_path=str(fixture_path),
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
    def __init__(self):
        super().__init__(level=logging.INFO)
        self.stages: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.msg
        args = record.args or ()
        if "ingestion completed in" in msg:
            self.stages["ingestion"] = float(args[-1])
        elif "TOTAL processing time" in msg:
            self.stages["total"] = float(args[-1])


def run() -> None:
    db = sessionLocal()
    user = _get_or_create_bench_user(db)
    db.close()

    logger = logging.getLogger("app.tasks.analysis")
    previous_level = logger.level
    logger.setLevel(logging.INFO)

    results: dict[str, dict[str, list[float]]] = {fmt: {v: [] for v in VARIANTS} for fmt in FORMATS}

    try:
        for fmt, fixture_path in FORMATS.items():
            for variant_label, (max_bytes, max_items) in VARIANTS.items():
                analysis_task.create_batches = functools.partial(
                    real_create_batches, max_batch_bytes=max_bytes, max_batch_items=max_items
                )
                for _ in range(3):
                    db = sessionLocal()
                    analysis_id = _reset_analysis(db, user.id, fixture_path)
                    db.close()

                    capture = _StageCapture()
                    logger.addHandler(capture)
                    wall_start = time.perf_counter()
                    try:
                        analysis_task.process_analysis(analysis_id)
                    finally:
                        logger.removeHandler(capture)
                    wall_elapsed = time.perf_counter() - wall_start

                    results[fmt][variant_label].append(wall_elapsed)
                    print(f"{fmt} / {variant_label}: {wall_elapsed:.3f}s (ingestion={capture.stages.get('ingestion')})")
    finally:
        analysis_task.create_batches = real_create_batches
        db = sessionLocal()
        old = db.query(Analysis).filter(Analysis.user_id == user.id).all()
        for analysis in old:
            db.query(Evidence).filter(Evidence.analysis_id == analysis.id).delete()
            db.query(AnalysisArtifact).filter(AnalysisArtifact.analysis_id == analysis.id).delete()
            db.delete(analysis)
        db.commit()
        db.close()
        logger.setLevel(previous_level)

    print("\n--- MEDIAN (n=3) ---")
    print(f"{'format':<10} " + " ".join(f"{v:<24}" for v in VARIANTS))
    for fmt in FORMATS:
        row = [f"{statistics.median(results[fmt][v]):.3f}s".ljust(24) for v in VARIANTS]
        print(f"{fmt:<10} " + " ".join(row))


if __name__ == "__main__":
    run()
