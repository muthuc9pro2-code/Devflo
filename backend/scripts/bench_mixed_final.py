from __future__ import annotations
import logging
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.database import sessionLocal
from app.models import Analysis, AnalysisArtifact, Evidence, User
import app.tasks.analysis as analysis_task

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"
SCRATCH = Path(tempfile.gettempdir()) / "devflo-bench-scratch"
ZIP_SOURCE = SCRATCH / "synthetic_source.zip"
GITHUB_URL = "https://github.com/pallets/flask.git"

BENCH_USERNAME = "__bench_pipeline__"
BENCH_EMAIL = "bench-pipeline@example.invalid"

ALL_FORMATS = [
    ("generic_10mib.log", "text/plain"),
    ("json_10mib.jsonl", "application/json"),
    ("stack_trace_10mib.log", "text/plain"),
    ("web_server_10mib.log", "text/plain"),
    ("container_10mib.log", "text/plain"),
    ("database_10mib.log", "text/plain"),
    ("cloud_gateway_10mib.log", "text/plain"),
    ("ci_cd_10mib.log", "text/plain"),
    ("browser_10mib.har", "application/json"),
    ("message_broker_10mib.log", "text/plain"),
    ("serverless_10mib.log", "text/plain"),
    ("syslog_10mib.log", "text/plain"),
    ("opentelemetry_10mib.json", "application/json"),
]

def _get_or_create_bench_user(db) -> User:
    user = db.query(User).filter(User.username == BENCH_USERNAME).first()
    if user is not None:
        return user
    user = User(username=BENCH_USERNAME, email=BENCH_EMAIL, hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def _cleanup_all(db, user_id: int) -> None:
    old = db.query(Analysis).filter(Analysis.user_id == user_id).all()
    for analysis in old:
        db.query(Evidence).filter(Evidence.analysis_id == analysis.id).delete()
        db.query(AnalysisArtifact).filter(AnalysisArtifact.analysis_id == analysis.id).delete()
        db.delete(analysis)
    db.commit()

def _make_mixed_analysis(db, user_id: int, source_kind: str | None, source_reference: str | None) -> int:
    _cleanup_all(db, user_id)

    analysis = Analysis(
        user_id=user_id,
        original_filename="mixed.zip",
        saved_file_path=str(FIXTURES_DIR / ALL_FORMATS[0][0]),
        source_kind=source_kind,
        source_reference=source_reference,
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
    )
    db.add(analysis)
    db.commit()
    db.refresh(analysis)

    for position, (name, content_type) in enumerate(ALL_FORMATS):
        path = FIXTURES_DIR / name
        artifact = AnalysisArtifact(
            analysis_id=analysis.id,
            position=position,
            original_filename=name,
            saved_file_path=str(path),
            content_type=content_type,
            size_bytes=path.stat().st_size,
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
        self.artifact_lines: list[tuple] = []

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.msg
        args = record.args or ()
        if "ingestion completed in" in msg:
            self.stages["ingestion_incl_source_prep"] = float(args[-1])
        elif "identity resolution completed in" in msg:
            self.stages["identity"] = float(args[-1])
        elif "timeline processing completed in" in msg:
            self.stages["timeline"] = float(args[-1])
        elif "TOTAL processing time" in msg:
            self.stages["total"] = float(args[-1])
        elif "artifact_position=" in msg:
            self.artifact_lines.append(args)

def run_scenario(label: str, source_kind: str | None, source_reference: str | None) -> None:
    db = sessionLocal()
    user = _get_or_create_bench_user(db)
    analysis_id = _make_mixed_analysis(db, user.id, source_kind, source_reference)
    db.close()

    logger = logging.getLogger("app.tasks.analysis")
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    capture = _StageCapture()
    logger.addHandler(capture)

    real_prepare_source_index = analysis_task._prepare_source_index
    source_prep_holder: dict[str, float] = {}

    def timed_prepare_source_index(analysis):
        t0 = time.perf_counter()
        result = real_prepare_source_index(analysis)
        source_prep_holder["source_prep_s"] = time.perf_counter() - t0
        return result

    analysis_task._prepare_source_index = timed_prepare_source_index

    wall_start = time.perf_counter()
    try:
        analysis_task.process_analysis(analysis_id)
    finally:
        analysis_task._prepare_source_index = real_prepare_source_index
        logger.removeHandler(capture)
        logger.setLevel(previous_level)
    wall_elapsed = time.perf_counter() - wall_start

    source_prep_s = source_prep_holder.get("source_prep_s", 0.0)
    ingestion_incl = capture.stages.get("ingestion_incl_source_prep", 0.0)
    pure_ingestion = ingestion_incl - source_prep_s

    print(f"\n=== Scenario {label} (source_kind={source_kind}) ===")
    print(f"wall_total: {wall_elapsed:.3f}s")
    print(f"source_prep (clone/extract + build_index): {source_prep_s:.3f}s")
    print(f"pure diagnostic ingestion (13 formats, 13x10MiB): {pure_ingestion:.3f}s")
    print(f"identity: {capture.stages.get('identity', 0):.3f}s")
    print(f"timeline: {capture.stages.get('timeline', 0):.3f}s")
    print(f"total (process_analysis-reported): {capture.stages.get('total', 0):.3f}s")
    for args in capture.artifact_lines:
        print(f"  {args}")

    db = sessionLocal()
    count = db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count()
    db.close()
    print(f"evidence_rows: {count}")

def main() -> None:
    if not ZIP_SOURCE.exists():
        print(f"WARNING: {ZIP_SOURCE} missing, regenerating via bench_source.py's generator")
        sys.exit(1)

    run_scenario("A: diagnostics only", None, None)
    run_scenario("B: diagnostics + source ZIP", "zip", str(ZIP_SOURCE))
    run_scenario("C: diagnostics + GitHub URL", "github", GITHUB_URL)

    db = sessionLocal()
    user = db.query(User).filter(User.username == BENCH_USERNAME).first()
    if user is not None:
        _cleanup_all(db, user.id)
    db.close()

    from app.core.processing_config import SOURCE_STORAGE_ROOT
    root = Path(SOURCE_STORAGE_ROOT)
    if root.exists():
        shutil.rmtree(root)
    print("\ncleaned up all bench rows and staged source directories")

if __name__ == "__main__":
    main()
