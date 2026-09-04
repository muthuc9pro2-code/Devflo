from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.tasks import analysis as analysis_task

def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    return db, user

def _analysis(db, user, status="processing"):
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status=status
    )
    db.add(analysis)
    db.commit()
    return analysis

def _artifact(db, analysis, **kwargs):
    defaults = dict(
        analysis_id=analysis.id, position=0, original_filename="a.log",
        saved_file_path="a.log", size_bytes=100, detected_format="generic",
        status="pending", processed_bytes=0,
    )
    defaults.update(kwargs)
    artifact = AnalysisArtifact(**defaults)
    db.add(artifact)
    db.commit()
    return artifact

def test_reports_byte_based_progress_during_ingestion():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=530, status="processing")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "processing"
    assert state["progress"] == 53
    assert "investigation_result" not in state

def test_progress_never_exceeds_98_while_still_ingesting():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=1000, status="processing")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 98
    assert state["progress"] < 99

def test_progress_can_be_zero_at_the_very_start():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=0, status="pending")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 0

def test_ingestion_done_but_not_yet_finalized_reports_99():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=1000, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "processing"
    assert state["progress"] == 99

def test_duplicate_and_unsupported_artifacts_do_not_block_the_99_transition():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(
        db, analysis, position=0, size_bytes=1000, processed_bytes=1000, status="completed"
    )
    _artifact(
        db, analysis, position=1, size_bytes=500, processed_bytes=500, status="duplicate",
        duplicate_of_artifact_id=1,
    )
    _artifact(
        db, analysis, position=2, size_bytes=10, processed_bytes=10, status="unsupported",
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 99

def test_duplicate_and_unsupported_artifacts_are_excluded_from_the_byte_ratio():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=500, status="processing")
    _artifact(
        db, analysis, position=1, size_bytes=500, processed_bytes=500, status="duplicate",
        duplicate_of_artifact_id=1,
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 50

def test_resource_limited_and_processing_error_artifacts_do_not_block_the_99_transition():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(
        db, analysis, position=0, size_bytes=1000, processed_bytes=1000, status="completed"
    )
    _artifact(
        db, analysis, position=1, size_bytes=0, processed_bytes=0, status="resource_limited",
    )
    _artifact(
        db, analysis, position=2, size_bytes=0, processed_bytes=0, status="processing_error",
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 99

def test_resource_limited_and_processing_error_artifacts_are_excluded_from_the_byte_ratio():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=500, status="processing")
    _artifact(
        db, analysis, position=1, size_bytes=500, processed_bytes=0, status="resource_limited",
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 50

def test_large_unsupported_artifact_does_not_suppress_progress():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(
        db, analysis, position=0, size_bytes=1_000_000_000, processed_bytes=0,
        status="unsupported",
    )
    _artifact(db, analysis, position=1, size_bytes=100, processed_bytes=100, status="processing")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 98

def test_completed_analysis_reports_99_never_100_and_status_completed():
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    _artifact(db, analysis, position=0, size_bytes=10, processed_bytes=10, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "completed"
    assert state["progress"] == 99
    assert state["progress"] != 100

def test_completed_analysis_must_not_misleadingly_restart_progress_animation():
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    _artifact(db, analysis, position=0, size_bytes=10, processed_bytes=10, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "completed"

def test_failed_analysis_reports_failed_status():
    db, user = _session()
    analysis = _analysis(db, user, status="failed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "failed"

def test_completed_zero_evidence_analysis_includes_zero_evidence_result():
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    _artifact(db, analysis, position=0, size_bytes=10, processed_bytes=10, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    result = state["investigation_result"]
    assert result["investigation_path"] == "zero_evidence"
    assert result["evidence_count"] == 0
    assert "unrelated" not in result["message"].lower()

def test_completed_simple_analysis_includes_simple_result():
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    artifact = _artifact(db, analysis, position=0, size_bytes=10, processed_bytes=10, status="completed")
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1,
        )
    )
    db.commit()

    state = analysis_task.compute_current_analysis_state(db, analysis)

    result = state["investigation_result"]
    assert result["investigation_path"] == "simple"
    assert result["evidence"][0]["service"] == "worker"
    assert "components" not in result

def test_completed_correlated_analysis_includes_correlated_result():
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    web = _artifact(db, analysis, position=0, original_filename="a.log", size_bytes=10, processed_bytes=10, status="completed")
    api = _artifact(db, analysis, position=1, original_filename="b.log", size_bytes=10, processed_bytes=10, status="completed")
    base = datetime.now(timezone.utc)
    db.add_all([
        Evidence(
            analysis_id=analysis.id, artifact_id=web.id, correlation_key="ck-1", fingerprint="fp-1",
            trace_id="trace-1", service="db", source_format="database",
            first_line_number=1, last_line_number=1, first_seen=base,
        ),
        Evidence(
            analysis_id=analysis.id, artifact_id=api.id, correlation_key="ck-2", fingerprint="fp-2",
            trace_id="trace-1", service="api", source_format="web_server",
            first_line_number=1, last_line_number=1, first_seen=base,
        ),
    ])
    db.commit()

    state = analysis_task.compute_current_analysis_state(db, analysis)

    result = state["investigation_result"]
    assert result["investigation_path"] == "correlated"
    assert result["component_count"] == 1
    assert len(result["components"][0]["nodes"]) == 2
    assert result["components"][0]["associations"]
    assert result["components"][0]["edges"] == []

def test_reconnect_snapshot_includes_already_known_unsupported_and_duplicate_outcomes():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    canonical = _artifact(
        db, analysis, position=0, original_filename="a.log",
        status="processing", processed_bytes=0,
    )
    _artifact(
        db, analysis, position=1, original_filename="weird.xyz",
        detected_format=None, status="unsupported", size_bytes=50, processed_bytes=50,
    )
    _artifact(
        db, analysis, position=2, original_filename="copy.log",
        status="duplicate", duplicate_of_artifact_id=canonical.id,
        size_bytes=100, processed_bytes=100,
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "processing"
    outcomes = {a["source_file"]: a for a in state["artifacts"]}
    assert outcomes["weird.xyz"]["status"] == "unsupported"
    assert "not supported" in outcomes["weird.xyz"]["message"].lower()
    assert outcomes["copy.log"]["status"] == "duplicate"
    assert outcomes["copy.log"]["duplicate_of_artifact_id"] == canonical.id
    assert outcomes["copy.log"]["duplicate_of_source_file"] == "a.log"
    assert "a.log" not in outcomes

def test_reconnect_snapshot_includes_known_zero_evidence_artifact_outcome():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(
        db, analysis, position=0, original_filename="empty.log",
        status="completed", size_bytes=10, processed_bytes=10,
    )
    _artifact(
        db, analysis, position=1, original_filename="still-going.log",
        status="processing", size_bytes=1000, processed_bytes=200,
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    outcomes = {a["source_file"]: a for a in state["artifacts"]}
    assert outcomes["empty.log"]["status"] == "processed"
    assert outcomes["empty.log"]["evidence_count"] == 0
    assert "no meaningful diagnostic evidence" in outcomes["empty.log"]["message"].lower()
    assert "still-going.log" not in outcomes

def test_reconnect_snapshot_artifacts_empty_when_nothing_terminal_yet():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, status="processing")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["artifacts"] == []
