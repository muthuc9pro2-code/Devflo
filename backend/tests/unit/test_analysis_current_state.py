"""Resumable current progress/state for frontend re-entry
(compute_current_analysis_state / reconstruct_current_investigation_result)
and SSE reconnect behavior.

All progress numbers are derived from already-persisted
AnalysisArtifact.processed_bytes/size_bytes/status and Analysis.status -
no Redis, no in-memory global, no second progress-tracking system. Uses
the exact same _ingestion_percentage formula/98-cap the live SSE stream
uses (extracted, not reimplemented, so the two cannot drift apart).
"""
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


# --- byte-based progress during ingestion, capped at 98 -------------------


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
    # All bytes physically copied, but this artifact hasn't reached
    # status="completed" yet (still mid-commit) - must not read as 99/100.
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


# --- post-ingestion (99), before Analysis.status flips to completed -------


def test_ingestion_done_but_not_yet_finalized_reports_99():
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=1000, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "processing"
    assert state["progress"] == 99


def test_duplicate_and_unsupported_artifacts_do_not_block_the_99_transition():
    """A duplicate/unsupported artifact never reaches status="completed",
    so it must not count against "is ingestion actually done" - only
    genuinely dispatchable artifacts do."""
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
    """Additional Requirement B: unsupported/duplicate artifacts are never
    dispatched for ingestion, so their size_bytes/processed_bytes must not
    enter the numerator or denominator at all - only genuinely dispatchable
    artifacts drive the ratio."""
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=500, status="processing")
    _artifact(
        db, analysis, position=1, size_bytes=500, processed_bytes=500, status="duplicate",
        duplicate_of_artifact_id=1,
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    # 500 / 1000 = 50% - driven only by the real, still-ingesting artifact.
    assert state["progress"] == 50


def test_resource_limited_and_processing_error_artifacts_do_not_block_the_99_transition():
    """A resource_limited/processing_error artifact is just as terminal as
    duplicate/unsupported - it will never reach status="completed" either,
    so it must not permanently block "is ingestion actually done" from
    ever becoming true for the rest of this analysis."""
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
    """A controlled artifact failure resets processed_bytes to 0 (see
    _record_controlled_artifact_failure) but its size_bytes is untouched -
    if it stayed in the denominator, the numerator could never catch up,
    permanently understating progress for the rest of a still-processing
    analysis, exactly like an unsupported/duplicate artifact would."""
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(db, analysis, position=0, size_bytes=1000, processed_bytes=500, status="processing")
    _artifact(
        db, analysis, position=1, size_bytes=500, processed_bytes=0, status="resource_limited",
    )

    state = analysis_task.compute_current_analysis_state(db, analysis)

    # 500 / 1000 = 50% - driven only by the real, still-ingesting artifact.
    assert state["progress"] == 50


def test_large_unsupported_artifact_does_not_suppress_progress():
    """Before Additional Requirement B, a huge skipped artifact sitting in
    the denominator could pin visible progress near 0% for the entire
    ingestion of the real, small artifact. It must not."""
    db, user = _session()
    analysis = _analysis(db, user, status="processing")
    _artifact(
        db, analysis, position=0, size_bytes=1_000_000_000, processed_bytes=0,
        status="unsupported",
    )
    _artifact(db, analysis, position=1, size_bytes=100, processed_bytes=100, status="processing")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["progress"] == 98


# --- completed / failed -----------------------------------------------


def test_completed_analysis_reports_99_never_100_and_status_completed():
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    _artifact(db, analysis, position=0, size_bytes=10, processed_bytes=10, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "completed"
    assert state["progress"] == 99
    assert state["progress"] != 100


def test_completed_analysis_must_not_misleadingly_restart_progress_animation():
    """A completed analysis must be immediately distinguishable from a
    fresh 0% start - status alone (not just the number) carries that."""
    db, user = _session()
    analysis = _analysis(db, user, status="completed")
    _artifact(db, analysis, position=0, size_bytes=10, processed_bytes=10, status="completed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "completed"  # not "processing"/"pending"


def test_failed_analysis_reports_failed_status():
    db, user = _session()
    analysis = _analysis(db, user, status="failed")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state["status"] == "failed"


# --- completed analysis includes the reconstructed final result -----------


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
    # Both rows share trace_id at the exact SAME timestamp - a real
    # relationship, but with no signal establishing which one came first,
    # so it is an association, not a fabricated causal edge.
    assert len(result["components"][0]["nodes"]) == 2
    assert result["components"][0]["associations"]
    assert result["components"][0]["edges"] == []


# --- Reconnect must not lose already-known artifact outcomes --------------


def test_reconnect_snapshot_includes_already_known_unsupported_and_duplicate_outcomes():
    """Redis pub/sub has no replay - if a client connects AFTER an
    upload-time unsupported/duplicate outcome was already published live,
    it must still see it via the initial snapshot, not just once the whole
    analysis eventually completes."""
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
    # The still-processing canonical artifact's outcome is NOT terminal yet
    # - it must not appear as if it were already decided.
    assert "a.log" not in outcomes


def test_reconnect_snapshot_includes_known_zero_evidence_artifact_outcome():
    """A per-artifact zero-evidence outcome (published live once that one
    artifact finishes ingestion, independent of the whole analysis) is
    subject to the exact same missed-event race."""
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
