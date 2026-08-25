"""Analysis lifecycle - cancellation, cleanup, race safety.
Recovery/orphan-handling tests live in
test_analysis_recovery.py; the live/reconnect SSE "cancelled" terminal
state is covered in test_analysis_stream_reconnect.py alongside
the existing completed/failed terminal-state tests it already has.

Endpoint functions are called directly (db/current_user/response passed
in explicitly), matching this repo's established pattern (see
test_analysis_history_api.py) rather than a FastAPI TestClient.
"""
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import analysis as analysis_api
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.tasks import analysis as analysis_task
from app.tasks.analysis import (
    _mark_analysis_failed,
    _persist_artifact_batch,
    cancel_analysis_and_cleanup,
)


# --- shared fixtures --------------------------------------------------------


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _user(db, name="alice") -> User:
    user = User(username=name, email=f"{name}@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    return user


def _analysis(db, user, *, status="pending", **kwargs) -> Analysis:
    defaults = dict(
        user_id=user.id, original_filename="a.log", saved_file_path="/uploads/a.log",
        status=status,
    )
    defaults.update(kwargs)
    analysis = Analysis(**defaults)
    db.add(analysis)
    db.commit()
    return analysis


def _artifact(db, analysis, position=0, status="pending", **kwargs) -> AnalysisArtifact:
    defaults = dict(
        analysis_id=analysis.id, position=position, original_filename=f"f{position}.log",
        saved_file_path=f"/uploads/f{position}.log", size_bytes=100, status=status,
        last_processed_line=3, processed_bytes=40,
    )
    defaults.update(kwargs)
    artifact = AnalysisArtifact(**defaults)
    db.add(artifact)
    db.commit()
    return artifact


def _evidence(db, analysis, artifact, correlation_key="k1", fingerprint="fp1") -> Evidence:
    evidence = Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key=correlation_key,
        fingerprint=fingerprint, first_line_number=1, last_line_number=1,
    )
    db.add(evidence)
    db.commit()
    return evidence


# --- "cancelled" is a real terminal enum state -------------------------


def test_cancelled_is_a_persistable_status_value_alongside_the_new_heartbeat_column():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled", processing_heartbeat_at=None)

    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.processing_heartbeat_at is None


# --- POST /analysis/{id}/cancel endpoint behavior -----------------------


def test_cancel_pending_analysis_succeeds(monkeypatch):
    monkeypatch.setattr(analysis_api, "publish_analysis_event", lambda *a, **k: None)
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending")

    result = analysis_api.cancel_analysis(analysis.id, db=db, current_user=alice)

    assert result == {"analysis_id": analysis.id, "status": "cancelled"}
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


def test_cancel_processing_analysis_succeeds(monkeypatch):
    monkeypatch.setattr(analysis_api, "publish_analysis_event", lambda *a, **k: None)
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")

    result = analysis_api.cancel_analysis(analysis.id, db=db, current_user=alice)

    assert result["status"] == "cancelled"
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


def test_cancel_already_cancelled_analysis_is_idempotent(monkeypatch):
    published = []
    monkeypatch.setattr(
        analysis_api, "publish_analysis_event", lambda *a, **k: published.append(a)
    )
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")

    result = analysis_api.cancel_analysis(analysis.id, db=db, current_user=alice)

    assert result == {"analysis_id": analysis.id, "status": "cancelled"}
    # Idempotent success returns early - no second cleanup/publish cycle.
    assert published == []


def test_cancel_completed_analysis_is_rejected_safely():
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="completed", result_snapshot={"investigation_path": "zero_evidence"},
    )

    with pytest.raises(HTTPException) as error:
        analysis_api.cancel_analysis(analysis.id, db=db, current_user=alice)

    assert error.value.status_code == 409
    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot is not None  # untouched


def test_cancel_failed_analysis_is_rejected_not_reinterpreted_as_cancelled():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="failed")

    with pytest.raises(HTTPException) as error:
        analysis_api.cancel_analysis(analysis.id, db=db, current_user=alice)

    assert error.value.status_code == 409
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "failed"


def test_cancel_nonexistent_analysis_404s():
    db = _session()
    alice = _user(db)

    with pytest.raises(HTTPException) as error:
        analysis_api.cancel_analysis(999999, db=db, current_user=alice)

    assert error.value.status_code == 404


def test_cancel_another_users_analysis_404s_indistinguishably():
    db = _session()
    alice = _user(db, "alice")
    bob = _user(db, "bob")
    bobs_analysis = _analysis(db, bob, status="pending")

    with pytest.raises(HTTPException) as error:
        analysis_api.cancel_analysis(bobs_analysis.id, db=db, current_user=alice)

    assert error.value.status_code == 404
    # Never actually cancelled by the non-owner's attempt.
    assert db.query(Analysis).filter(Analysis.id == bobs_analysis.id).first().status == "pending"


def test_cancel_response_leaks_no_internal_fields(monkeypatch):
    monkeypatch.setattr(analysis_api, "publish_analysis_event", lambda *a, **k: None)
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="pending", saved_file_path="/uploads/super-secret-internal-path.log",
    )

    result = analysis_api.cancel_analysis(analysis.id, db=db, current_user=alice)

    assert set(result.keys()) == {"analysis_id", "status"}


# --- Cancel endpoint false-success race: the response must reflect what
# cancel_analysis_and_cleanup() actually observed, not the endpoint's stale
# initial read -------------------------------------------------------------


def test_cancel_endpoint_does_not_report_cancelled_when_completion_wins_the_race(monkeypatch):
    """The endpoint's initial read sees "processing", but a competing
    lifecycle transition (the finalizer) commits "completed" before
    cancel_analysis_and_cleanup gets to run its own re-check - the helper
    correctly returns None in that case (see
    test_cancel_and_cleanup_returns_none_for_a_non_cancellable_analysis).
    The endpoint used to ignore that return value and report "cancelled"
    anyway; it must now report the state that actually won instead.
    Simulated by having the patched helper perform the competing
    transition itself, then delegate to the real function so its actual
    None-returning behavior is still exercised end to end."""
    published = []
    monkeypatch.setattr(
        analysis_api, "publish_analysis_event", lambda *a, **k: published.append(a)
    )
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    real_cancel_and_cleanup = cancel_analysis_and_cleanup

    def racing_cancel_and_cleanup(db_arg, aid):
        # Simulates the finalizer's own transaction winning between the
        # endpoint's initial read (already done, above) and this call.
        db_arg.query(Analysis).filter(Analysis.id == aid).update({"status": "completed"})
        db_arg.commit()
        return real_cancel_and_cleanup(db_arg, aid)

    monkeypatch.setattr(analysis_api, "cancel_analysis_and_cleanup", racing_cancel_and_cleanup)

    with pytest.raises(HTTPException) as error:
        analysis_api.cancel_analysis(analysis_id, db=db, current_user=alice)

    assert error.value.status_code == 409
    assert "Completed" in error.value.detail
    assert published == []  # no cancelled SSE event for a completed analysis
    assert db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "completed"


def test_cancel_endpoint_does_not_report_cancelled_when_failure_wins_the_race(monkeypatch):
    monkeypatch.setattr(analysis_api, "publish_analysis_event", lambda *a, **k: None)
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    real_cancel_and_cleanup = cancel_analysis_and_cleanup

    def racing_cancel_and_cleanup(db_arg, aid):
        db_arg.query(Analysis).filter(Analysis.id == aid).update({"status": "failed"})
        db_arg.commit()
        return real_cancel_and_cleanup(db_arg, aid)

    monkeypatch.setattr(analysis_api, "cancel_analysis_and_cleanup", racing_cancel_and_cleanup)

    with pytest.raises(HTTPException) as error:
        analysis_api.cancel_analysis(analysis_id, db=db, current_user=alice)

    assert error.value.status_code == 409
    assert "Failed" in error.value.detail
    assert db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "failed"


def test_cancel_endpoint_is_idempotent_when_another_request_wins_the_cancel_race(monkeypatch):
    """A losing racer (another concurrent cancel request already won) must
    still report the same idempotent "cancelled" success, not an error -
    and must not publish a second/duplicate cancelled event."""
    published = []
    monkeypatch.setattr(
        analysis_api, "publish_analysis_event", lambda *a, **k: published.append(a)
    )
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    real_cancel_and_cleanup = cancel_analysis_and_cleanup

    def racing_cancel_and_cleanup(db_arg, aid):
        real_cancel_and_cleanup(db_arg, aid)  # another request cancels first
        return None  # this call's own attempt loses the race

    monkeypatch.setattr(analysis_api, "cancel_analysis_and_cleanup", racing_cancel_and_cleanup)

    result = analysis_api.cancel_analysis(analysis_id, db=db, current_user=alice)

    assert result == {"analysis_id": analysis_id, "status": "cancelled"}
    assert published == []
    assert db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "cancelled"


# --- Cleanup ordering, cross-analysis isolation, race safety ------------


def test_cancel_and_cleanup_deletes_evidence_only_for_this_analysis():
    db = _session()
    alice = _user(db)
    target = _analysis(db, alice, status="processing")
    target_artifact = _artifact(db, target, status="processing")
    _evidence(db, target, target_artifact)
    _evidence(db, target, target_artifact, correlation_key="k2", fingerprint="fp2")

    other = _analysis(db, alice, status="processing")
    other_artifact = _artifact(db, other, status="processing")
    _evidence(db, other, other_artifact)

    cancel_analysis_and_cleanup(db, target.id)

    assert db.query(Evidence).filter(Evidence.analysis_id == target.id).count() == 0
    # A completely unrelated analysis's Evidence is never touched.
    assert db.query(Evidence).filter(Evidence.analysis_id == other.id).count() == 1
    assert db.query(Analysis).filter(Analysis.id == other.id).first().status == "processing"


def test_cancel_and_cleanup_clears_ai_analysis_and_result_snapshot():
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        ai_analysis={"title": "stale"}, result_snapshot={"investigation_path": "simple"},
    )

    cancel_analysis_and_cleanup(db, analysis.id)

    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.ai_analysis is None
    assert reloaded.result_snapshot is None


def test_cancel_and_cleanup_resets_only_abandoned_in_flight_artifacts():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    abandoned = _artifact(
        db, analysis, position=0, status="processing",
        processed_bytes=500, last_processed_line=12,
        fallback_context={"kind": "text", "text": "x"}, failure_reason=None,
    )
    terminal = _artifact(
        db, analysis, position=1, status="completed",
        processed_bytes=1000, last_processed_line=30,
    )
    controlled_failure = _artifact(
        db, analysis, position=2, status="resource_limited",
        processed_bytes=0, last_processed_line=0, failure_reason="too large",
    )

    cancel_analysis_and_cleanup(db, analysis.id)

    db.expire_all()
    assert abandoned.processed_bytes == 0
    assert abandoned.last_processed_line == 0
    assert abandoned.fallback_context is None
    # Terminal outcomes are a truthful historical record - left exactly as-is.
    assert terminal.processed_bytes == 1000
    assert terminal.last_processed_line == 30
    assert terminal.status == "completed"
    assert controlled_failure.status == "resource_limited"
    assert controlled_failure.failure_reason == "too large"


def test_cancel_and_cleanup_keeps_the_analysis_and_artifact_rows():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending")
    artifact = _artifact(db, analysis, status="pending")

    cancel_analysis_and_cleanup(db, analysis.id)

    assert db.query(Analysis).filter(Analysis.id == analysis.id).first() is not None
    assert db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact.id).first() is not None


def test_cancel_and_cleanup_returns_none_for_a_non_cancellable_analysis():
    db = _session()
    alice = _user(db)
    completed = _analysis(db, alice, status="completed")

    assert cancel_analysis_and_cleanup(db, completed.id) is None
    assert db.query(Analysis).filter(Analysis.id == completed.id).first().status == "completed"


def test_cancel_and_cleanup_returns_the_original_status():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")

    assert cancel_analysis_and_cleanup(db, analysis.id) == "processing"


# --- The cancel-vs-Evidence-commit race, closed by the per-batch fence --


def _retained_batch():
    """One evidence-worthy ParsedEvent wrapped the same
    SimpleNamespace(event=..., end_offset=..., artifact_line_number=...)
    shape _process_artifact's real record objects have - matches the
    established pattern in test_multifile_processing.py."""
    from types import SimpleNamespace

    from app.services.log_praser import ParsedEvent

    event = ParsedEvent(line_number=1, raw_line="ERROR failure", level="ERROR")
    return [
        SimpleNamespace(
            event=event, end_offset=20, artifact_line_number=1, global_end_line_number=1,
        )
    ]


def test_persist_artifact_batch_rolls_back_and_signals_cancellation_when_already_cancelled(monkeypatch):
    """The fence inside _persist_artifact_batch: by the time a batch is
    ready to persist, the analysis has already been committed cancelled -
    the batch must not commit any Evidence and must signal the caller
    (via the None sentinel) to stop, not merely skip this one batch."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")
    artifact = _artifact(db, analysis, status="processing")
    persist_calls = []
    monkeypatch.setattr(
        analysis_task, "persist_evidence_batch", lambda **kwargs: persist_calls.append(kwargs)
    )
    original_processed_bytes = artifact.processed_bytes

    batch_result = _persist_artifact_batch(
        db=db, analysis=analysis, artifact=artifact, batch=_retained_batch(),
    )

    assert batch_result is None
    assert persist_calls == []  # never reached persistence
    db.expire_all()
    # The checkpoint columns were never advanced for this abandoned batch.
    assert artifact.processed_bytes == original_processed_bytes


def test_persist_artifact_batch_persists_normally_when_not_cancelled(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    artifact = _artifact(db, analysis, status="processing", processed_bytes=0)
    persist_calls = []
    monkeypatch.setattr(
        analysis_task, "persist_evidence_batch", lambda **kwargs: persist_calls.append(kwargs)
    )

    batch_result = _persist_artifact_batch(
        db=db, analysis=analysis, artifact=artifact, batch=_retained_batch(),
    )

    assert batch_result == 1
    assert len(persist_calls) == 1
    assert len(persist_calls[0]["events"]) == 1
    db.expire_all()
    assert artifact.processed_bytes == 20  # advanced to the batch's end_offset


def test_cancellation_committed_after_a_batch_already_committed_is_still_caught_by_cleanup():
    """The other half of the race: a batch that commits its Evidence
    BEFORE the cancel tombstone wins is still fully reclaimed by
    cancel_analysis_and_cleanup's own DELETE, which runs after the
    tombstone commits and captures everything present at that moment."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    artifact = _artifact(db, analysis, status="processing")
    _evidence(db, analysis, artifact)  # simulates a batch that "won" the race

    cancel_analysis_and_cleanup(db, analysis.id)

    assert db.query(Evidence).filter(Evidence.analysis_id == analysis.id).count() == 0
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


# --- Cancellation checkpoints stop stale/redelivered work ---------------


def test_process_analysis_does_not_dispatch_for_an_already_cancelled_analysis(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)

    group_mock_calls = []
    monkeypatch.setattr(analysis_task, "group", lambda sigs: group_mock_calls.append(sigs))
    monkeypatch.setattr(analysis_task, "chord", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("must not build a chord for a cancelled analysis")
    ))

    analysis_task.process_analysis.run(analysis.id)

    assert group_mock_calls == []
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


def test_process_artifact_task_returns_immediately_for_cancelled_analysis(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")
    artifact = _artifact(db, analysis, status="pending")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(
        analysis_task, "_process_artifact",
        lambda **k: (_ for _ in ()).throw(AssertionError("must never parse a cancelled artifact")),
    )

    result = analysis_task._process_artifact_task.run(analysis.id, artifact.id)

    assert result == 0
    assert db.query(Evidence).filter(Evidence.analysis_id == analysis.id).count() == 0


def test_prepare_source_task_skips_when_already_cancelled(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled", source_kind="github", source_status=None)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(
        analysis_task, "_prepare_source_index",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must never prepare source for cancelled")),
    )

    analysis_task._prepare_source_task.run(analysis.id)

    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.source_status is None  # never set to "ready" or "unavailable"


def test_prepare_source_task_discards_prepared_source_when_cancelled_mid_prep(monkeypatch):
    """Cancellation observed by the fresh re-check performed
    right after preparation but before persisting source_status="ready"."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind="zip", source_status=None)
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)

    def fake_prepare(analysis_arg):
        # Simulate the cancel endpoint racing in while the (real,
        # slow) source prep call above was running.
        db.query(Analysis).filter(Analysis.id == analysis_id).update({"status": "cancelled"})
        db.commit()

    monkeypatch.setattr(analysis_task, "_prepare_source_index", fake_prepare)
    cleanup_calls = []
    monkeypatch.setattr(
        analysis_task, "cleanup_prepared_source", lambda aid: cleanup_calls.append(aid)
    )

    analysis_task._prepare_source_task.run(analysis_id)

    assert cleanup_calls == [analysis_id]
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.source_status is None  # never flipped to "ready"


def test_prepare_source_task_does_not_mark_unavailable_for_a_cancelled_analysis(monkeypatch):
    from app.services.source_archive import SourceInputError

    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind="github", source_status=None)
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)

    def fake_prepare(analysis_arg):
        db.query(Analysis).filter(Analysis.id == analysis_id).update({"status": "cancelled"})
        db.commit()
        raise SourceInputError("repository not found")

    monkeypatch.setattr(analysis_task, "_prepare_source_index", fake_prepare)
    monkeypatch.setattr(analysis_task, "cleanup_prepared_source", lambda aid: None)

    analysis_task._prepare_source_task.run(analysis_id)

    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.source_status is None  # never "unavailable"


# --- Finalize checkpoints, Gemini-result discard, mark-failed guard -----


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def test_finalize_does_nothing_for_an_already_cancelled_analysis(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")
    analysis_id = analysis.id
    db.close()

    monkeypatch.setattr(
        analysis_task, "run_correlation",
        lambda **k: (_ for _ in ()).throw(AssertionError("must not correlate a cancelled analysis")),
    )
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation",
        lambda ctx: (_ for _ in ()).throw(AssertionError("must not call Gemini for a cancelled analysis")),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    db2.close()


def test_finalize_discards_a_gemini_result_when_cancelled_arrives_while_gemini_was_running(monkeypatch):
    """An in-flight Gemini call may finish naturally, but if
    cancellation is observed once it returns, the result must be thrown
    away, never persisted - proven on the zero-evidence/fallback path."""
    from app.schemas.gemini import GeminiInvestigationResponse

    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    _artifact(
        db, analysis, status="completed",
        fallback_context={"kind": "text", "text": "payment worker stops after restart"},
    )
    analysis_id = analysis.id
    db.close()

    fake_result = GeminiInvestigationResponse(
        title="t", summary="s", probable_root_causes=[], what_happened=[],
        source_code_findings=[], recommended_actions=[], uncertainties=[],
    )

    def fake_gemini(context):
        # The cancel endpoint races in while this "real" Gemini call is
        # still in flight.
        cancel_db = session_factory()
        cancel_analysis_and_cleanup(cancel_db, analysis_id)
        cancel_db.close()
        return fake_result

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", fake_gemini)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    published = []
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    assert published == []  # the (post-cancel) result was never published
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    assert reloaded.ai_analysis is None
    db2.close()


# --- Finalizer-vs-cancel transactional race ------------------------------
#
# test_finalize_discards_a_gemini_result_when_cancelled_arrives_while_
# gemini_was_running above already covers cancellation landing DURING the
# Gemini call, caught by the existing "after Gemini" _bail_if_cancelled()
# checkpoint. The tests below cover the separate, narrower gap AFTER that
# last ordinary checkpoint has already passed (seeing status ==
# "processing") and BEFORE the completed commit - the window
# _finalize_commit_if_processing()'s row lock exists to close.


def test_finalize_commit_if_processing_discards_when_cancellation_wins_in_the_gap(monkeypatch):
    """Direct test of the centralized helper every completed-persistence
    branch in _finalize_analysis_task() now goes through. Cancellation is
    committed from a second session bound to the SAME engine between
    loading the `analysis` ORM object and calling the helper - exactly
    representing a cancel endpoint request that lands in that window, not
    one already visible at the top of the finalize run."""
    from app.tasks.analysis import _finalize_commit_if_processing

    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id
    artifact = _artifact(db, analysis, status="completed")
    _evidence(db, analysis, artifact)

    # The finalizer's own in-memory pending change (set right after its own
    # Gemini call, before this final fence) - must never be flushed if
    # cancellation already won.
    analysis.ai_analysis = {"title": "stale, must never be persisted"}

    # The race: a second session commits the durable cancel tombstone (and
    # its Evidence cleanup) in the gap before the helper runs.
    cancel_db = session_factory()
    cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    won = _finalize_commit_if_processing(
        db, analysis, result_snapshot={"investigation_path": "simple"}, stage="test",
    )

    assert won is False
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"  # never resurrected to completed
    assert reloaded.result_snapshot is None
    assert reloaded.ai_analysis is None
    assert db2.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 0
    db2.close()


def test_finalize_commit_if_processing_commits_normally_when_not_cancelled(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    from app.tasks.analysis import _finalize_commit_if_processing

    won = _finalize_commit_if_processing(
        db, analysis, result_snapshot={"investigation_path": "zero_evidence"}, stage="test",
    )

    assert won is True
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot == {"investigation_path": "zero_evidence"}
    db2.close()


def test_finalize_zero_evidence_branch_completed_persistence_is_fenced(monkeypatch):
    """Representative branch coverage: the zero-evidence completed-
    persistence branch, with cancellation landing in the gap AFTER the
    "before correlation" checkpoint (the only one that runs on this
    branch) and BEFORE the final commit - injected via
    build_source_outcome_payload, the real call sandwiched between that
    checkpoint and _finalize_commit_if_processing on every branch."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id
    db.close()

    def fake_source_outcome(*args, **kwargs):
        cancel_db = session_factory()
        cancel_analysis_and_cleanup(cancel_db, analysis_id)
        cancel_db.close()
        return None

    monkeypatch.setattr(analysis_task, "build_source_outcome_payload", fake_source_outcome)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    published = []
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    assert published == []
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    db2.close()


def test_finalize_fallback_branch_completed_persistence_is_fenced(monkeypatch):
    """Representative branch coverage: the FALLBACK completed-persistence
    branch, with cancellation landing AFTER the "after Gemini (fallback)"
    checkpoint (Gemini itself returns successfully here, unlike the
    existing in-flight-cancellation test above) and BEFORE the final
    commit."""
    from app.schemas.gemini import GeminiInvestigationResponse

    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    _artifact(
        db, analysis, status="completed",
        fallback_context={"kind": "text", "text": "payment worker stops after restart"},
    )
    analysis_id = analysis.id
    db.close()

    fake_result = GeminiInvestigationResponse(
        title="t", summary="s", probable_root_causes=[], what_happened=[],
        source_code_findings=[], recommended_actions=[], uncertainties=[],
    )
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: fake_result
    )

    def fake_source_outcome(*args, **kwargs):
        cancel_db = session_factory()
        cancel_analysis_and_cleanup(cancel_db, analysis_id)
        cancel_db.close()
        return None

    monkeypatch.setattr(analysis_task, "build_source_outcome_payload", fake_source_outcome)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    published = []
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    assert published == []
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    assert reloaded.ai_analysis is None
    db2.close()


def test_mark_analysis_failed_never_overwrites_cancelled():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")

    _mark_analysis_failed(db, analysis.id)

    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


def test_mark_analysis_failed_never_overwrites_completed():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="completed")

    _mark_analysis_failed(db, analysis.id)

    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "completed"


def test_mark_analysis_failed_still_marks_a_genuinely_in_flight_analysis_failed():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")

    _mark_analysis_failed(db, analysis.id)

    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "failed"


# --- Durable-reconnect half: compute_current_analysis_state --------------


def test_compute_current_analysis_state_returns_a_small_terminal_cancelled_payload():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="cancelled")

    state = analysis_task.compute_current_analysis_state(db, analysis)

    assert state == {"analysis_id": analysis.id, "status": "cancelled"}
    assert "progress" not in state
    assert "investigation_result" not in state


# --- Cancelled analyses are excluded from History at the DB level -------


def test_history_excludes_cancelled_analyses():
    from fastapi import Response

    db = _session()
    alice = _user(db)
    cancelled = _analysis(db, alice, status="cancelled")
    pending = _analysis(db, alice, status="pending")

    page = analysis_api.get_analysis_history(db=db, current_user=alice, response=Response())

    ids = {item.analysis_id for item in page.items}
    assert pending.id in ids
    assert cancelled.id not in ids


def test_history_still_returns_every_other_status():
    from fastapi import Response

    db = _session()
    alice = _user(db)
    statuses = ["pending", "processing", "completed", "failed"]
    expected_ids = {
        _analysis(db, alice, status=status).id for status in statuses
    }

    page = analysis_api.get_analysis_history(db=db, current_user=alice, response=Response())

    assert {item.analysis_id for item in page.items} == expected_ids
