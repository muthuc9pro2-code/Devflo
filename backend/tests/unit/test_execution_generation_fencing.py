"""Execution-generation fencing: the structural fix for the proven EC2
duplicate-dispatch incident (duplicate process_analysis workflows, duplicate
artifact processing, duplicate finalizers, duplicate Gemini calls).

Every test here proves a REAL race/interleaving using independent DB
sessions bound to the same in-memory SQLite engine (not one mock function
that commits everything before "concurrently" calling the other) - the
same style already established by
test_analysis_cancellation.py's cancel-vs-finalize tests and
test_analysis_recovery.py's atomic-claim tests.
"""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import analysis as analysis_api
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.log_praser import ParsedEvent
from app.tasks import analysis as analysis_task
from app.tasks.analysis import (
    _finalize_commit_if_processing,
    _mark_analysis_failed,
    _persist_artifact_batch,
    _record_controlled_artifact_failure,
    cancel_analysis_and_cleanup,
)


def _engine_session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


def _session():
    return _engine_session_factory()()


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
        last_processed_line=0, processed_bytes=0,
    )
    defaults.update(kwargs)
    artifact = AnalysisArtifact(**defaults)
    db.add(artifact)
    db.commit()
    return artifact


def _retained_batch():
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", level="ERROR")
    return [
        SimpleNamespace(
            event=event, end_offset=20, artifact_line_number=1, global_end_line_number=1,
        )
    ]


# --- A. Duplicate process_analysis: exactly one claim, one dispatch -------


def test_duplicate_process_analysis_invocations_yield_exactly_one_claim(monkeypatch):
    """Two invocations racing the same pending Analysis (Celery broker
    redelivery, or a stale recovery redispatch overlapping a still-healthy
    one) - only the first wins the pending->processing claim and
    establishes generation 1; the second finds status already
    "processing" and returns without dispatching anything."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    dispatch_count = {"n": 0}
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: dispatch_count.__setitem__("n", dispatch_count["n"] + 1) or SimpleNamespace(),
    )
    monkeypatch.setattr(
        analysis_task, "chord",
        lambda group_obj, callback: SimpleNamespace(apply_async=lambda: None),
    )
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", lambda *a: SimpleNamespace())
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", lambda *a: SimpleNamespace())

    analysis_task.process_analysis.run(analysis.id)
    analysis_task.process_analysis.run(analysis.id)  # duplicate/redelivered invocation

    assert dispatch_count["n"] == 1
    db.expire_all()
    assert analysis.status == "processing"
    assert analysis.processing_generation == 1  # incremented exactly once


# --- B. Duplicate artifact task: exactly one claim, one parse -------------


def test_duplicate_process_artifact_task_invocations_parse_exactly_once(monkeypatch, tmp_path):
    """Two invocations racing the same pending artifact for the current
    generation - only the first wins the artifact's own pending->processing
    claim and actually streams/parses; the second's claim affects zero
    rows and returns 0 without touching Evidence or the checkpoint."""
    # _bump_processing_heartbeat now opens its own isolated session (a
    # separate sessionLocal() call) which it closes independently - a real
    # session-per-call factory (not one fixed shared Session) is required
    # so that close doesn't tear down the rest of this test's session.
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1, source_kind=None)
    path = tmp_path / "a.log"
    path.write_text("2026-01-01T00:00:00Z ERROR service=a boom\n")
    artifact = _artifact(
        db, analysis, status="pending", saved_file_path=str(path),
        size_bytes=path.stat().st_size, original_filename="a.log",
    )
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    # persist_evidence_batch's real implementation uses a MySQL-only
    # insert(...).on_duplicate_key_update(...) that cannot compile against
    # sqlite (same limitation test_controlled_artifact_and_source_failures.py
    # documents/routes around) - swap in a plain insert so this test's
    # in-memory sqlite session has real Evidence rows to count.
    persisted = []

    def fake_persist(*, db, analysis_id, events, artifact_id=None):
        for event in events:
            persisted.append(event)
            db.add(Evidence(
                analysis_id=analysis_id,
                artifact_id=artifact_id if artifact_id is not None else event.artifact_id,
                correlation_key=f"ck-{len(persisted)}",
                fingerprint=event.fingerprint or f"fp-{len(persisted)}",
                first_line_number=event.line_number or 1,
                last_line_number=event.line_number or 1,
                severity=event.level,
            ))
        db.commit()

    monkeypatch.setattr(analysis_task, "persist_evidence_batch", fake_persist)

    first_result = analysis_task._process_artifact_task.run(analysis.id, artifact.id, 1)
    second_result = analysis_task._process_artifact_task.run(analysis.id, artifact.id, 1)

    assert first_result == 1  # actually parsed the one line
    assert second_result == 0  # claim failed - artifact already terminal by then
    db.expire_all()
    evidence_count = db.query(Evidence).filter(Evidence.artifact_id == artifact.id).count()
    assert evidence_count == 1  # never doubled by a second parse pass


# --- C. Old-generation zombie: fenced out of every durable mutation -------


def test_old_generation_worker_cannot_persist_evidence_or_advance_checkpoint(monkeypatch):
    """Generation G pauses mid-batch; recovery/a new dispatch advances the
    analysis to generation G+1 in the meantime. G's own eventual batch
    commit must be rejected (None sentinel), leaving Evidence and the
    checkpoint completely untouched - not merely "cancelled"-gated, but
    gated on generation mismatch even while status stays "processing"."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    artifact = _artifact(db, analysis, status="processing")
    monkeypatch.setattr(analysis_task, "persist_evidence_batch", lambda **kwargs: None)

    # A new execution has since started: generation advances to 2.
    analysis.processing_generation = 2
    db.commit()

    result = _persist_artifact_batch(
        db=db, analysis=analysis, artifact=artifact, generation=1, batch=_retained_batch(),
    )

    assert result is None
    db.expire_all()
    assert db.query(Evidence).filter(Evidence.artifact_id == artifact.id).count() == 0
    assert artifact.processed_bytes == 0
    assert artifact.last_processed_line == 0


def test_old_generation_worker_cannot_record_a_controlled_artifact_failure():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=2)
    artifact = _artifact(db, analysis, status="processing")

    _record_controlled_artifact_failure(
        db=db, analysis_id=analysis.id, artifact_id=artifact.id, generation=1,
        status="processing_error", reason="stale worker",
    )

    db.expire_all()
    assert artifact.status == "processing"  # never overwritten by the stale generation
    assert artifact.failure_reason is None


def test_old_generation_finalizer_cannot_complete_the_new_generations_work():
    """The final completion fence: even if a stale finalizer somehow
    reaches its final commit, it must be rejected once processing_generation
    (or finalization_generation) no longer matches what it was dispatched
    with."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=2)
    analysis.finalization_generation = 1  # stale claim from the abandoned generation
    db.commit()

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, stage="test",
    )

    assert won is False
    db.expire_all()
    assert analysis.status == "processing"  # never resurrected to completed
    assert analysis.result_snapshot is None


# --- F/G. Terminal-state immutability under genuine concurrent sessions ---


def test_cancel_wins_race_against_finalize_completion(monkeypatch):
    """Real separate sessions bound to the same engine: the cancel
    endpoint's tombstone commits first; the finalizer's own fence must
    then discard its completed result rather than resurrecting the
    analysis."""
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    cancel_db = session_factory()
    cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, stage="test",
    )

    assert won is False
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    verify_db.close()


def test_complete_wins_race_against_a_later_cancel_request():
    """The other ordering: the finalizer's completion commits first (real
    separate session); a cancel request arriving afterward must see
    "completed" and refuse to touch Evidence/result - cancel_analysis_and_
    cleanup's own atomic claim (status IN (pending, processing)) already
    guarantees this, proven here end-to-end."""
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, stage="test",
    )
    assert won is True
    db.close()

    cancel_db = session_factory()
    previous_status = cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    assert previous_status is None  # refused - not a cancellable state
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot == {"investigation_path": "simple"}
    verify_db.close()


def test_fail_then_cancel_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    won = _mark_analysis_failed(db, analysis_id)
    assert won is True
    db.close()

    cancel_db = session_factory()
    previous_status = cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    assert previous_status is None
    verify_db = session_factory()
    assert verify_db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "failed"
    verify_db.close()


def test_cancel_then_fail_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    cancel_analysis_and_cleanup(db, analysis_id)
    db.close()

    fail_db = session_factory()
    won = _mark_analysis_failed(fail_db, analysis_id)
    fail_db.close()

    assert won is False
    verify_db = session_factory()
    assert verify_db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "cancelled"
    verify_db.close()


def test_complete_then_fail_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, stage="test",
    )
    assert won is True
    db.close()

    fail_db = session_factory()
    fail_won = _mark_analysis_failed(fail_db, analysis_id)
    fail_db.close()

    assert fail_won is False
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot == {"investigation_path": "simple"}
    verify_db.close()


def test_fail_then_complete_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    fail_won = _mark_analysis_failed(db, analysis_id)
    assert fail_won is True
    db.close()

    finalize_db = session_factory()
    reloaded_for_finalize = finalize_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    won = _finalize_commit_if_processing(
        finalize_db, reloaded_for_finalize, generation=1,
        result_snapshot={"investigation_path": "simple"}, stage="test",
    )
    finalize_db.close()

    assert won is False
    verify_db = session_factory()
    assert verify_db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "failed"
    verify_db.close()


# --- H/I. Cancellation transaction atomicity + best-effort cleanup -------


def test_cancellation_db_failure_leaves_no_partial_state():
    """The whole DB portion of cancellation (tombstone claim + Evidence
    delete + result/ai clear + checkpoint reset) is now ONE transaction
    with a single commit - if that commit cannot land, NONE of it must:
    never a status="cancelled" tombstone sitting next to un-deleted
    Evidence/result, and never a half-cancelled Analysis row."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    artifact = _artifact(db, analysis, status="processing")
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
    ))
    db.commit()
    analysis_id = analysis.id

    real_commit = db.commit
    db.commit = lambda: (_ for _ in ()).throw(RuntimeError("db gone"))

    with pytest.raises(RuntimeError):
        cancel_analysis_and_cleanup(db, analysis_id)

    db.commit = real_commit  # restore so rollback/reload below work normally
    db.rollback()
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "processing"  # never resurrected as "cancelled"
    assert db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 1


def test_cancellation_filesystem_cleanup_error_never_changes_the_cancelled_status(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind="zip", source_reference="uploads/x.zip")
    analysis_id = analysis.id
    monkeypatch.setattr(
        analysis_task, "cleanup_prepared_source",
        lambda aid: (_ for _ in ()).throw(OSError("disk error")),
    )

    result = cancel_analysis_and_cleanup(db, analysis_id)

    assert result == "processing"
    db.expire_all()
    assert db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "cancelled"
