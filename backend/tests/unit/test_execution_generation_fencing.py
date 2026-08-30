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


def test_workflow_publish_failure_returns_analysis_to_recoverable_pending(monkeypatch):
    """Interleaving F (child workflow publish failure): process_analysis's
    own pending->processing claim succeeds and establishes generation 1,
    but publishing this generation's only child work (the artifact-group/
    chord) to the broker then raises. Left alone, the Analysis would be
    stuck "processing" forever with zero children ever dispatched. This
    must instead: fence generation 1 (return the row to "pending"), never
    silently swallow the failure without doing so, and attempt exactly one
    immediate redispatch - which itself establishes a fresh generation 2
    once it re-claims, proving recovery can dispatch a genuine replacement
    rather than resurrecting generation 1."""
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(RuntimeError("broker connection refused")),
    )

    redispatched = []
    monkeypatch.setattr(
        analysis_task.process_analysis, "delay", lambda aid: redispatched.append(aid)
    )

    analysis_task.process_analysis.run(analysis_id)

    assert redispatched == [analysis_id]
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    # Fenced back to a genuinely recoverable "pending" - never left
    # "processing" with generation 1 established but nothing ever
    # dispatched for it.
    assert reloaded.status == "pending"
    assert reloaded.finalization_generation is None
    reloaded_artifact = (
        db.query(AnalysisArtifact).filter(AnalysisArtifact.analysis_id == analysis_id).first()
    )
    assert reloaded_artifact.status == "pending"  # never falsely claimed

    # The redispatch this triggered (delivered as a genuinely separate task
    # invocation in production, simulated here as a second, later .run())
    # re-claims cleanly and establishes an actually-new generation - never
    # generation 1 resurrected as still "processing" without ever having
    # published anything.
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: [sig for sig in sigs],
    )
    monkeypatch.setattr(
        analysis_task, "chord",
        lambda group_obj, callback: SimpleNamespace(apply_async=lambda: None),
    )
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", lambda *a: SimpleNamespace())
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", lambda *a: SimpleNamespace())

    analysis_task.process_analysis.run(analysis_id)

    db.expire_all()
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "processing"
    assert reloaded.processing_generation == 2


def test_workflow_publish_failure_when_redispatch_also_fails_leaves_analysis_pending(monkeypatch):
    """The degraded but still-safe outcome: both the original publish AND
    the one immediate redispatch attempt fail (e.g. a sustained broker
    outage). The Analysis must be left durably "pending" - never stuck
    "processing" with no children - so the existing 30-minute
    stale-pending recovery net can still pick it up later."""
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(RuntimeError("broker connection refused")),
    )
    monkeypatch.setattr(
        analysis_task.process_analysis, "delay",
        lambda aid: (_ for _ in ()).throw(RuntimeError("still down")),
    )

    analysis_task.process_analysis.run(analysis_id)  # must not raise

    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "pending"
    assert reloaded.finalization_generation is None


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


# --- Interleaving B: controlled artifact failure races a real cancel -------


def test_controlled_failure_commit_loses_to_a_cancel_that_lands_first(monkeypatch):
    """A REAL interleaving using two independent sessions bound to the SAME
    engine, not a pre-set stale value: worker W is mid-flight recording a
    controlled artifact failure (already past its OWN generation-ownership
    read, about to commit) when a completely separate session commits a
    real cancellation first. W's own commit must then be rejected -
    cancelled must remain immutable, and W must never alter the artifact
    or Evidence after that tombstone has landed."""
    session_factory = _engine_session_factory()
    worker_db = session_factory()
    alice = _user(worker_db)
    analysis = _analysis(worker_db, alice, status="processing", processing_generation=1)
    artifact = _artifact(worker_db, analysis, status="processing")
    worker_db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
    ))
    worker_db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id

    # Worker W's OWN ownership read (matching what
    # _record_controlled_artifact_failure does internally) - confirms
    # ownership looks valid to W at this point in time, BEFORE the
    # concurrent cancel below lands.
    owned = (
        worker_db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis_id)
        .first()
    )
    assert owned == ("processing", 1)

    # A completely independent session now cancels the analysis for real,
    # landing its tombstone commit BEFORE worker W's own controlled-
    # failure commit below.
    cancel_db = session_factory()
    result = cancel_analysis_and_cleanup(cancel_db, analysis_id)
    assert result == "processing"
    cancel_db.close()

    # Worker W, unaware, now proceeds with what it believes is still a
    # valid controlled-failure recording for generation 1.
    _record_controlled_artifact_failure(
        db=worker_db, analysis_id=analysis_id, artifact_id=artifact_id, generation=1,
        status="processing_error", reason="parser exploded",
    )

    verify_db = session_factory()
    reloaded_analysis = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    reloaded_artifact = (
        verify_db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    )
    assert reloaded_analysis.status == "cancelled"  # immutable - W never touched it
    assert reloaded_artifact.status == "processing"  # never flipped to processing_error by W
    assert reloaded_artifact.failure_reason is None
    # cancel_analysis_and_cleanup's own Evidence wipe already ran - W's
    # controlled-failure path (which would ALSO have deleted this
    # artifact's Evidence) must never have gotten the chance to run either
    # way, but this confirms the row is gone through the correct owner.
    assert verify_db.query(Evidence).filter(Evidence.artifact_id == artifact_id).count() == 0


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
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
    )

    assert won is False
    db.expire_all()
    assert analysis.status == "processing"  # never resurrected to completed
    assert analysis.result_snapshot is None


# --- Interleaving E: finalizer generation loss right before Gemini --------


def test_stale_finalizer_never_calls_gemini_once_a_concurrent_cancel_lands_first(monkeypatch):
    """A REAL interleaving: the finalizer has already won its own
    finalization claim and is partway through finalizing (past identity
    persistence, about to build the LLM context and call Gemini) when a
    completely independent session commits a real cancellation. The
    finalizer's OWN "before Gemini" ownership check (_finalizer_owns_
    generation) must catch this and stop it - generate_investigation_
    explanation must never be called by this now-stale finalizer, and the
    cancelled tombstone must remain untouched."""
    session_factory = _engine_session_factory()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)

    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1, source_kind=None)
    artifact = _artifact(db, analysis, status="completed", last_processed_line=1, processed_bytes=10)
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
        fingerprint="fp-1", first_line_number=1, last_line_number=1, severity="ERROR",
    ))
    db.commit()
    analysis_id = analysis.id
    db.close()

    gemini_calls = []
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation",
        lambda ctx: gemini_calls.append(ctx) or (_ for _ in ()).throw(
            AssertionError("a stale finalizer must never reach Gemini")
        ),
    )

    real_persist_resolved_identities = analysis_task.persist_resolved_identities

    def persist_identities_then_cancel(*args, **kwargs):
        # The interleaving point: right after identity persistence (a real
        # step the finalizer takes before ever reaching Gemini), an
        # entirely independent session cancels the analysis for real.
        result = real_persist_resolved_identities(*args, **kwargs)
        cancel_db = session_factory()
        cancel_analysis_and_cleanup(cancel_db, analysis_id)
        cancel_db.close()
        return result

    monkeypatch.setattr(
        analysis_task, "persist_resolved_identities", persist_identities_then_cancel
    )

    analysis_task._finalize_analysis_task.run([1], analysis_id, 1, None)

    assert gemini_calls == []
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    assert reloaded.ai_analysis is None
    assert verify_db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 0


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
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
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
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
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
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
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
        result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
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
