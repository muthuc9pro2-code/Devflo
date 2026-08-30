"""Orphan/stale-analysis recovery.
Explicit cancellation tests live in test_analysis_cancellation.py;
this file covers the other half of the analysis lifecycle: what happens
when a worker/Redis/machine is unexpectedly interrupted rather than the
user cancelling on purpose.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from celery.exceptions import Retry
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.tasks import analysis as analysis_task


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


def _analysis(db, user, *, status="processing", **kwargs) -> Analysis:
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


# --- The throttled heartbeat ----------------------------------------------


def _heartbeat_session_factory(monkeypatch):
    """_bump_processing_heartbeat now owns its own isolated session (see
    its docstring) rather than reusing the caller's - it calls
    sessionLocal() itself. Tests must therefore patch sessionLocal to a
    real FACTORY bound to one shared engine (so the heartbeat's own
    session reads/writes the same underlying data), not a single fixed
    Session instance, which would be closed out from under any other code
    still using it."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def test_bump_processing_heartbeat_writes_on_first_call(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)
    analysis_id = analysis.id

    analysis_task._bump_processing_heartbeat(analysis_id, analysis.processing_generation)

    db.expire_all()
    assert analysis.processing_heartbeat_at is not None


def test_bump_processing_heartbeat_is_throttled_within_the_minimum_interval(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)
    analysis_id, generation = analysis.id, analysis.processing_generation

    analysis_task._bump_processing_heartbeat(analysis_id, generation)
    db.expire_all()
    first_write = analysis.processing_heartbeat_at

    # A burst of further calls within the same throttle window (as a tight
    # batch loop would produce) must not write again - this is exactly
    # what keeps the heartbeat from recreating per-batch shared-row
    # contention.
    for _ in range(50):
        analysis_task._bump_processing_heartbeat(analysis_id, generation)

    db.expire_all()
    assert analysis.processing_heartbeat_at == first_write


def test_bump_processing_heartbeat_writes_again_after_the_interval_elapses(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    monkeypatch.setattr(analysis_task, "_HEARTBEAT_MIN_INTERVAL_SECONDS", 0.0)
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)
    analysis_id, generation = analysis.id, analysis.processing_generation

    analysis_task._bump_processing_heartbeat(analysis_id, generation)
    db.expire_all()
    first_write = analysis.processing_heartbeat_at

    analysis_task._bump_processing_heartbeat(analysis_id, generation)

    db.expire_all()
    assert analysis.processing_heartbeat_at is not None
    assert analysis.processing_heartbeat_at >= first_write


def test_bump_processing_heartbeat_does_not_touch_a_stale_generation(monkeypatch):
    """The conditional UPDATE's WHERE clause requires
    processing_generation == generation - an old generation's heartbeat
    call must not refresh a row a new generation now owns."""
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", processing_generation=2, processing_heartbeat_at=None,
    )
    analysis_id = analysis.id

    analysis_task._bump_processing_heartbeat(analysis_id, 1)  # stale generation

    db.expire_all()
    assert analysis.processing_heartbeat_at is None


def test_bump_processing_heartbeat_does_not_touch_a_terminal_analysis(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="completed", processing_heartbeat_at=None)
    analysis_id, generation = analysis.id, analysis.processing_generation

    analysis_task._bump_processing_heartbeat(analysis_id, generation)

    db.expire_all()
    assert analysis.processing_heartbeat_at is None


def test_bump_processing_heartbeat_failure_is_logged_and_swallowed(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    broken_db = Mock()
    broken_db.execute.side_effect = RuntimeError("db gone")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: broken_db)

    analysis_task._bump_processing_heartbeat(9, 0)  # must not raise

    broken_db.rollback.assert_called_once()
    broken_db.close.assert_called_once()


def test_bump_processing_heartbeat_throttle_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    monkeypatch.setattr(analysis_task, "_HEARTBEAT_THROTTLE_CACHE_MAX_ENTRIES", 3)
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analyses = [_analysis(db, alice, status="processing") for _ in range(5)]

    for analysis in analyses:
        analysis_task._bump_processing_heartbeat(analysis.id, analysis.processing_generation)

    assert len(analysis_task._last_heartbeat_write) == 3


def test_bump_processing_heartbeat_throttle_cache_keys_by_generation(monkeypatch):
    """After recovery advances processing_generation, a throttle entry the
    OLD generation left behind must not suppress the new generation's own
    first heartbeat write."""
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", processing_generation=1, processing_heartbeat_at=None,
    )
    analysis_id = analysis.id

    analysis_task._bump_processing_heartbeat(analysis_id, 1)
    db.expire_all()
    assert analysis.processing_heartbeat_at is not None

    # Recovery advances the generation; the throttle cache still holds a
    # fresh (analysis_id, 1) entry, but generation 2 has never written yet.
    analysis.processing_generation = 2
    analysis.processing_heartbeat_at = None
    db.commit()

    analysis_task._bump_processing_heartbeat(analysis_id, 2)

    db.expire_all()
    assert analysis.processing_heartbeat_at is not None  # not suppressed by generation 1's entry


def test_process_artifact_bumps_heartbeat_after_each_persisted_batch(monkeypatch, tmp_path):
    """A single genuinely long _process_artifact() run (many parser
    batches) must refresh the heartbeat at a coarse persisted-progress
    boundary, not just at task/stage entry - otherwise a healthy analysis
    stuck processing one huge artifact for longer than
    _STALE_ANALYSIS_THRESHOLD_SECONDS could look orphaned to recovery.
    create_batches()/_persist_artifact_batch() are faked to control the
    batch count directly, independent of what a real file would produce -
    the real _bump_processing_heartbeat() (and its real throttle/DB write)
    still run underneath the spy."""
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    session_factory = _heartbeat_session_factory(monkeypatch)
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)
    artifact_path = tmp_path / "a.log"
    artifact_path.write_text("2026-01-01 10:00:00 ERROR service=a boom\n")
    artifact = _artifact(
        db, analysis, status="processing", saved_file_path=str(artifact_path),
        size_bytes=artifact_path.stat().st_size, original_filename="a.log",
    )

    monkeypatch.setattr(analysis_task, "create_batches", lambda records: [["b1"], ["b2"], ["b3"]])
    monkeypatch.setattr(analysis_task, "_persist_artifact_batch", lambda **kwargs: 1)
    monkeypatch.setattr(analysis_task, "_publish_ingestion_progress", lambda **kwargs: -1)

    heartbeat_calls = []
    real_bump = analysis_task._bump_processing_heartbeat

    def spy_bump(analysis_id, generation):
        heartbeat_calls.append(analysis_id)
        return real_bump(analysis_id, generation)

    monkeypatch.setattr(analysis_task, "_bump_processing_heartbeat", spy_bump)

    parsed_count = analysis_task._process_artifact(
        db=db, analysis=analysis, artifact=artifact, generation=0
    )

    # Invoked once per persisted batch (the cheap Python call this section
    # requires)...
    assert heartbeat_calls == [analysis.id, analysis.id, analysis.id]
    # ...but real_bump's own 60s throttle (proven independently above)
    # still means this never becomes three real DB writes - a single write
    # is enough to prove the call site is wired through to it at all.
    db.expire_all()
    assert analysis.processing_heartbeat_at is not None
    # The heartbeat addition changes nothing about parsing/checkpoint
    # results: three fake batches of 1 record each still sum to 3, and the
    # artifact still reaches its normal terminal checkpoint state.
    assert parsed_count == 3
    assert artifact.status == "completed"
    assert artifact.processed_bytes == artifact.size_bytes


# --- The stale threshold is a conservative, simple constant ---------------


def test_stale_threshold_is_a_single_conservative_constant():
    # 300 seconds (5 minutes): comfortably above any single normal
    # processing stage (parsing - refreshed every persisted batch; OCR -
    # bounded by MAX_OCR_IMAGE_PIXELS/MAX_OCR_IMAGE_BYTES; source clone/
    # prep - bounded by GITHUB_CLONE_TIMEOUT_SECONDS plus bounded
    # extraction/indexing; Gemini retries - refreshed once the call
    # resolves) while still reclaiming a genuinely orphaned analysis in
    # bounded time. See the comment above _STALE_ANALYSIS_THRESHOLD_SECONDS
    # for the full rationale - this just pins the value against silent
    # drift.
    assert analysis_task._STALE_ANALYSIS_THRESHOLD_SECONDS == 300


# --- recover_stale_analyses scan + atomic claim ----------------------------


def _stale_analysis(db, user, *, status="processing", age_seconds=None, **kwargs):
    heartbeat = (
        None if age_seconds is None
        else datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    )
    return _analysis(db, user, status=status, processing_heartbeat_at=heartbeat, **kwargs)


def test_recover_stale_analyses_redispatches_when_heartbeat_is_null(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(db, alice, age_seconds=None)
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [analysis_id]
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.processing_heartbeat_at is not None  # claimed: heartbeat refreshed


def test_recover_stale_analyses_redispatches_when_heartbeat_is_older_than_threshold(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(
        db, alice, age_seconds=analysis_task._STALE_ANALYSIS_THRESHOLD_SECONDS + 30
    )
    # An actively-processing artifact is what makes the fast 300s window
    # (rather than the conservative 30-minute queue-wait window) apply.
    _artifact(db, analysis, status="processing")
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [analysis_id]


def test_recover_stale_analyses_does_not_claim_activity_299_seconds_old(monkeypatch):
    """Precise boundary check for the current 300-second threshold (the
    developer's own intentional change from 600s) - 299 seconds old must
    still read as healthy, not stale."""
    db = _session()
    alice = _user(db)
    _stale_analysis(db, alice, age_seconds=299)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


def test_recover_stale_analyses_claims_activity_301_seconds_old(monkeypatch):
    """The other side of the same boundary: just past 300 seconds old is
    recoverable."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(db, alice, age_seconds=301)
    _artifact(db, analysis, status="processing")
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [analysis_id]


# --- IMAGE (OCR) artifacts: restart-only recovery, never a byte/line resume --


def test_stale_recovery_resets_a_stuck_image_artifact_for_a_clean_restart(monkeypatch, tmp_path):
    """OCR's own logical text-line offsets are not byte offsets into the
    encoded image, so a partially-processed IMAGE artifact can never safely
    resume from its checkpoint the way a text/JSON artifact can - the only
    safe recovery is a full restart. Reclaiming a stuck IMAGE artifact must
    therefore also erase its partial Evidence and OCR fallback excerpt
    (which are only meaningful together with the checkpoint they were
    produced alongside), while leaving the raw image file itself alone so
    the next attempt can OCR it again from scratch."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(db, alice, age_seconds=301)
    image_path = tmp_path / "shot.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    artifact = _artifact(
        db, analysis, status="processing",
        detected_format="image",
        saved_file_path=str(image_path),
        processed_bytes=2,  # a bogus "line count", never a real byte offset
        last_processed_line=2,
        fallback_context="partial OCR excerpt from before the crash",
    )
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
        severity="ERROR", event_type="exception",
    ))
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k2",
        fingerprint="fp2", first_line_number=2, last_line_number=2,
        severity="ERROR", event_type="exception",
    ))
    db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: None)

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    reloaded = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    assert reloaded.status == "pending"
    assert reloaded.processed_bytes == 0
    assert reloaded.last_processed_line == 0
    assert reloaded.fallback_context is None
    assert db.query(Evidence).filter(Evidence.artifact_id == artifact_id).count() == 0
    # The raw image itself must survive - OCR needs to run on it again.
    assert image_path.exists()
    reloaded_analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded_analysis.status == "pending"


def test_stale_recovery_does_not_touch_a_sibling_text_artifacts_checkpoint(monkeypatch):
    """The IMAGE-specific wipe in stale recovery must be scoped to IMAGE
    artifacts only - a text/JSON artifact stuck alongside a stuck IMAGE
    artifact in the same analysis keeps its normal, safely-resumable
    checkpoint and its already-committed Evidence untouched."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(db, alice, age_seconds=301)
    image_artifact = _artifact(
        db, analysis, position=0, status="processing", detected_format="image",
        processed_bytes=2, last_processed_line=2, fallback_context="partial ocr",
    )
    text_artifact = _artifact(
        db, analysis, position=1, status="processing", detected_format="generic",
        processed_bytes=500, last_processed_line=10, fallback_context="small text excerpt",
    )
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=text_artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
        severity="ERROR", event_type="exception",
    ))
    db.commit()
    text_artifact_id = text_artifact.id
    image_artifact_id = image_artifact.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: None)

    analysis_task.recover_stale_analyses.run()

    reloaded_text = (
        db.query(AnalysisArtifact).filter(AnalysisArtifact.id == text_artifact_id).first()
    )
    assert reloaded_text.status == "pending"
    assert reloaded_text.processed_bytes == 500  # untouched checkpoint
    assert reloaded_text.last_processed_line == 10
    assert reloaded_text.fallback_context == "small text excerpt"
    assert db.query(Evidence).filter(Evidence.artifact_id == text_artifact_id).count() == 1
    reloaded_image = (
        db.query(AnalysisArtifact).filter(AnalysisArtifact.id == image_artifact_id).first()
    )
    assert reloaded_image.processed_bytes == 0
    assert reloaded_image.fallback_context is None


def test_image_artifact_processed_bytes_stays_zero_until_full_completion(monkeypatch, tmp_path):
    """OCR text-reconstruction offsets are logical line positions within the
    extracted text, never byte offsets into the encoded image - persisting
    them into processed_bytes mid-run would masquerade as a real byte
    checkpoint (and, combined with old partial Evidence, invite a resume
    that double-counts). processed_bytes must stay 0 through every
    intermediate batch and only become size_bytes on the single terminal
    completion write."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=3)
    image_path = tmp_path / "shot.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    artifact = _artifact(
        db, analysis, status="processing", detected_format=None,
        saved_file_path=str(image_path), size_bytes=image_path.stat().st_size,
    )
    monkeypatch.setattr(
        analysis_task, "extract_text_from_image_with_confidence",
        lambda path: ("ERROR: first failure\nERROR: second failure\n", 0.9),
    )
    monkeypatch.setattr(analysis_task, "publish_artifact_outcome", lambda *a, **k: None)

    seen_offsets = []

    def fake_persist(*, db, analysis_id, events, artifact_id=None):
        seen_offsets.append(artifact.processed_bytes)

    monkeypatch.setattr(analysis_task, "persist_evidence_batch", fake_persist)

    analysis_task._process_artifact(db=db, analysis=analysis, artifact=artifact, generation=3)

    assert seen_offsets, "expected at least one persisted batch"
    assert all(offset == 0 for offset in seen_offsets)
    assert artifact.status == "completed"
    assert artifact.processed_bytes == artifact.size_bytes  # only set at the very end


def test_recover_stale_analyses_never_touches_a_fresh_heartbeat(monkeypatch):
    """Conservative by design: a normal in-flight analysis with a recent
    heartbeat (well within the threshold - e.g. mid-parse, mid-OCR,
    mid-Gemini-retry) must never be redispatched."""
    db = _session()
    alice = _user(db)
    _stale_analysis(db, alice, age_seconds=5)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


@pytest.mark.parametrize("status", ["cancelled", "completed", "failed"])
def test_recover_stale_analyses_never_claims_a_terminal_analysis(monkeypatch, status):
    db = _session()
    alice = _user(db)
    _stale_analysis(db, alice, status=status, age_seconds=None)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


def test_recover_stale_analyses_claim_is_atomic_against_a_concurrent_second_scan(monkeypatch):
    """Two scans (or a scan racing this same task's next tick)
    must never produce two logical redispatches for the same orphaned
    analysis - the second scan's conditional UPDATE affects zero rows
    once the first scan already refreshed the heartbeat, so it claims
    nothing."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(db, alice, age_seconds=None)
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    first_scan_claims = analysis_task.recover_stale_analyses.run()
    second_scan_claims = analysis_task.recover_stale_analyses.run()

    assert first_scan_claims == 1
    assert second_scan_claims == 0
    assert delayed == [analysis_id]  # exactly one redispatch, not two


def test_recover_stale_analyses_scan_is_bounded_per_tick(monkeypatch):
    db = _session()
    alice = _user(db)
    limit = analysis_task._RECOVERY_SCAN_BATCH_LIMIT
    for _ in range(limit + 5):
        _stale_analysis(db, alice, age_seconds=None)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == limit  # bounded, not all (limit + 5)


# --- pending recovery uses its own, much more conservative window --------
#
# Production incident: worker_concurrency=2 meant two large, genuinely
# healthy analyses (~1GB and ~500MB) occupied both worker slots while a
# third, smaller one sat normally queued. The old code treated "pending +
# NULL heartbeat + 300s" as orphaned, so recovery redispatched a SECOND
# process_analysis for the still-healthy queued one - once a worker slot
# freed up, both the original and the falsely "recovered" copy ran,
# causing duplicate artifact processing, duplicate finalization, and a
# duplicate Gemini call. The tests below pin the fix: pending status uses
# COALESCE(processing_heartbeat_at, created_at) against a much longer
# (30-minute) grace window, completely independent of the 300-second
# processing window.


def _pending_analysis(db, user, *, created_age_seconds=0, heartbeat_age_seconds=None, **kwargs):
    now = datetime.now(timezone.utc)
    heartbeat = (
        None if heartbeat_age_seconds is None
        else now - timedelta(seconds=heartbeat_age_seconds)
    )
    return _analysis(
        db, user, status="pending",
        created_at=now - timedelta(seconds=created_age_seconds),
        processing_heartbeat_at=heartbeat,
        **kwargs,
    )


def test_pending_recovery_threshold_is_a_conservative_30_minute_constant():
    assert analysis_task._PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS == 30 * 60


def test_recover_stale_analyses_does_not_falsely_reclaim_a_healthy_pending_backlog(monkeypatch):
    """Reproduces the exact production bug (Analyses 28/29/30): Analysis A
    is a genuinely healthy large analysis occupying the (intentionally
    small) worker pool; B and C are healthy pending analyses still queued
    behind it - older than the old, wrong 300-second window but
    comfortably inside the new 30-minute pending grace window. Recovery
    must claim none of them, even across repeated Beat ticks, so it can
    never create the duplicate top-level process_analysis dispatch that
    caused duplicate work/finalization/Gemini calls in production."""
    db = _session()
    alice = _user(db)
    analysis_a = _analysis(
        db, alice, status="processing",
        processing_heartbeat_at=datetime.now(timezone.utc) - timedelta(seconds=5),
    )
    analysis_b = _pending_analysis(db, alice, created_age_seconds=400)
    analysis_c = _pending_analysis(db, alice, created_age_seconds=600)
    ids = [analysis_a.id, analysis_b.id, analysis_c.id]
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    first_claimed = analysis_task.recover_stale_analyses.run()
    second_claimed = analysis_task.recover_stale_analyses.run()

    assert first_claimed == 0
    assert second_claimed == 0
    assert delayed == []
    for analysis_id in ids:
        assert analysis_id not in delayed


def test_pending_with_null_heartbeat_301_seconds_old_is_not_recovered(monkeypatch):
    """This was specifically WRONG under the old shared 300-second rule."""
    db = _session()
    alice = _user(db)
    _pending_analysis(db, alice, created_age_seconds=301)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


def test_pending_with_null_heartbeat_several_minutes_old_is_not_recovered(monkeypatch):
    db = _session()
    alice = _user(db)
    _pending_analysis(db, alice, created_age_seconds=600)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


def test_pending_just_below_the_pending_threshold_is_not_recovered(monkeypatch):
    db = _session()
    alice = _user(db)
    _pending_analysis(
        db, alice,
        created_age_seconds=analysis_task._PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS - 1,
    )
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


def test_pending_just_beyond_the_pending_threshold_is_recovered_exactly_once(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _pending_analysis(
        db, alice,
        created_age_seconds=analysis_task._PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS + 1,
    )
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [analysis_id]
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.processing_heartbeat_at is not None  # claim fence written


def test_pending_previously_claimed_with_a_fresh_heartbeat_is_not_reclaimed_again(monkeypatch):
    """Proves the COALESCE/fresh-claim-time behavior: an old created_at
    must not matter once a previous recovery claim wrote a fresh
    processing_heartbeat_at - that heartbeat becomes the new age
    reference, exactly as it already does for "processing" rows."""
    db = _session()
    alice = _user(db)
    _pending_analysis(db, alice, created_age_seconds=100_000, heartbeat_age_seconds=5)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert delayed == []


def test_pending_becomes_recoverable_again_once_its_claim_heartbeat_goes_stale(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _pending_analysis(
        db, alice, created_age_seconds=100_000,
        heartbeat_age_seconds=analysis_task._PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS + 1,
    )
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [analysis_id]


def test_recover_stale_pending_claim_is_atomic_against_a_concurrent_second_scan(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _pending_analysis(
        db, alice,
        created_age_seconds=analysis_task._PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS + 1,
    )
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    first_scan_claims = analysis_task.recover_stale_analyses.run()
    second_scan_claims = analysis_task.recover_stale_analyses.run()

    assert first_scan_claims == 1
    assert second_scan_claims == 0
    assert delayed == [analysis_id]  # exactly one redispatch, not two


def test_recover_stale_pending_scan_is_bounded_per_tick(monkeypatch):
    db = _session()
    alice = _user(db)
    limit = analysis_task._RECOVERY_SCAN_BATCH_LIMIT
    threshold = analysis_task._PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS
    for _ in range(limit + 5):
        _pending_analysis(db, alice, created_age_seconds=threshold + 1)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == limit  # bounded, not all (limit + 5)


def test_recover_stale_analyses_processing_and_pending_windows_are_independent(monkeypatch):
    """A single scan correctly separates the two populations: a stale
    processing analysis is claimed under the 300s rule while a healthy
    pending backlog under the 30-minute grace window is left alone, in
    the same tick."""
    db = _session()
    alice = _user(db)
    stale_processing = _stale_analysis(
        db, alice, status="processing",
        age_seconds=analysis_task._STALE_ANALYSIS_THRESHOLD_SECONDS + 30,
    )
    _artifact(db, stale_processing, status="processing")
    healthy_pending = _pending_analysis(db, alice, created_age_seconds=400)
    stale_processing_id = stale_processing.id
    healthy_pending_id = healthy_pending.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [stale_processing_id]
    assert healthy_pending_id not in delayed


# --- process_analysis terminal guards allow legitimate redispatch ---------


def test_process_analysis_does_not_redispatch_an_already_processing_analysis(monkeypatch):
    """The core fix for the production duplicate-dispatch incident:
    process_analysis's ONLY way to start a workflow is winning an atomic
    pending->processing claim. A duplicate invocation that finds the
    analysis already "processing" (Celery broker redelivery, or any other
    trigger) must return WITHOUT dispatching anything - it is no longer
    treated as "fine to proceed from," unlike the old behavior. Only
    recovery's own demote-to-pending step (see
    _claim_and_demote_stale_processing) can make a genuinely-stale
    "processing" analysis dispatchable again."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind=None)
    _artifact(db, analysis, status="pending")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(
            AssertionError("must not dispatch for an already-processing analysis")
        ),
    )

    analysis_task.process_analysis.run(analysis.id)  # must not raise, must not dispatch

    db.expire_all()
    assert analysis.status == "processing"
    assert analysis.processing_generation == 0  # unclaimed - no new generation


def test_process_analysis_claims_a_pending_analysis_and_establishes_a_fresh_generation(monkeypatch):
    """The companion case: a genuinely "pending" analysis (a fresh upload,
    or one recovery has just demoted back to pending) IS claimed and
    dispatched, with processing_generation incremented exactly once."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    captured = {}
    monkeypatch.setattr(analysis_task, "group", lambda sigs: captured.setdefault("group_sigs", list(sigs)))
    monkeypatch.setattr(
        analysis_task, "chord",
        lambda group_obj, callback: SimpleNamespace(apply_async=lambda: captured.setdefault("dispatched", True)),
    )
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", lambda *a: Mock())
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", lambda *a: Mock())

    analysis_task.process_analysis.run(analysis.id)

    assert captured.get("dispatched") is True
    db.expire_all()
    assert analysis.status == "processing"
    assert analysis.processing_generation == 1


@pytest.mark.parametrize("status", ["cancelled", "completed", "failed"])
def test_process_analysis_never_redispatches_a_terminal_analysis(monkeypatch, status):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status=status)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(AssertionError(f"must not dispatch for {status}")),
    )

    analysis_task.process_analysis.run(analysis.id)  # must not raise


# --- Source recovery does not reclone or retry unnecessarily --------------


def test_prepare_source_task_reuses_the_ready_marker_instead_of_recloning(monkeypatch, tmp_path):
    """Not recloning when already ready is satisfied one layer
    down from _prepare_source_task: source_archive.prepare_source() itself
    is idempotent via an on-disk ready marker (see its own docstring -
    "resuming a GitHub-sourced analysis always fails outright (git clone
    refuses a non-empty destination)" is exactly the failure this avoids).
    _prepare_source_task calling _prepare_source_index again on a
    recovery redispatch is therefore safe and cheap: it hits the marker
    and returns the cached index without touching the network/filesystem
    acquisition path again."""
    from app.services import source_archive

    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path))
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", source_kind="github",
        source_reference="https://github.com/example/repo", source_status="ready",
    )
    # _bump_processing_heartbeat now owns its own isolated session (a
    # separate sessionLocal() call it closes itself) - give it its own
    # throwaway Mock so it doesn't close this test's real `db` out from
    # under the rest of _prepare_source_task.
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(side_effect=[db, Mock()]))
    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})

    clone_calls = []
    monkeypatch.setattr(source_archive, "_clone_github", lambda *a, **k: clone_calls.append(a))
    dest = tmp_path / str(analysis.id)
    dest.mkdir(parents=True)
    source_archive._ready_marker(dest).touch()
    monkeypatch.setattr(
        source_archive, "load_index_manifest", lambda manifest_path, d: {"cached": True}
    )

    analysis_task._prepare_source_task.run(analysis.id, 0)

    assert clone_calls == []  # the ready marker short-circuited real acquisition
    db.expire_all()
    assert analysis.source_status == "ready"

def test_duplicate_source_preparing_waiter_retries_without_heartbeat_or_failure(
    monkeypatch,
):
    db = _session()

    alice = _user(db)

    analysis = _analysis(
        db,
        alice,
        status="processing",
        source_kind="github",
        source_reference="https://github.com/example/repo",
        source_status="preparing",
    )

    analysis_id = analysis.id

    heartbeat = Mock()
    mark_failed = Mock()

    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        lambda **_k: db,
    )

    monkeypatch.setattr(
        analysis_task,
        "_bump_processing_heartbeat",
        heartbeat,
    )

    monkeypatch.setattr(
        analysis_task,
        "_mark_analysis_failed",
        mark_failed,
    )

    monkeypatch.setattr(
        analysis_task,
        "load_ready_source_index",
        lambda _aid: None,
    )

    assert (
        analysis_task._prepare_source_task.max_retries
        is None
    )

    with pytest.raises(Retry):
        analysis_task._prepare_source_task.run(
            analysis_id,
            0,
        )

    heartbeat.assert_not_called()
    mark_failed.assert_not_called()


def test_prepare_source_task_adopts_complete_ready_source_after_owner_crash(
    monkeypatch,
    tmp_path,
):
    from app.services import source_archive

    monkeypatch.setattr(
        source_archive,
        "SOURCE_STORAGE_ROOT",
        str(tmp_path / "sources"),
    )

    monkeypatch.setattr(
        analysis_task,
        "_source_index_process_cache",
        {},
    )

    def fake_clone(_url, dest):
        dest.mkdir(
            parents=True,
            exist_ok=True,
        )

        (dest / "app.py").write_text(
            "print(1)\n"
        )

    monkeypatch.setattr(
        source_archive,
        "_clone_github",
        fake_clone,
    )

    db = _session()

    alice = _user(db)

    analysis = _analysis(
        db,
        alice,
        status="processing",
        source_kind="github",
        source_reference="https://github.com/example/repo",
        source_status="preparing",
    )

    analysis_id = analysis.id

    # Exact crash window:
    #
    # tree + index + manifest + .ready exist,
    # but DB still says preparing.
    source_archive.prepare_source(
        "github",
        "https://github.com/example/repo",
        analysis_id,
        0,
    )

    assert source_archive._ready_marker(
        tmp_path / "sources" / str(analysis_id)
    ).exists()

    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        lambda **_k: db,
    )

    monkeypatch.setattr(
        analysis_task,
        "_bump_processing_heartbeat",
        lambda *_a, **_k: (
            _ for _ in ()
        ).throw(
            AssertionError(
                "duplicate ready-source adopter must not bump heartbeat"
            )
        ),
    )

    analysis_task._prepare_source_task.run(
        analysis_id,
        0,
    )

    db.expire_all()

    reloaded = (
        db.query(Analysis)
        .filter(
            Analysis.id == analysis_id
        )
        .one()
    )

    assert reloaded.source_status == "ready"

    assert (
        analysis_id,
        0,
    ) in analysis_task._source_index_process_cache


def test_source_publication_guard_rejects_superseded_generation(
    monkeypatch,
):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:"
    )

    Base.metadata.create_all(engine)

    session_factory = sessionmaker(
        bind=engine
    )

    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        session_factory,
    )

    db = session_factory()

    alice = _user(db)

    analysis = _analysis(
        db,
        alice,
        status="processing",
        processing_generation=2,
        source_kind="github",
        source_reference="https://github.com/example/repo",
        source_status="preparing",
    )

    analysis_id = analysis.id

    db.close()

    publisher = Mock(
        return_value=object()
    )

    result = (
        analysis_task
        ._publish_source_for_current_generation(
            analysis_id,
            1,
            publisher,
        )
    )

    assert result is None

    publisher.assert_not_called()


def test_recovery_source_temp_cleanup_runs_only_after_demote_commit(
    monkeypatch,
):
    db = _session()

    alice = _user(db)

    analysis = _analysis(
        db,
        alice,
        status="processing",
        processing_generation=3,
        source_kind="github",
        source_reference="https://github.com/example/repo",
        source_status="preparing",
    )

    analysis_id = analysis.id

    committed = {
        "value": False
    }

    real_commit = db.commit

    def tracked_commit():
        real_commit()
        committed["value"] = True

    monkeypatch.setattr(
        db,
        "commit",
        tracked_commit,
    )

    cleanup_calls = []

    def tracked_cleanup(
        aid,
        generation,
    ):
        assert committed["value"] is True

        cleanup_calls.append(
            (
                aid,
                generation,
            )
        )

    monkeypatch.setattr(
        analysis_task,
        "cleanup_generation_source_temp",
        tracked_cleanup,
    )

    claimed = (
        analysis_task
        ._claim_and_demote_stale_processing(
            db,
            Analysis.id == analysis_id,
            datetime.now(timezone.utc),
        )
    )

    assert claimed == [analysis_id]

    assert cleanup_calls == [
        (
            analysis_id,
            3,
        )
    ]

    db.expire_all()

    reloaded = (
        db.query(Analysis)
        .filter(
            Analysis.id == analysis_id
        )
        .one()
    )

    assert reloaded.status == "pending"
    assert reloaded.source_status is None

def test_prepare_source_task_does_not_retry_when_source_is_already_unavailable(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", source_kind="zip", source_status="unavailable",
        source_failure_reason="Uploaded source ZIP could not be prepared: bad zip",
    )
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(
        analysis_task, "_acquire_source_index",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not retry an already-unavailable source")),
    )

    analysis_task._prepare_source_task.run(analysis.id, 0)

    db.expire_all()
    assert analysis.source_status == "unavailable"


# --- Zombie recovery (all artifacts terminal, finalize missing) -----------


def test_process_analysis_finalizes_directly_when_every_artifact_is_already_terminal(monkeypatch):
    """The old-process-died-before-finalization zombie case: recovery
    redispatches process_analysis, which must invoke the finalizer
    directly - never rebuild a chord/group over already-terminal
    artifacts, never reparse them."""
    db = _session()
    alice = _user(db)
    # "pending": recovery's own demotion step (see
    # _claim_and_demote_stale_processing) is what returns a genuinely-stale
    # zombie analysis to "pending" before redispatching process_analysis -
    # process_analysis itself only ever claims from "pending".
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, position=0, status="completed")
    _artifact(db, analysis, position=1, status="resource_limited")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(AssertionError("must not build a chord for the zombie case")),
    )
    finalize_calls = []
    monkeypatch.setattr(
        analysis_task._finalize_analysis_task, "delay",
        lambda results, aid, generation, dispatch_start: finalize_calls.append((results, aid, generation)),
    )

    analysis_task.process_analysis.run(analysis.id)

    assert finalize_calls == [([], analysis.id, 1)]


def test_process_analysis_raises_when_the_analysis_has_no_artifacts_at_all(monkeypatch):
    """Distinguishes the genuine "no persisted artifacts" bug case from
    the zombie-recovery case above - upload_file() already guarantees
    this can't happen on a fresh upload, so seeing it at all is a real
    error, not a recovery scenario."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        analysis_task, "_mark_analysis_failed", lambda db, aid, generation=None: False
    )

    with pytest.raises(RuntimeError, match="no persisted diagnostic artifacts"):
        analysis_task.process_analysis.run(analysis.id)


def test_recovered_zombie_finalizes_end_to_end_without_reparsing_completed_artifacts(monkeypatch):
    """Full-depth version of the zombie case: the finalizer this recovery
    path invokes must actually complete the investigation using only
    already-persisted Evidence, with no re-run of parsing/correlation
    for the completed artifact."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    artifact = _artifact(
        db, analysis, status="completed", processed_bytes=100, last_processed_line=5,
    )
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
            fingerprint="fp1", first_line_number=1, last_line_number=1,
            severity="ERROR", event_type="exception",
        )
    )
    db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id
    db.close()

    gemini_calls = []

    def fake_gemini(context):
        gemini_calls.append(context)
        return GeminiInvestigationResponse(
            title="t", summary="s", probable_root_causes=[], what_happened=[],
            source_code_findings=[], recommended_actions=[], uncertainties=[],
        )

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", fake_gemini)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    published = []
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )

    # Exactly what process_analysis's finalize_only branch does: invoke
    # the finalizer directly with an empty results list (no artifact
    # tasks actually ran this time).
    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(published) == 1
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot is not None
    # The artifact's own persisted checkpoint was never disturbed/reset.
    reloaded_artifact = db2.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    assert reloaded_artifact.processed_bytes == 100
    assert reloaded_artifact.last_processed_line == 5
    db2.close()


# --- DB-outage-left processing state remains recoverable ------------------


def test_artifact_left_processing_after_a_db_outage_is_recoverable_with_checkpoint_intact(monkeypatch):
    """The exact observed-in-production shape: MySQL disappears mid-
    artifact, the artifact task's own generic except-block re-raises a
    real OperationalError, and _mark_analysis_failed cannot write "failed"
    either (its own independent swallow-on-failure guarantee is proven in
    isolation by test_mark_analysis_failed_never_overwrites_cancelled's
    sibling tests in test_analysis_cancellation.py - stubbed here rather
    than re-derived). The Analysis row must be left exactly as it was
    ("processing", never resurrected to "failed"), the last successfully
    committed artifact checkpoint and its Evidence must be untouched, and
    once heartbeat goes stale the existing recovery scan must still find
    and reclaim it - no special-casing for how the exception originated."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    # "pending" with a nonzero checkpoint: a prior crash already left this
    # exact resumable state (recovery resets a stuck-"processing" artifact
    # back to "pending" while preserving its last committed checkpoint -
    # see _claim_and_demote_stale_processing). This redispatch's own atomic
    # artifact claim is what moves it back to "processing" below.
    artifact = _artifact(
        db, analysis, status="pending", processed_bytes=500, last_processed_line=10,
    )
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
        severity="ERROR", event_type="exception",
    ))
    db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)

    def raise_operational_error(**kwargs):
        raise OperationalError(
            "UPDATE analysis_artifacts ...", {},
            Exception("(2003, \"Can't connect to MySQL server on 'db:3306'\")"),
        )

    monkeypatch.setattr(analysis_task, "_process_artifact", raise_operational_error)
    # DB is down for the whole scenario - _mark_analysis_failed's own real
    # write would fail and swallow too (proven elsewhere); stubbed here so
    # this test's assertions stay focused on the surrounding state.
    monkeypatch.setattr(
        analysis_task, "_mark_analysis_failed", lambda db_arg, aid, generation=None: False
    )

    with pytest.raises(OperationalError):
        analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)

    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "processing"  # never falsely "failed"
    reloaded_artifact = (
        db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    )
    assert reloaded_artifact.processed_bytes == 500  # pre-outage checkpoint intact
    assert reloaded_artifact.last_processed_line == 10
    assert db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 1

    # "MySQL later returns" - simulated here as time passing far enough
    # that the heartbeat set before the outage is now stale.
    db.query(Analysis).filter(Analysis.id == analysis_id).update(
        {
            "processing_heartbeat_at": datetime.now(timezone.utc)
            - timedelta(seconds=analysis_task._STALE_ANALYSIS_THRESHOLD_SECONDS + 30)
        }
    )
    db.commit()
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed = analysis_task.recover_stale_analyses.run()

    assert claimed == 1
    assert delayed == [analysis_id]


def test_safe_rollback_swallows_a_failing_rollback():
    db = Mock()
    db.rollback.side_effect = OperationalError(
        "ROLLBACK", {}, Exception("(2003, \"Can't connect to MySQL server\")")
    )

    analysis_task._safe_rollback(db)  # must not raise

    db.rollback.assert_called_once()


def test_a_failing_rollback_does_not_mask_the_original_db_failure(monkeypatch):
    """Same shape as test_artifact_left_processing_after_a_db_outage_is_
    recoverable_with_checkpoint_intact above, but the connection is dead
    enough that db.rollback() itself also raises - the ORIGINAL
    OperationalError (from _process_artifact) must still be what
    propagates out of _process_artifact_task, not a rollback-originated
    exception, and durable state must be left exactly as untouched."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    artifact = _artifact(
        db, analysis, status="pending", processed_bytes=500, last_processed_line=10,
    )
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
        severity="ERROR", event_type="exception",
    ))
    db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)

    original_error = OperationalError(
        "SELECT ...", {}, Exception("(2003, \"Can't connect to MySQL server on 'db:3306'\")"),
    )

    def raise_original_error(**kwargs):
        raise original_error

    monkeypatch.setattr(analysis_task, "_process_artifact", raise_original_error)
    monkeypatch.setattr(
        db, "rollback",
        Mock(side_effect=OperationalError("ROLLBACK", {}, Exception("connection gone"))),
    )
    monkeypatch.setattr(
        analysis_task, "_mark_analysis_failed", lambda db_arg, aid, generation=None: None
    )

    with pytest.raises(OperationalError) as exc_info:
        analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)

    assert exc_info.value is original_error  # not the rollback's own exception
    db.rollback.assert_called_once()

    # Durable state is untouched - no fake completed/failed/resource_limited
    # outcome, no destroyed checkpoint/Evidence.
    engine = db.get_bind()
    verify_db = sessionmaker(bind=engine)()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "processing"
    reloaded_artifact = (
        verify_db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    )
    assert reloaded_artifact.status == "processing"
    assert reloaded_artifact.processed_bytes == 500
    assert reloaded_artifact.last_processed_line == 10
    assert verify_db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 1
    verify_db.close()


# --- Gemini/finalize heartbeat coverage (no client-side timeout on --------
# generate_investigation_explanation) ---------------------------------------


def test_finalize_bumps_heartbeat_after_gemini_resolves(monkeypatch):
    """generate_investigation_explanation() has no client-side timeout
    (google-genai's default is unbounded) and can retry up to
    _MAX_ATTEMPTS times, so real wall-clock time may pass between
    _finalize_analysis_task's single entry-heartbeat and its final commit.
    Proven here on the zero-evidence/fallback branch (the same branch
    test_finalize_discards_a_gemini_result_when_cancelled_arrives_while_
    gemini_was_running in test_analysis_cancellation.py already exercises
    for the cancellation race) by spying on the real heartbeat helper."""
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    db = session_factory()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", processing_heartbeat_at=None,
    )
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
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: None)

    heartbeat_calls = []
    real_bump = analysis_task._bump_processing_heartbeat

    def spy_bump(aid, generation):
        heartbeat_calls.append(aid)
        return real_bump(aid, generation)

    monkeypatch.setattr(analysis_task, "_bump_processing_heartbeat", spy_bump)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    # Once at finalize entry, once more after Gemini resolves.
    assert heartbeat_calls == [analysis_id, analysis_id]
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.processing_heartbeat_at is not None
    db2.close()


# --- Final proof pass: explicit artifact-format recovery matrix ------------


def test_recovery_matrix_explicitly_accounts_for_every_artifact_format():
    from app.services.artifact_detector import ArtifactFormat

    resumable = {
        "generic",
        "json",
        "stack_trace",
        "web_server",
        "container",
        "database",
        "cloud_gateway",
        "ci_cd",
        "browser",
        "message_broker",
        "serverless",
        "syslog",
        "opentelemetry",
    }
    assert {item.value for item in ArtifactFormat} == resumable | {
        "image",
        "unsupported",
    }


@pytest.mark.parametrize(
    "artifact_format",
    [
        "generic",
        "json",
        "stack_trace",
        "web_server",
        "container",
        "database",
        "cloud_gateway",
        "ci_cd",
        "browser",
        "message_broker",
        "serverless",
        "syslog",
        "opentelemetry",
    ],
)
def test_recovery_matrix_preserves_committed_checkpoint_and_evidence_for_every_resumable_format(
    monkeypatch,
    artifact_format,
):
    """Every supported non-image diagnostic format resumes from durable
    state. Recovery releases only the in-flight artifact claim; it must not
    erase already-committed Evidence, line progress, byte/record checkpoint,
    or bounded fallback context. Structured document formats legitimately use
    a record checkpoint with processed_bytes == 0, so the fixture mirrors that
    rather than inventing a byte offset they do not persist."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(
        db,
        alice,
        age_seconds=301,
    )
    structured_record_formats = {
        "json",
        "browser",
        "opentelemetry",
    }
    checkpoint_bytes = (
        0
        if artifact_format in structured_record_formats
        else 321
    )
    artifact = _artifact(
        db,
        analysis,
        status="processing",
        detected_format=artifact_format,
        processed_bytes=checkpoint_bytes,
        last_processed_line=7,
        fallback_context="committed fallback context",
    )
    db.add(
        Evidence(
            analysis_id=analysis.id,
            artifact_id=artifact.id,
            correlation_key=f"recovery-{artifact_format}",
            fingerprint=f"fp-{artifact_format}",
            source_format=artifact_format,
            first_line_number=7,
            last_line_number=7,
            severity="ERROR",
            representative_line="committed evidence",
        )
    )
    db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id
    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        lambda **_kwargs: db,
    )
    monkeypatch.setattr(
        analysis_task.process_analysis,
        "delay",
        lambda _aid: None,
    )

    claimed_count = (
        analysis_task.recover_stale_analyses.run()
    )

    assert claimed_count == 1
    db.expire_all()
    recovered_analysis = (
        db.query(Analysis)
        .filter(Analysis.id == analysis_id)
        .one()
    )
    recovered_artifact = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.id == artifact_id)
        .one()
    )
    assert recovered_analysis.status == "pending"
    assert recovered_artifact.status == "pending"
    assert (
        recovered_artifact.detected_format
        == artifact_format
    )
    assert (
        recovered_artifact.processed_bytes
        == checkpoint_bytes
    )
    assert (
        recovered_artifact.last_processed_line
        == 7
    )
    assert (
        recovered_artifact.fallback_context
        == "committed fallback context"
    )
    assert (
        db.query(Evidence)
        .filter(Evidence.artifact_id == artifact_id)
        .count()
        == 1
    )


def test_recovery_matrix_image_is_restart_only_and_clears_partial_state(
    monkeypatch,
):
    """IMAGE is the one supported format that is intentionally not
    resumable: OCR checkpoints are logical extracted-text positions, not byte
    offsets into the encoded image. Recovery must therefore cleanly restart
    it and remove partial Evidence/fallback state."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(
        db,
        alice,
        age_seconds=301,
    )
    artifact = _artifact(
        db,
        analysis,
        status="processing",
        detected_format="image",
        processed_bytes=9,
        last_processed_line=9,
        fallback_context="partial OCR fallback",
    )
    db.add(
        Evidence(
            analysis_id=analysis.id,
            artifact_id=artifact.id,
            correlation_key="image-partial",
            fingerprint="image-partial",
            source_format="image",
            first_line_number=1,
            last_line_number=1,
            severity="ERROR",
        )
    )
    db.commit()
    artifact_id = artifact.id
    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        lambda **_kwargs: db,
    )
    monkeypatch.setattr(
        analysis_task.process_analysis,
        "delay",
        lambda _aid: None,
    )

    assert (
        analysis_task.recover_stale_analyses.run()
        == 1
    )
    db.expire_all()
    recovered = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.id == artifact_id)
        .one()
    )
    assert recovered.status == "pending"
    assert recovered.detected_format == "image"
    assert recovered.processed_bytes == 0
    assert recovered.last_processed_line == 0
    assert recovered.fallback_context is None
    assert (
        db.query(Evidence)
        .filter(Evidence.artifact_id == artifact_id)
        .count()
        == 0
    )


def test_recovery_matrix_unsupported_artifact_remains_terminal(
    monkeypatch,
):
    """UNSUPPORTED is a detected format but never resumable work. Recovery
    must leave its terminal artifact outcome untouched while recovering a
    genuinely stuck sibling that makes the parent stale/recoverable."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(
        db,
        alice,
        age_seconds=301,
    )
    unsupported = _artifact(
        db,
        analysis,
        position=0,
        status="unsupported",
        detected_format="unsupported",
        processed_bytes=0,
        last_processed_line=0,
        fallback_context=None,
    )
    stuck = _artifact(
        db,
        analysis,
        position=1,
        status="processing",
        detected_format="generic",
        processed_bytes=100,
        last_processed_line=5,
    )
    db.commit()
    unsupported_id = unsupported.id
    stuck_id = stuck.id
    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        lambda **_kwargs: db,
    )
    monkeypatch.setattr(
        analysis_task.process_analysis,
        "delay",
        lambda _aid: None,
    )

    assert (
        analysis_task.recover_stale_analyses.run()
        == 1
    )
    db.expire_all()
    unsupported_after = (
        db.query(AnalysisArtifact)
        .filter(
            AnalysisArtifact.id
            == unsupported_id
        )
        .one()
    )
    stuck_after = (
        db.query(AnalysisArtifact)
        .filter(
            AnalysisArtifact.id == stuck_id
        )
        .one()
    )
    assert unsupported_after.status == "unsupported"
    assert (
        unsupported_after.detected_format
        == "unsupported"
    )
    assert stuck_after.status == "pending"
    assert stuck_after.processed_bytes == 100
    assert stuck_after.last_processed_line == 5
