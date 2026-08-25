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


def test_bump_processing_heartbeat_writes_on_first_call(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)

    analysis_task._bump_processing_heartbeat(db, analysis.id)

    db.expire_all()
    assert analysis.processing_heartbeat_at is not None


def test_bump_processing_heartbeat_is_throttled_within_the_minimum_interval(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)

    analysis_task._bump_processing_heartbeat(db, analysis.id)
    db.expire_all()
    first_write = analysis.processing_heartbeat_at

    # A burst of further calls within the same throttle window (as a tight
    # batch loop would produce) must not write again - this is exactly
    # what keeps the heartbeat from recreating per-batch shared-row
    # contention.
    for _ in range(50):
        analysis_task._bump_processing_heartbeat(db, analysis.id)

    db.expire_all()
    assert analysis.processing_heartbeat_at == first_write


def test_bump_processing_heartbeat_writes_again_after_the_interval_elapses(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    monkeypatch.setattr(analysis_task, "_HEARTBEAT_MIN_INTERVAL_SECONDS", 0.0)
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)

    analysis_task._bump_processing_heartbeat(db, analysis.id)
    db.expire_all()
    first_write = analysis.processing_heartbeat_at

    analysis_task._bump_processing_heartbeat(db, analysis.id)

    db.expire_all()
    assert analysis.processing_heartbeat_at is not None
    assert analysis.processing_heartbeat_at >= first_write


def test_bump_processing_heartbeat_failure_is_logged_and_swallowed(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    db = Mock()
    db.query.return_value.filter.return_value.update.side_effect = RuntimeError("db gone")

    analysis_task._bump_processing_heartbeat(db, 9)  # must not raise

    db.rollback.assert_called_once()


def test_bump_processing_heartbeat_throttle_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(analysis_task, "_last_heartbeat_write", {})
    monkeypatch.setattr(analysis_task, "_HEARTBEAT_THROTTLE_CACHE_MAX_ENTRIES", 3)
    db = _session()
    alice = _user(db)
    analyses = [_analysis(db, alice, status="processing") for _ in range(5)]

    for analysis in analyses:
        analysis_task._bump_processing_heartbeat(db, analysis.id)

    assert len(analysis_task._last_heartbeat_write) == 3


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
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_heartbeat_at=None)
    artifact_path = tmp_path / "a.log"
    artifact_path.write_text("2026-01-01 10:00:00 ERROR service=a boom\n")
    artifact = _artifact(
        db, analysis, status="pending", saved_file_path=str(artifact_path),
        size_bytes=artifact_path.stat().st_size, original_filename="a.log",
    )

    monkeypatch.setattr(analysis_task, "create_batches", lambda records: [["b1"], ["b2"], ["b3"]])
    monkeypatch.setattr(analysis_task, "_persist_artifact_batch", lambda **kwargs: 1)
    monkeypatch.setattr(analysis_task, "_publish_ingestion_progress", lambda **kwargs: -1)

    heartbeat_calls = []
    real_bump = analysis_task._bump_processing_heartbeat

    def spy_bump(db_arg, analysis_id):
        heartbeat_calls.append(analysis_id)
        return real_bump(db_arg, analysis_id)

    monkeypatch.setattr(analysis_task, "_bump_processing_heartbeat", spy_bump)

    parsed_count = analysis_task._process_artifact(db=db, analysis=analysis, artifact=artifact)

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
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    delayed = []
    monkeypatch.setattr(analysis_task.process_analysis, "delay", lambda aid: delayed.append(aid))

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 1
    assert delayed == [analysis_id]


def test_recover_stale_analyses_never_touches_a_fresh_heartbeat(monkeypatch):
    """Conservative by design: a normal in-flight analysis with a recent
    heartbeat (well within the threshold - e.g. mid-parse, mid-OCR,
    mid-Gemini-retry) must never be redispatched."""
    db = _session()
    alice = _user(db)
    analysis = _stale_analysis(db, alice, age_seconds=5)
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


# --- process_analysis terminal guards allow legitimate redispatch ---------


def test_process_analysis_allows_a_recovered_processing_analysis_to_redispatch(monkeypatch):
    """A "processing" analysis reaching process_analysis again (via the
    recovery scan's redispatch, or a legitimate redelivery of the
    top-level task itself) must NOT be treated as already-terminal - only
    cancelled/completed/failed are terminal guards."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind=None)
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
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})

    clone_calls = []
    monkeypatch.setattr(source_archive, "_clone_github", lambda *a, **k: clone_calls.append(a))
    dest = tmp_path / str(analysis.id)
    dest.mkdir(parents=True)
    source_archive._ready_marker(dest).touch()
    monkeypatch.setattr(
        source_archive, "load_index_manifest", lambda manifest_path, d: {"cached": True}
    )

    analysis_task._prepare_source_task.run(analysis.id)

    assert clone_calls == []  # the ready marker short-circuited real acquisition
    db.expire_all()
    assert analysis.source_status == "ready"


def test_prepare_source_task_does_not_retry_when_source_is_already_unavailable(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", source_kind="zip", source_status="unavailable",
        source_failure_reason="Uploaded source ZIP could not be prepared: bad zip",
    )
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(
        analysis_task, "_prepare_source_index",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not retry an already-unavailable source")),
    )

    analysis_task._prepare_source_task.run(analysis.id)

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
    analysis = _analysis(db, alice, status="processing", source_kind=None)
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
        lambda results, aid, dispatch_start: finalize_calls.append((results, aid)),
    )

    analysis_task.process_analysis.run(analysis.id)

    assert finalize_calls == [([], analysis.id)]


def test_process_analysis_raises_when_the_analysis_has_no_artifacts_at_all(monkeypatch):
    """Distinguishes the genuine "no persisted artifacts" bug case from
    the zombie-recovery case above - upload_file() already guarantees
    this can't happen on a fresh upload, so seeing it at all is a real
    error, not a recovery scenario."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind=None)
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "_mark_analysis_failed", lambda db, aid: None)

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
    analysis_task._finalize_analysis_task.run([], analysis_id, None)

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
    artifact = _artifact(
        db, analysis, status="processing", processed_bytes=500, last_processed_line=10,
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
    monkeypatch.setattr(analysis_task, "_mark_analysis_failed", lambda db_arg, aid: None)

    with pytest.raises(OperationalError):
        analysis_task._process_artifact_task.run(analysis_id, artifact_id)

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
        db, analysis, status="processing", processed_bytes=500, last_processed_line=10,
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
    monkeypatch.setattr(analysis_task, "_mark_analysis_failed", lambda db_arg, aid: None)

    with pytest.raises(OperationalError) as exc_info:
        analysis_task._process_artifact_task.run(analysis_id, artifact_id)

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

    def spy_bump(db_arg, aid):
        heartbeat_calls.append(aid)
        return real_bump(db_arg, aid)

    monkeypatch.setattr(analysis_task, "_bump_processing_heartbeat", spy_bump)

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    # Once at finalize entry, once more after Gemini resolves.
    assert heartbeat_calls == [analysis_id, analysis_id]
    db2 = session_factory()
    reloaded = db2.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.processing_heartbeat_at is not None
    db2.close()
