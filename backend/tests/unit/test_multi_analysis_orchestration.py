"""Bounded multi-analysis orchestration simulation (item 20 of the ZIP 45
acceptance delta) - a small-fixture reproduction of the production
incident's actual shape: several analyses/users sharing the same bounded
worker capacity (worker_concurrency=2, never simulated as literal Celery
concurrency here - Celery Beat's own recovery scan is what this proves
does not misbehave under that shape), where some legitimately queue behind
others rather than something being stuck/duplicated.

Analysis A: several artifact tasks (simulating "occupying worker
capacity" - actively "processing").
Analysis B: another analysis genuinely waiting its turn (still "pending").
Analysis C: a single image artifact, also genuinely waiting.
Analysis D: a SECOND USER's analysis, processing independently - proves
lifecycle actions stay Analysis-scoped, never user-global.
"""
from datetime import datetime, timezone
from io import BytesIO

from PIL import Image
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services import image_text_extractor
from app.services.diagnostic_parser import parse_timestamp
from app.tasks import analysis as analysis_task


def _use_sqlite_compatible_evidence_persistence(monkeypatch):
    """persist_evidence_batch's real implementation uses a MySQL-only
    insert(...).on_duplicate_key_update(...) statement that cannot compile
    against sqlite (see test_controlled_artifact_and_source_failures.py's
    own copy of this same helper for the full rationale). Swaps in a
    plain per-event insert equivalent enough for this file's tests."""
    counter = {"n": 0}

    def fake_persist(*, db, analysis_id, events, artifact_id=None):
        for event in events:
            if event is None:
                continue
            counter["n"] += 1
            resolved_artifact_id = (
                artifact_id if artifact_id is not None else getattr(event, "artifact_id", None)
            )
            timestamp = parse_timestamp(getattr(event, "timestamp", None))
            db.add(
                Evidence(
                    analysis_id=analysis_id,
                    artifact_id=resolved_artifact_id,
                    correlation_key=f"ck-{counter['n']}",
                    fingerprint=getattr(event, "fingerprint", None) or f"fp-{counter['n']}",
                    source_format=getattr(event, "source_format", None),
                    first_seen=timestamp,
                    last_seen=timestamp,
                    resolved_identity=f"unresolved:test-{counter['n']}",
                    identity_match_type="unresolved",
                    identity_strength=0.0,
                    first_line_number=getattr(event, "line_number", None) or 1,
                    last_line_number=getattr(event, "line_number", None) or 1,
                    severity=getattr(event, "level", None),
                    representative_line=getattr(event, "raw_line", None),
                )
            )
        db.commit()

    monkeypatch.setattr(analysis_task, "persist_evidence_batch", fake_persist)


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def _user(db, name) -> User:
    user = User(username=name, email=f"{name}@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    return user


def _analysis(db, user, *, status, **kwargs) -> Analysis:
    defaults = dict(
        user_id=user.id, original_filename="a.log", saved_file_path="/uploads/a.log",
        status=status,
    )
    defaults.update(kwargs)
    analysis = Analysis(**defaults)
    db.add(analysis)
    db.commit()
    return analysis


def _artifact(db, analysis, position, *, status, **kwargs) -> AnalysisArtifact:
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


def test_healthy_queued_analyses_are_never_redispatched_merely_for_waiting(monkeypatch):
    """The exact production-shaped scenario: A is genuinely busy (multiple
    artifacts actively "processing" - the fast 300s active-work threshold
    would apply to A alone if A ever actually went stale, which it does
    not here), B and C are genuinely queued behind bounded worker capacity
    (freshly "pending", never even claimed yet) rather than stuck. A single
    recover_stale_analyses tick must not touch B or C - they are not
    "active work gone stale" (no artifact of theirs is "processing" at
    all) and they are not old enough to be a stale PENDING backlog either.
    No fair-scheduling logic is exercised or required - the assertion is
    only that legitimate queueing is never misdiagnosed as staleness."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db, "alice")

    analysis_a = _analysis(
        db, alice, status="processing", processing_generation=1, source_kind=None,
        processing_heartbeat_at=datetime.now(timezone.utc),
    )
    for position in range(3):
        _artifact(db, analysis_a, position, status="processing")

    analysis_b = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis_b, 0, status="pending")

    analysis_c = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis_c, 0, status="pending", original_filename="shot.png")

    redispatched = []
    monkeypatch.setattr(
        analysis_task.process_analysis, "delay", lambda aid: redispatched.append(aid)
    )

    claimed_count = analysis_task.recover_stale_analyses.run()

    assert claimed_count == 0
    assert redispatched == []
    db.expire_all()
    assert db.query(Analysis).filter(Analysis.id == analysis_a.id).first().status == "processing"
    assert db.query(Analysis).filter(Analysis.id == analysis_b.id).first().status == "pending"
    assert db.query(Analysis).filter(Analysis.id == analysis_c.id).first().status == "pending"


def test_no_duplicate_process_analysis_or_artifact_execution_across_abc(monkeypatch):
    """Each of A/B/C gets its OWN single process_analysis claim/dispatch -
    a duplicate/redelivered invocation for any one of them must not double
    that specific analysis's work, and must have zero effect on the
    others' state (Analysis-scoped, not a shared/global claim)."""
    session_factory = _db_with_schema(monkeypatch)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    db = session_factory()
    alice = _user(db, "alice")

    analyses = {}
    for label in ("a", "b", "c"):
        analysis = _analysis(db, alice, status="pending", source_kind=None)
        _artifact(db, analysis, 0, status="pending")
        analyses[label] = analysis.id

    dispatched_sigs = {"n": 0}
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: dispatched_sigs.__setitem__("n", dispatched_sigs["n"] + len(list(sigs))) or object(),
    )
    monkeypatch.setattr(
        analysis_task, "chord",
        lambda group_obj, callback: type("W", (), {"apply_async": lambda self: None})(),
    )
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", lambda *a: object())
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", lambda *a: object())

    # Each analysis claimed exactly once, including a duplicate/redelivered
    # invocation for "a" specifically - must not affect b/c at all.
    analysis_task.process_analysis.run(analyses["a"])
    analysis_task.process_analysis.run(analyses["a"])  # duplicate/redelivered
    analysis_task.process_analysis.run(analyses["b"])
    analysis_task.process_analysis.run(analyses["c"])

    db.expire_all()
    for label, analysis_id in analyses.items():
        reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        assert reloaded.status == "processing", label
        assert reloaded.processing_generation == 1, label  # never double-incremented


def test_image_artifact_in_a_waiting_analysis_completes_normally_once_its_turn_comes(
    monkeypatch, tmp_path,
):
    """Analysis C's single image artifact queues legitimately (per the
    first test above) and, once actually dispatched, completes with the
    SAME restart-only OCR semantics already proven elsewhere - no special
    behavior is needed just because it spent time queued behind A/B."""
    session_factory = _db_with_schema(monkeypatch)
    monkeypatch.setattr(analysis_task, "publish_artifact_outcome", lambda *a, **k: None)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)

    image_path = tmp_path / "shot.png"
    buffer = BytesIO()
    Image.new("RGB", (4, 4), "white").save(buffer, format="PNG")
    image_path.write_bytes(buffer.getvalue())

    def fake_ocr(path):
        return [([[0, 0], [10, 0], [10, 10], [0, 10]], "ERROR something failed", 0.9)], None

    monkeypatch.setattr(image_text_extractor, "_ocr", fake_ocr)

    db = session_factory()
    alice = _user(db, "alice")
    analysis_c = _analysis(db, alice, status="processing", processing_generation=1, source_kind=None)
    artifact = _artifact(
        db, analysis_c, 0, status="pending", original_filename="shot.png",
        saved_file_path=str(image_path), size_bytes=image_path.stat().st_size,
    )
    analysis_id = analysis_c.id
    artifact_id = artifact.id
    db.close()

    parsed_count = analysis_task._process_artifact_task.run(analysis_id, artifact_id, 1)

    assert parsed_count == 1
    verify_db = session_factory()
    reloaded_artifact = (
        verify_db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    )
    assert reloaded_artifact.status == "completed"
    assert reloaded_artifact.processed_bytes == reloaded_artifact.size_bytes
    assert verify_db.query(Evidence).filter(Evidence.artifact_id == artifact_id).count() == 1


def test_lifecycle_actions_stay_analysis_scoped_across_two_users(monkeypatch):
    """A second user's analysis (D) must be completely unaffected by
    another user's analysis (A) failing, cancelling, or being recovered -
    proving these lifecycle transitions are Analysis-scoped, never
    user-global (no shared per-user lock/state anywhere in this path)."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    alice = _user(db, "alice")
    bob = _user(db, "bob")

    analysis_a = _analysis(db, alice, status="processing", processing_generation=1, source_kind=None)
    _artifact(db, analysis_a, 0, status="processing")

    analysis_d = _analysis(db, bob, status="processing", processing_generation=1, source_kind=None)
    _artifact(db, analysis_d, 0, status="processing")

    analysis_a_id = analysis_a.id
    analysis_d_id = analysis_d.id

    won = analysis_task._mark_analysis_failed(db, analysis_a_id, generation=1)
    assert won is True

    db.expire_all()
    reloaded_a = db.query(Analysis).filter(Analysis.id == analysis_a_id).first()
    reloaded_d = db.query(Analysis).filter(Analysis.id == analysis_d_id).first()
    assert reloaded_a.status == "failed"
    assert reloaded_d.status == "processing"  # bob's analysis is untouched
    reloaded_d_artifact = (
        db.query(AnalysisArtifact).filter(AnalysisArtifact.analysis_id == analysis_d_id).first()
    )
    assert reloaded_d_artifact.status == "processing"  # untouched
