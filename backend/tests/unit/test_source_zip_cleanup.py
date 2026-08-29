"""Reclaiming the original staged optional source ZIP.

Successful ZIP preparation already removes the original staged archive
(app.tasks.analysis._prepare_source_index, via _remove_staged_source_archive)
once prepare_source() has copied its bytes into investigation-scoped
storage. Two paths previously left that original archive behind on disk:

  1. cancellation before _prepare_source_index() ever reaches the
     successful-preparation archive removal;
  2. optional source preparation failure (_record_optional_source_failure),
     which cleaned the *prepared* source but never the *original staged*
     archive.

Both call sites reuse the existing _remove_staged_source_archive() helper -
this file proves that reuse happens, in the right order (durable DB state
committed first, filesystem cleanup best-effort afterward), idempotently,
and without ever turning a cancellation/failure into an analysis-wide
failure when the filesystem cleanup itself raises OSError.
"""
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.source_archive import SourceInputError
from app.tasks import analysis as analysis_task
from app.tasks.analysis import (
    _record_optional_source_failure,
    cancel_analysis_and_cleanup,
)

UPLOADS_DIR = Path("uploads")


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


@pytest.fixture
def staged_zip():
    """A real file under uploads/ - _remove_staged_source_archive() only
    ever acts on a reference whose parent resolves to this exact
    directory, so a real path (not tmp_path) is required to exercise it
    honestly. Cleaned up unconditionally afterward."""
    UPLOADS_DIR.mkdir(exist_ok=True)
    path = UPLOADS_DIR / f"test_source_zip_cleanup_{uuid.uuid4().hex}_source.zip"
    path.write_bytes(b"PK\x03\x04fake-zip-bytes")
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


# --- Cancellation reclaims the original staged ZIP -------------------------


def test_cancel_zip_source_analysis_removes_staged_archive_and_keeps_rows(staged_zip):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="zip", source_reference=str(staged_zip), source_status=None,
    )
    artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="f0.log",
        saved_file_path="/uploads/f0.log", size_bytes=10, status="processing",
    )
    db.add(artifact)
    db.commit()
    evidence = Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
    )
    db.add(evidence)
    db.commit()

    assert staged_zip.exists()

    result = cancel_analysis_and_cleanup(db, analysis.id)

    assert result == "processing"
    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded is not None
    assert reloaded.status == "cancelled"
    assert reloaded.source_reference == str(staged_zip)  # metadata preserved
    assert db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact.id).first() is not None
    assert db.query(Evidence).filter(Evidence.analysis_id == analysis.id).count() == 0
    assert not staged_zip.exists()


def test_cancel_zip_source_when_archive_already_deleted_is_idempotent(staged_zip):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="pending",
        source_kind="zip", source_reference=str(staged_zip), source_status=None,
    )
    staged_zip.unlink()  # simulate another path having already removed it

    result = cancel_analysis_and_cleanup(db, analysis.id)

    assert result == "pending"
    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.status == "cancelled"
    assert not staged_zip.exists()


def test_cancel_github_source_does_not_attempt_staged_archive_removal(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="github", source_reference="https://github.com/owner/repo",
        source_status=None,
    )
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(AssertionError("must never remove a github reference")),
    )

    result = cancel_analysis_and_cleanup(db, analysis.id)

    assert result == "processing"
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


def test_cancel_no_source_analysis_attempts_no_staged_cleanup(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None, source_reference=None)
    monkeypatch.setattr(
        analysis_task, "cleanup_prepared_source",
        lambda aid: (_ for _ in ()).throw(AssertionError("must never clean prepared source for a no-source analysis")),
    )
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(AssertionError("must never attempt staged-archive removal")),
    )

    result = cancel_analysis_and_cleanup(db, analysis.id)

    assert result == "pending"
    assert db.query(Analysis).filter(Analysis.id == analysis.id).first().status == "cancelled"


def test_cancel_zip_source_archive_unlink_oserror_does_not_break_the_tombstone(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="zip", source_reference="uploads/does-not-matter_source.zip",
        source_status=None,
    )
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(OSError("permission denied")),
    )

    result = cancel_analysis_and_cleanup(db, analysis.id)

    assert result == "processing"
    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.status == "cancelled"  # the durable tombstone still wins


# --- Optional source preparation failure reclaims the staged ZIP -----------


def test_zip_source_preparation_failure_marks_unavailable_and_removes_staged_archive(staged_zip):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="zip", source_reference=str(staged_zip), source_status=None,
    )

    handled = _record_optional_source_failure(
        db, analysis, SourceInputError("corrupt archive"), generation=0,
    )

    assert handled is True
    assert analysis.source_status == "unavailable"
    assert "corrupt archive" in analysis.source_failure_reason
    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.status == "processing"  # diagnostic analysis remains viable
    assert reloaded.source_reference == str(staged_zip)
    assert not staged_zip.exists()


def test_github_source_preparation_failure_does_not_attempt_staged_archive_removal(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="github", source_reference="https://github.com/owner/repo",
        source_status=None,
    )
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(AssertionError("must never remove a github reference")),
    )

    handled = _record_optional_source_failure(
        db, analysis, SourceInputError("repository not found"), generation=0,
    )

    assert handled is True
    assert analysis.source_status == "unavailable"


def test_source_failure_archive_unlink_oserror_does_not_break_unavailable_state(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="zip", source_reference="uploads/does-not-matter_source.zip",
        source_status=None,
    )
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(OSError("permission denied")),
    )

    handled = _record_optional_source_failure(
        db, analysis, SourceInputError("corrupt archive"), generation=0,
    )

    assert handled is True
    reloaded = db.query(Analysis).filter(Analysis.id == analysis.id).first()
    assert reloaded.source_status == "unavailable"  # durable state still wins
    assert reloaded.status == "processing"  # never turned into an analysis-wide failure


def test_source_failure_ignored_when_analysis_already_cancelled(monkeypatch):
    """The pre-existing cancellation fence: no new-archive-removal
    behavior should run at all once cancellation already won."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="cancelled",
        source_kind="zip", source_reference="uploads/does-not-matter_source.zip",
        source_status=None,
    )
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(AssertionError("must never run for a cancelled analysis")),
    )

    handled = _record_optional_source_failure(
        db, analysis, SourceInputError("corrupt archive"), generation=0,
    )

    assert handled is False
    assert analysis.source_status is None


# --- Successful preparation's existing archive-removal is unchanged --------


def test_successful_zip_preparation_still_removes_the_staged_archive(staged_zip, monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="zip", source_reference=str(staged_zip), source_status=None,
    )
    monkeypatch.setattr(analysis_task, "prepare_source", lambda kind, ref, aid: object())

    index = analysis_task._prepare_source_index(analysis, 0)

    assert index is not None
    assert not staged_zip.exists()


# --- A staged-ZIP cleanup failure AFTER a real success is not a source failure --


def test_staged_zip_unlink_oserror_after_success_does_not_mark_source_unavailable(
    staged_zip, monkeypatch,
):
    """prepare_source() itself already fully succeeded (tree + index +
    manifest durably complete) by the time _remove_staged_source_archive
    runs - an OSError from THAT purely-cosmetic cleanup step must never be
    reinterpreted as "the source failed to prepare". _prepare_source_index
    must return the real index, not raise SourceSubsystemError."""
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing",
        source_kind="zip", source_reference=str(staged_zip), source_status=None,
    )
    # The process-local cache is module-level and keyed by
    # (analysis.id, generation) - this test's fresh in-memory DB restarts
    # ids at 1, so without resetting the cache a stale entry left by an
    # earlier test in this file could mask what this test actually checks.
    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})
    real_index = object()
    monkeypatch.setattr(analysis_task, "prepare_source", lambda kind, ref, aid: real_index)
    monkeypatch.setattr(
        analysis_task, "_remove_staged_source_archive",
        lambda ref: (_ for _ in ()).throw(OSError("permission denied")),
    )

    index = analysis_task._prepare_source_index(analysis, 0)

    assert index is real_index
    assert analysis.source_status is None  # never flipped to "unavailable"


# --- Process-local cache is generation-scoped, never analysis_id alone -----


def test_source_index_cache_does_not_leak_across_generations(monkeypatch):
    """A worker that built/cached a SourceIndex for processing_generation 1
    of some analysis_id must not hand that SAME cached object back for
    processing_generation 2 of the SAME analysis_id - recovery can demote a
    stale "processing" analysis back to "pending" and a later
    process_analysis call establishes a brand-new generation for it, and a
    long-lived worker process must never confuse the two."""
    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})
    analysis = SimpleNamespace(
        id=77, source_kind="github", source_reference="https://github.com/acme/project",
    )
    build_calls = []

    def fake_prepare(kind, ref, aid):
        index = object()
        build_calls.append(index)
        return index

    monkeypatch.setattr(analysis_task, "prepare_source", fake_prepare)
    monkeypatch.setattr(analysis_task, "_remove_staged_source_archive", lambda ref: None)

    generation_1_index = analysis_task._prepare_source_index(analysis, 1)
    generation_1_index_again = analysis_task._prepare_source_index(analysis, 1)
    generation_2_index = analysis_task._prepare_source_index(analysis, 2)

    assert generation_1_index_again is generation_1_index  # same generation: cached
    assert generation_2_index is not generation_1_index  # new generation: rebuilt
    assert len(build_calls) == 2


# --- Cross-worker "source became unavailable" observation (item 4E) --------


def test_batch_persistence_stops_correlating_once_source_becomes_unavailable_elsewhere(
    monkeypatch,
):
    """A worker holding a process-local SourceIndex object has no way to
    know another worker/session just committed source_status="unavailable"
    for the same analysis (its own matcher failure, or a source-prep
    failure discovered later). The very next batch this worker persists
    must observe that durable flag and stop attempting NEW correlation -
    while diagnostic Evidence persistence for that batch still proceeds
    exactly as normal, never blocked by this check."""
    from types import SimpleNamespace as NS
    from app.services.log_praser import ParsedEvent

    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", source_kind="github",
        source_reference="https://github.com/acme/project", source_status="unavailable",
    )
    artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="f0.log",
        saved_file_path="/uploads/f0.log", size_bytes=10, status="processing",
    )
    db.add(artifact)
    db.commit()

    monkeypatch.setattr(analysis_task, "persist_evidence_batch", lambda **k: None)
    correlate_calls = []
    monkeypatch.setattr(
        analysis_task, "correlate_event",
        lambda event, index: correlate_calls.append(event) or [],
    )

    stale_source_index = object()  # this worker's own still-live cached reference
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", level="ERROR")
    batch = [NS(event=event, end_offset=10, artifact_line_number=1, global_end_line_number=1)]

    result = analysis_task._persist_artifact_batch(
        db=db, analysis=analysis, artifact=artifact, generation=0,
        batch=batch, source_index=stale_source_index,
    )

    assert result == 1  # Evidence persistence still proceeded
    assert correlate_calls == []  # but no NEW correlation was attempted
    assert event.source_matches == []
