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
    UPLOADS_DIR.mkdir(exist_ok=True)
    path = UPLOADS_DIR / f"test_source_zip_cleanup_{uuid.uuid4().hex}_source.zip"
    path.write_bytes(b"PK\x03\x04fake-zip-bytes")
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)

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
    assert reloaded.source_reference == str(staged_zip)
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
    staged_zip.unlink()

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
    assert reloaded.status == "cancelled"

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
    assert reloaded.status == "processing"
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
    assert reloaded.source_status == "unavailable"
    assert reloaded.status == "processing"

def test_source_failure_ignored_when_analysis_already_cancelled(monkeypatch):
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

def test_successful_zip_preparation_removes_staged_archive_only_after_ready_commit(
    staged_zip,
    monkeypatch,
):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:"
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)

    db = session_factory()

    alice = _user(db)

    analysis = _analysis(
        db,
        alice,
        status="processing",
        source_kind="zip",
        source_reference=str(staged_zip),
        source_status=None,
    )

    analysis_id = analysis.id
    db.close()

    monkeypatch.setattr(
        analysis_task,
        "sessionLocal",
        session_factory,
    )

    monkeypatch.setattr(
        analysis_task,
        "_bump_processing_heartbeat",
        lambda *_a, **_k: None,
    )

    real_index = object()

    def fake_acquire(
        analysis_arg,
        generation_arg,
        publish_callback=None,
    ):
        assert staged_zip.exists()
        assert publish_callback is not None

        return publish_callback(
            lambda: real_index
        )

    monkeypatch.setattr(
        analysis_task,
        "_acquire_source_index",
        fake_acquire,
    )

    observed_source_status = []

    real_remove = (
        analysis_task._remove_staged_source_archive
    )

    def tracked_remove(reference):
        check_db = session_factory()

        try:
            observed_source_status.append(
                check_db.query(
                    Analysis.source_status
                )
                .filter(
                    Analysis.id == analysis_id
                )
                .scalar()
            )
        finally:
            check_db.close()

        real_remove(reference)

    monkeypatch.setattr(
        analysis_task,
        "_remove_staged_source_archive",
        tracked_remove,
    )

    analysis_task._prepare_source_task.run(
        analysis_id,
        0,
    )

    check_db = session_factory()

    try:
        reloaded = (
            check_db.query(Analysis)
            .filter(
                Analysis.id == analysis_id
            )
            .one()
        )

        assert reloaded.source_status == "ready"

    finally:
        check_db.close()

    assert observed_source_status == ["ready"]
    assert not staged_zip.exists()

def test_staged_zip_unlink_oserror_after_ready_is_housekeeping_only(
    staged_zip,
    monkeypatch,
):
    db = _session()

    alice = _user(db)

    analysis = _analysis(
        db,
        alice,
        status="processing",
        source_kind="zip",
        source_reference=str(staged_zip),
        source_status="ready",
    )

    monkeypatch.setattr(
        analysis_task,
        "_remove_staged_source_archive",
        lambda ref: (
            _ for _ in ()
        ).throw(
            OSError("permission denied")
        ),
    )

    analysis_task._remove_staged_zip_after_ready(
        analysis
    )

    db.expire_all()

    assert analysis.status == "processing"
    assert analysis.source_status == "ready"
    assert staged_zip.exists()

def test_source_index_cache_does_not_leak_across_generations(monkeypatch):
    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})
    analysis = SimpleNamespace(
        id=77, source_kind="github", source_reference="https://github.com/acme/project",
    )
    build_calls = []

    def fake_prepare(kind, ref, aid, gen):
        index = object()
        build_calls.append(index)
        return index

    monkeypatch.setattr(analysis_task, "prepare_source", fake_prepare)
    monkeypatch.setattr(analysis_task, "_remove_staged_source_archive", lambda ref: None)

    generation_1_index = analysis_task._acquire_source_index(analysis, 1)
    generation_1_index_again = analysis_task._acquire_source_index(analysis, 1)
    generation_2_index = analysis_task._acquire_source_index(analysis, 2)

    assert generation_1_index_again is generation_1_index
    assert generation_2_index is not generation_1_index
    assert len(build_calls) == 2

def test_batch_persistence_stops_correlating_once_source_becomes_unavailable_elsewhere(
    monkeypatch,
):
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

    stale_source_index = object()
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", level="ERROR")
    batch = [NS(event=event, end_offset=10, artifact_line_number=1, global_end_line_number=1)]

    result = analysis_task._persist_artifact_batch(
        db=db, analysis=analysis, artifact=artifact, generation=0,
        batch=batch, source_index=stale_source_index,
    )

    assert result == 1
    assert correlate_calls == []
    assert event.source_matches == []

def test_artifact_level_source_failure_never_deletes_the_canonical_tree_a_sibling_may_be_reading(
    tmp_path, monkeypatch,
):
    from app.services import source_archive

    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    canonical_dir = tmp_path / "sources" / "1"
    canonical_dir.mkdir(parents=True)
    (canonical_dir / "app.py").write_text("print('still here')\n")
    source_archive._ready_marker(canonical_dir).touch()

    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, status="processing", source_kind="github",
        source_reference="https://github.com/acme/project", source_status="ready",
    )

    handled = analysis_task._record_optional_source_failure(
        db, analysis, SourceInputError("published source tree unexpectedly unavailable"),
        generation=0, remove_prepared_source=False,
    )

    assert handled is True
    assert analysis.source_status == "unavailable"
    assert canonical_dir.exists()
    assert (canonical_dir / "app.py").exists()
    assert source_archive._ready_marker(canonical_dir).exists()
