from types import SimpleNamespace
from unittest.mock import Mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.api import analysis as analysis_api
from app.db.database import Base
from app.models import User
from app.tasks import analysis as analysis_task

def _upload(filename: str, content: bytes):
    from io import BytesIO

    return SimpleNamespace(filename=filename, content_type="text/plain", file=BytesIO(content))

def _sqlite_db(tmp_path, monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())
    return db, user

def test_unsupported_artifact_publishes_a_live_outcome_event(tmp_path, monkeypatch):
    db, user = _sqlite_db(tmp_path, monkeypatch)
    published = []
    monkeypatch.setattr(
        analysis_api, "publish_artifact_outcome", lambda aid, payload: published.append(payload)
    )

    binary_content = b"\x00\x01\x02\xffPK\x03\x04random binary payload"
    analysis_api.upload_file(
        file=[_upload("app.log", b"ERROR failed"), _upload("payload.bin", binary_content)],
        db=db,
        current_user=SimpleNamespace(id=user.id),
    )

    assert len(published) == 1
    event = published[0]
    assert event["status"] == "unsupported"
    assert event["source_file"] == "payload.bin"
    assert event["source_format"] is None
    assert "analysis_id" in event
    assert "not supported" in event["message"].lower()

def test_duplicate_artifact_publishes_a_live_outcome_event_with_canonical_reference(
    tmp_path, monkeypatch
):
    db, user = _sqlite_db(tmp_path, monkeypatch)
    published = []
    monkeypatch.setattr(
        analysis_api, "publish_artifact_outcome", lambda aid, payload: published.append(payload)
    )

    same_bytes = b"ERROR identical payload\n"
    analysis_api.upload_file(
        file=[_upload("original.log", same_bytes), _upload("copy.log", same_bytes)],
        db=db,
        current_user=SimpleNamespace(id=user.id),
    )

    assert len(published) == 1
    event = published[0]
    assert event["status"] == "duplicate"
    assert event["source_file"] == "copy.log"
    assert event["duplicate_of_source_file"] == "original.log"
    assert "duplicate_of_artifact_id" in event
    assert event["evidence_count"] == 0

def test_evidence_bearing_artifact_does_not_publish_a_live_outcome_event_at_upload(
    tmp_path, monkeypatch
):
    db, user = _sqlite_db(tmp_path, monkeypatch)
    published = []
    monkeypatch.setattr(
        analysis_api, "publish_artifact_outcome", lambda aid, payload: published.append(payload)
    )

    analysis_api.upload_file(
        file=_upload("app.log", b"ERROR failed"),
        db=db,
        current_user=SimpleNamespace(id=user.id),
    )

    assert published == []

def _artifact_with_source(tmp_path, name: str, content: bytes):
    path = tmp_path / name
    path.write_bytes(content)
    return SimpleNamespace(
        id=101, position=0, original_filename=name, saved_file_path=str(path),
        content_type=None, size_bytes=path.stat().st_size, detected_format="generic",
        status="pending", last_processed_line=0, processed_bytes=0, duplicate_of_artifact_id=None,
    )

def test_zero_evidence_artifact_publishes_live_outcome_after_processing(monkeypatch, tmp_path):
    db = Mock()
    db.query.return_value.filter.return_value.scalar.return_value = 0
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = (
        "processing", 0,
    )
    db.query.return_value.filter.return_value.with_for_update.return_value.scalar.return_value = (
        "processing"
    )
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    artifact = _artifact_with_source(tmp_path, "quiet.log", b"INFO nothing to see here\n")

    published = []
    monkeypatch.setattr(
        analysis_task, "publish_artifact_outcome", lambda aid, payload: published.append(payload)
    )

    analysis_task._process_artifact(db=db, analysis=analysis, artifact=artifact, generation=0)

    assert artifact.status == "completed"
    assert len(published) == 1
    event = published[0]
    assert event["analysis_id"] == 9
    assert event["status"] == "processed"
    assert event["evidence_count"] == 0
    assert event["source_file"] == "quiet.log"
    assert "unrelated" not in event["message"].lower()

def test_evidence_bearing_artifact_does_not_publish_zero_evidence_event(monkeypatch, tmp_path):
    db = Mock()
    db.query.return_value.filter.return_value.scalar.return_value = 3
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = (
        "processing", 0,
    )
    db.query.return_value.filter.return_value.with_for_update.return_value.scalar.return_value = (
        "processing"
    )
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    artifact = _artifact_with_source(tmp_path, "loud.log", b"ERROR something failed\n")

    published = []
    monkeypatch.setattr(
        analysis_task, "publish_artifact_outcome", lambda aid, payload: published.append(payload)
    )

    analysis_task._process_artifact(db=db, analysis=analysis, artifact=artifact, generation=0)

    assert artifact.status == "completed"
    assert published == []
