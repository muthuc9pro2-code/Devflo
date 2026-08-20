"""Content-based (SHA-256) duplicate-artifact detection.

Covers the three layers this task touches:
  - upload_staging.copy_upload: streaming digest, bounded memory.
  - crud.analysis.create_analysis: within-analysis duplicate resolution
    (first occurrence of a content hash stays canonical/pending, later
    occurrences become status="duplicate" pointing at it).
  - tasks.analysis.process_analysis: duplicate/unsupported artifacts are
    excluded from Celery dispatch entirely - never processed, never
    produce their own Evidence, never participate in correlation.

No hash/duplicate concept existed anywhere in the repository before this
change (confirmed by inspection - AnalysisArtifact had no such column), so
this adds exactly one persisted identity: content_sha256 plus
duplicate_of_artifact_id, reusing the existing `status` column/vocabulary
rather than a second tracking system.
"""
import hashlib
from types import SimpleNamespace
from unittest.mock import Mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.crud.analysis import create_analysis
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, User
from app.services.upload_staging import copy_upload
from app.tasks import analysis as analysis_task


def _sqlite_session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    return session_factory, db, user


# --- copy_upload: streaming SHA-256, bounded memory ------------------------


class _TrackedReadFile:
    """Fails the test if anything ever asks for more than chunk_bytes at
    once - the only way to prove "never load the whole artifact into RAM"
    from a unit test."""

    def __init__(self, content: bytes, chunk_bytes: int):
        self._buffer = content
        self._offset = 0
        self._chunk_bytes = chunk_bytes
        self.max_requested = 0

    def read(self, size=-1):
        assert size != -1 and size <= self._chunk_bytes, (
            f"requested {size} bytes at once; expected <= {self._chunk_bytes}"
        )
        self.max_requested = max(self.max_requested, size)
        chunk = self._buffer[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def test_copy_upload_streams_in_bounded_chunks_and_computes_the_real_digest(tmp_path):
    content = (b"ERROR database timeout\n" * 50_000)  # ~1.2MB, larger than any single chunk
    chunk_bytes = 8192
    upload = SimpleNamespace(file=_TrackedReadFile(content, chunk_bytes))
    target = tmp_path / "large.log"

    size, sample, content_sha256 = copy_upload(
        upload, target, max_bytes=len(content) + 1, detail="too big", chunk_bytes=chunk_bytes
    )

    assert size == len(content)
    assert content_sha256 == hashlib.sha256(content).hexdigest()
    assert upload.file.max_requested <= chunk_bytes
    assert target.read_bytes() == content


def test_copy_upload_digest_differs_for_different_content(tmp_path):
    upload_a = SimpleNamespace(file=_TrackedReadFile(b"content A", 64))
    upload_b = SimpleNamespace(file=_TrackedReadFile(b"content B", 64))

    _, _, digest_a = copy_upload(upload_a, tmp_path / "a", 100, "x", 64)
    _, _, digest_b = copy_upload(upload_b, tmp_path / "b", 100, "x", 64)

    assert digest_a != digest_b


def test_copy_upload_digest_matches_for_identical_content_different_filenames(tmp_path):
    upload_a = SimpleNamespace(file=_TrackedReadFile(b"identical bytes here", 64))
    upload_b = SimpleNamespace(file=_TrackedReadFile(b"identical bytes here", 64))

    _, _, digest_a = copy_upload(upload_a, tmp_path / "original.log", 100, "x", 64)
    _, _, digest_b = copy_upload(upload_b, tmp_path / "copy.log", 100, "x", 64)

    assert digest_a == digest_b


# --- create_analysis: within-analysis duplicate resolution -----------------


def test_identical_bytes_different_filename_is_flagged_duplicate():
    session_factory, db, user = _sqlite_session()
    digest = hashlib.sha256(b"same bytes").hexdigest()

    analysis = create_analysis(
        db=db,
        user_id=user.id,
        filename="original.log",
        saved_file_path="uploads/original.log",
        artifacts=[
            {
                "original_filename": "original.log",
                "saved_file_path": "uploads/original.log",
                "size_bytes": 10,
                "detected_format": "web_server",
                "content_sha256": digest,
            },
            {
                "original_filename": "copy.log",
                "saved_file_path": "uploads/copy.log",
                "size_bytes": 10,
                "detected_format": "web_server",
                "content_sha256": digest,
            },
        ],
    )

    rows = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis.id)
        .order_by(AnalysisArtifact.position)
        .all()
    )
    original, copy = rows
    assert original.status == "pending"
    assert original.duplicate_of_artifact_id is None
    assert copy.status == "duplicate"
    assert copy.duplicate_of_artifact_id == original.id
    db.close()


def test_duplicate_staged_bytes_are_deleted_but_metadata_and_canonical_survive(
    tmp_path, monkeypatch
):
    """Section 6: duplicate rows were previously marked "duplicate" in the
    DB but their staged physical bytes were never reclaimed. Only the
    duplicate's file must be deleted - never the canonical's - and only
    after the DB has durably established the relationship; all metadata
    (filename, duplicate_of_artifact_id) must remain queryable."""
    from app.crud import analysis as crud_analysis

    monkeypatch.setattr(crud_analysis, "_UPLOAD_ROOT", tmp_path.resolve())

    canonical_path = tmp_path / "original.log"
    duplicate_path = tmp_path / "copy.log"
    canonical_path.write_bytes(b"same bytes")
    duplicate_path.write_bytes(b"same bytes")

    session_factory, db, user = _sqlite_session()
    digest = hashlib.sha256(b"same bytes").hexdigest()

    analysis = create_analysis(
        db=db,
        user_id=user.id,
        filename="original.log",
        saved_file_path=str(canonical_path),
        artifacts=[
            {
                "original_filename": "original.log",
                "saved_file_path": str(canonical_path),
                "size_bytes": 10,
                "detected_format": "web_server",
                "content_sha256": digest,
            },
            {
                "original_filename": "copy.log",
                "saved_file_path": str(duplicate_path),
                "size_bytes": 10,
                "detected_format": "web_server",
                "content_sha256": digest,
            },
        ],
    )

    assert canonical_path.exists()
    assert not duplicate_path.exists()

    rows = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis.id)
        .order_by(AnalysisArtifact.position)
        .all()
    )
    original, copy = rows
    assert original.status == "pending"
    assert copy.status == "duplicate"
    assert copy.duplicate_of_artifact_id == original.id
    assert copy.original_filename == "copy.log"
    db.close()


def test_duplicate_deletion_refuses_to_delete_outside_the_upload_root(tmp_path, monkeypatch):
    """Safety: even if a saved_file_path somehow pointed outside the upload
    root, the deletion helper must refuse rather than deleting it."""
    from app.crud import analysis as crud_analysis

    monkeypatch.setattr(crud_analysis, "_UPLOAD_ROOT", (tmp_path / "uploads").resolve())

    outside_path = tmp_path / "outside.log"
    outside_path.write_bytes(b"do not delete me")

    crud_analysis._delete_staged_upload(str(outside_path))

    assert outside_path.exists()


def test_identical_filename_different_bytes_is_not_duplicate():
    session_factory, db, user = _sqlite_session()

    analysis = create_analysis(
        db=db,
        user_id=user.id,
        filename="app.log",
        saved_file_path="uploads/app.log",
        artifacts=[
            {
                "original_filename": "app.log",
                "saved_file_path": "uploads/app_1.log",
                "size_bytes": 10,
                "detected_format": "generic",
                "content_sha256": hashlib.sha256(b"first version").hexdigest(),
            },
            {
                "original_filename": "app.log",
                "saved_file_path": "uploads/app_2.log",
                "size_bytes": 12,
                "detected_format": "generic",
                "content_sha256": hashlib.sha256(b"second, different version").hexdigest(),
            },
        ],
    )

    rows = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis.id)
        .order_by(AnalysisArtifact.position)
        .all()
    )
    assert [row.status for row in rows] == ["pending", "pending"]
    assert all(row.duplicate_of_artifact_id is None for row in rows)
    db.close()


def test_multiple_different_artifacts_remain_unchanged():
    session_factory, db, user = _sqlite_session()

    analysis = create_analysis(
        db=db,
        user_id=user.id,
        filename="a.log",
        saved_file_path="uploads/a.log",
        artifacts=[
            {
                "original_filename": name,
                "saved_file_path": f"uploads/{name}",
                "size_bytes": 10,
                "detected_format": "generic",
                "content_sha256": hashlib.sha256(name.encode()).hexdigest(),
            }
            for name in ("a.log", "b.log", "c.log")
        ],
    )

    rows = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis.id)
        .order_by(AnalysisArtifact.position)
        .all()
    )
    assert [row.status for row in rows] == ["pending", "pending", "pending"]
    assert [row.duplicate_of_artifact_id for row in rows] == [None, None, None]
    db.close()


def test_unsupported_artifacts_are_excluded_from_duplicate_grouping():
    """Two unsupported artifacts with identical bytes must not be linked
    as duplicate-of-each-other - duplicate detection only applies to
    artifacts that would otherwise be processed."""
    session_factory, db, user = _sqlite_session()
    digest = hashlib.sha256(b"binary junk").hexdigest()

    analysis = create_analysis(
        db=db,
        user_id=user.id,
        filename="a.bin",
        saved_file_path="uploads/a.bin",
        artifacts=[
            {
                "original_filename": "a.bin",
                "saved_file_path": "uploads/a.bin",
                "size_bytes": 5,
                "detected_format": None,
                "content_sha256": digest,
                "status": "unsupported",
            },
            {
                "original_filename": "b.bin",
                "saved_file_path": "uploads/b.bin",
                "size_bytes": 5,
                "detected_format": None,
                "content_sha256": digest,
                "status": "unsupported",
            },
        ],
    )

    rows = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis.id)
        .order_by(AnalysisArtifact.position)
        .all()
    )
    assert [row.status for row in rows] == ["unsupported", "unsupported"]
    assert all(row.duplicate_of_artifact_id is None for row in rows)
    db.close()


# --- Celery dispatch: duplicates/unsupported never processed ---------------


def test_duplicate_and_unsupported_artifacts_are_excluded_from_dispatch(monkeypatch):
    """3/4/9. The original (canonical) artifact is dispatched normally; the
    duplicate and an unsupported artifact are not - proving a duplicate
    cannot produce independent evidence or participate in correlation,
    because it is never even handed to the ingestion pipeline."""
    session_factory, db, user = _sqlite_session()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="pending"
    )
    db.add(analysis)
    db.commit()

    original = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="original.log",
        saved_file_path="original.log", size_bytes=10, detected_format="web_server",
        status="pending", content_sha256="abc123",
    )
    db.add(original)
    db.commit()
    duplicate = AnalysisArtifact(
        analysis_id=analysis.id, position=1, original_filename="copy.log",
        saved_file_path="copy.log", size_bytes=10, detected_format="web_server",
        status="duplicate", content_sha256="abc123", duplicate_of_artifact_id=original.id,
        processed_bytes=10,
    )
    unsupported = AnalysisArtifact(
        analysis_id=analysis.id, position=2, original_filename="weird.xyz",
        saved_file_path="weird.xyz", size_bytes=3, detected_format=None,
        status="unsupported", processed_bytes=3,
    )
    db.add_all([duplicate, unsupported])
    db.commit()
    analysis_id = analysis.id
    original_id = original.id
    db.close()

    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    captured = {}
    monkeypatch.setattr(
        analysis_task,
        "group",
        lambda sigs: captured.setdefault("sigs", list(sigs)) or SimpleNamespace(),
    )
    monkeypatch.setattr(analysis_task, "chord", Mock(return_value=Mock()))
    process_sig = Mock()
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", Mock(return_value=process_sig))
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", Mock())

    analysis_task.process_analysis.run(analysis_id)

    dispatched_ids = {
        call.args[1] for call in analysis_task._process_artifact_task.si.call_args_list
    }
    assert dispatched_ids == {original_id}  # only the canonical artifact


def test_finalize_does_not_block_on_duplicate_or_unsupported_artifacts(monkeypatch):
    """A duplicate/unsupported artifact never reaches status="completed" -
    finalize must not wait for it forever."""
    session_factory, db, user = _sqlite_session()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    db.add(analysis)
    db.commit()
    db.add_all(
        [
            AnalysisArtifact(
                analysis_id=analysis.id, position=0, original_filename="original.log",
                saved_file_path="original.log", size_bytes=10, detected_format="web_server",
                status="completed", last_processed_line=1, processed_bytes=10,
            ),
            AnalysisArtifact(
                analysis_id=analysis.id, position=1, original_filename="copy.log",
                saved_file_path="copy.log", size_bytes=10, detected_format="web_server",
                status="duplicate", processed_bytes=10, duplicate_of_artifact_id=1,
            ),
            AnalysisArtifact(
                analysis_id=analysis.id, position=2, original_filename="weird.xyz",
                saved_file_path="weird.xyz", size_bytes=3, detected_format=None,
                status="unsupported", processed_bytes=3,
            ),
        ]
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    investigation_calls = []
    monkeypatch.setattr(
        analysis_task,
        "publish_investigation_result",
        lambda aid, p: investigation_calls.append(p),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    # Reached the zero-evidence completion path (no Evidence rows at all
    # here) instead of being stuck skipping finalize forever.
    assert len(investigation_calls) == 1
    db2 = session_factory()
    try:
        assert db2.query(Analysis).filter(Analysis.id == analysis_id).first().status == "completed"
    finally:
        db2.close()
