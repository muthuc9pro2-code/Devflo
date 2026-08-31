from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.crud import analysis as crud_analysis
from app.crud.analysis import ActiveAnalysisLimitReached, create_analysis
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, User


def test_create_analysis_bulk_adds_artifacts_in_one_transaction(monkeypatch):
    db = Mock()
    monkeypatch.setattr(
        crud_analysis, "_ensure_user_analysis_capacity", lambda *_args, **_kwargs: None
    )

    def assign_id():
        db.add.call_args.args[0].id = 13

    db.flush.side_effect = assign_id
    analysis = create_analysis(
        db=db,
        user_id=2,
        filename="one.txt",
        saved_file_path="uploads/one.txt",
        source_kind="zip",
        source_reference="uploads/sources/source-1",
        artifacts=[
            {
                "original_filename": "one.txt",
                "saved_file_path": "uploads/one.txt",
                "content_type": "text/plain",
                "size_bytes": 3,
            },
            {
                "original_filename": "two.txt",
                "saved_file_path": "uploads/two.txt",
                "content_type": "text/plain",
                "size_bytes": 4,
            },
        ],
    )

    rows = db.add_all.call_args.args[0]
    assert analysis.id == 13
    assert analysis.source_kind == "zip"
    assert analysis.source_reference == "uploads/sources/source-1"
    assert all(isinstance(row, AnalysisArtifact) for row in rows)
    assert [row.position for row in rows] == [0, 1]
    db.commit.assert_called_once()


def test_create_analysis_rolls_back_on_artifact_failure(monkeypatch):
    db = Mock()
    monkeypatch.setattr(
        crud_analysis, "_ensure_user_analysis_capacity", lambda *_args, **_kwargs: None
    )
    db.flush.side_effect = RuntimeError("database failed")

    with pytest.raises(RuntimeError):
        create_analysis(
            db=db,
            user_id=2,
            filename="one.txt",
            saved_file_path="uploads/one.txt",
        )

    db.rollback.assert_called_once()


def _real_db_with_user():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(
        username="quota-user",
        email="quota@example.com",
        hashed_password="x",
        is_verified=True,
    )
    db.add(user)
    db.commit()
    return db, user


def test_create_analysis_allows_three_nonterminal_analyses_and_ignores_terminal_rows():
    db, user = _real_db_with_user()
    db.add_all(
        [
            Analysis(
                user_id=user.id,
                original_filename=f"terminal-{status}.log",
                saved_file_path=f"uploads/terminal-{status}.log",
                status=status,
            )
            for status in ("completed", "failed", "cancelled")
        ]
        + [
            Analysis(
                user_id=user.id,
                original_filename="pending.log",
                saved_file_path="uploads/pending.log",
                status="pending",
            ),
            Analysis(
                user_id=user.id,
                original_filename="processing.log",
                saved_file_path="uploads/processing.log",
                status="processing",
            ),
        ]
    )
    db.commit()

    created = create_analysis(
        db=db,
        user_id=user.id,
        filename="third.log",
        saved_file_path="uploads/third.log",
    )

    assert created.status == "pending"
    assert (
        db.query(Analysis)
        .filter(
            Analysis.user_id == user.id,
            Analysis.status.in_(("pending", "processing")),
        )
        .count()
        == 3
    )
    db.close()


def test_create_analysis_rejects_fourth_nonterminal_analysis_and_rolls_back():
    db, user = _real_db_with_user()
    db.add_all(
        [
            Analysis(
                user_id=user.id,
                original_filename=f"active-{index}.log",
                saved_file_path=f"uploads/active-{index}.log",
                status="processing" if index == 0 else "pending",
            )
            for index in range(3)
        ]
    )
    db.commit()
    before = db.query(Analysis).count()

    with pytest.raises(ActiveAnalysisLimitReached, match="3 active investigations"):
        create_analysis(
            db=db,
            user_id=user.id,
            filename="blocked.log",
            saved_file_path="uploads/blocked.log",
        )

    assert db.query(Analysis).count() == before
    db.close()


def test_duplicate_cleanup_performs_no_database_read_after_commit(tmp_path, monkeypatch):
    """Duplicate-file cleanup after durable creation must be filesystem-only.

    SQLAlchemy expires ORM instances on commit by default. A stale implementation
    that reads duplicate.saved_file_path after commit therefore performs an
    implicit SELECT and can re-enter the upload's pre-durable failure path if
    the database disappears immediately after commit.
    """
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(
        username="duplicate-boundary-user",
        email="duplicate-boundary@example.com",
        hashed_password="x",
        is_verified=True,
    )
    db.add(user)
    db.commit()

    canonical_path = tmp_path / "canonical.log"
    duplicate_path = tmp_path / "duplicate.log"
    canonical_path.write_bytes(b"same diagnostic bytes")
    duplicate_path.write_bytes(b"same diagnostic bytes")

    monkeypatch.setattr(crud_analysis, "_UPLOAD_ROOT", tmp_path.resolve())

    post_commit = False

    def mark_target_commit(_session):
        nonlocal post_commit
        post_commit = True

    def reject_post_commit_sql(
        _connection, _cursor, _statement, _parameters, _context, _executemany
    ):
        if post_commit:
            raise AssertionError(
                "create_analysis performed database I/O after durable commit"
            )

    event.listen(db, "after_commit", mark_target_commit)
    event.listen(engine, "before_cursor_execute", reject_post_commit_sql)

    try:
        create_analysis(
            db=db,
            user_id=user.id,
            filename="canonical.log",
            saved_file_path=str(canonical_path),
            artifacts=[
                {
                    "original_filename": "canonical.log",
                    "saved_file_path": str(canonical_path),
                    "size_bytes": canonical_path.stat().st_size,
                    "detected_format": "generic",
                    "content_sha256": "same-digest",
                },
                {
                    "original_filename": "duplicate.log",
                    "saved_file_path": str(duplicate_path),
                    "size_bytes": duplicate_path.stat().st_size,
                    "detected_format": "generic",
                    "content_sha256": "same-digest",
                },
            ],
        )
    finally:
        event.remove(engine, "before_cursor_execute", reject_post_commit_sql)
        event.remove(db, "after_commit", mark_target_commit)

    assert canonical_path.exists()
    assert not duplicate_path.exists()

    verify_db = session_factory()
    try:
        rows = (
            verify_db.query(AnalysisArtifact)
            .order_by(AnalysisArtifact.position)
            .all()
        )
        assert len(rows) == 2
        assert rows[0].status == "pending"
        assert rows[0].duplicate_of_artifact_id is None
        assert rows[1].status == "duplicate"
        assert rows[1].duplicate_of_artifact_id == rows[0].id
    finally:
        verify_db.close()
        db.close()
