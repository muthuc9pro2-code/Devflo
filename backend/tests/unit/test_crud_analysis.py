from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
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
