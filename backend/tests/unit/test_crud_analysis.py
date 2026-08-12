from unittest.mock import Mock

import pytest

from app.crud.analysis import create_analysis
from app.models import AnalysisArtifact


def test_create_analysis_bulk_adds_artifacts_in_one_transaction():
    db = Mock()

    def assign_id():
        db.add.call_args.args[0].id = 13

    db.flush.side_effect = assign_id
    analysis = create_analysis(
        db=db,
        user_id=2,
        filename="one.txt",
        saved_file_path="uploads/one.txt",
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
    assert all(isinstance(row, AnalysisArtifact) for row in rows)
    assert [row.position for row in rows] == [0, 1]
    db.commit.assert_called_once()


def test_create_analysis_rolls_back_on_artifact_failure():
    db = Mock()
    db.flush.side_effect = RuntimeError("database failed")

    with pytest.raises(RuntimeError):
        create_analysis(
            db=db,
            user_id=2,
            filename="one.txt",
            saved_file_path="uploads/one.txt",
        )

    db.rollback.assert_called_once()
