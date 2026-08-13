from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy.orm import Session

from app.models.analysis import Analysis
from app.models.analysis_artifact import AnalysisArtifact


def create_analysis(
    db: Session,
    user_id: int,
    filename: str,
    saved_file_path: str,
    artifacts: Sequence[Mapping[str, Any]] | None = None,
    source_kind: str | None = None,
    source_reference: str | None = None,
) -> Analysis:
    analysis = Analysis(
        user_id=user_id,
        original_filename=filename,
        saved_file_path=saved_file_path,
        source_kind=source_kind,
        source_reference=source_reference,
    )

    try:
        db.add(analysis)
        db.flush()

        artifact_rows = artifacts or (
            {
                "original_filename": filename,
                "saved_file_path": saved_file_path,
                "content_type": None,
                "size_bytes": 0,
            },
        )
        db.add_all(
            [
                AnalysisArtifact(
                    analysis_id=analysis.id,
                    position=position,
                    original_filename=str(item["original_filename"]),
                    saved_file_path=str(item["saved_file_path"]),
                    content_type=item.get("content_type"),
                    size_bytes=int(item.get("size_bytes", 0)),
                )
                for position, item in enumerate(artifact_rows)
            ]
        )
        db.commit()
        db.refresh(analysis)
    except Exception:
        db.rollback()
        raise

    return analysis
