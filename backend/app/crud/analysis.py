import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from sqlalchemy.orm import Session
from app.models.analysis import Analysis
from app.models.analysis_artifact import AnalysisArtifact

logger = logging.getLogger(__name__)

_UPLOAD_ROOT = Path("uploads").resolve()

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

        artifact_models = [
            AnalysisArtifact(
                analysis_id=analysis.id,
                position=position,
                original_filename=str(item["original_filename"]),
                saved_file_path=str(item["saved_file_path"]),
                content_type=item.get("content_type"),
                size_bytes=int(item.get("size_bytes", 0)),
                detected_format=item.get("detected_format"),
                content_sha256=item.get("content_sha256"),
                status=item.get("status", "pending"),
            )
            for position, item in enumerate(artifact_rows)
        ]
        db.add_all(artifact_models)

        # Content-identity duplicate detection: within THIS analysis only,
        # the first artifact (upload order) carrying a given content_sha256
        # stays canonical/pending; any later one with the exact same digest
        # becomes a duplicate of it instead of an independent artifact.
        # Unsupported rows carry no meaningful content_sha256 relationship
        # here (they are never processed either way) and are excluded.
        canonical_by_digest: dict[str, AnalysisArtifact] = {}
        duplicates: list[tuple[AnalysisArtifact, AnalysisArtifact]] = []

        for model in artifact_models:
            if model.content_sha256 is None or model.status == "unsupported":
                continue

            canonical = canonical_by_digest.get(model.content_sha256)

            if canonical is None:
                canonical_by_digest[model.content_sha256] = model
            else:
                duplicates.append((model, canonical))

        if duplicates:
            # Need real, DB-assigned artifact ids before duplicate rows can
            # reference their canonical sibling - one flush, still inside
            # the same transaction as the eventual commit below.
            db.flush()

            for duplicate, canonical in duplicates:
                duplicate.status = "duplicate"
                duplicate.duplicate_of_artifact_id = canonical.id
                # Never dispatched for ingestion, so there are no bytes
                # actually pending for it - without this it would sit
                # forever in the ingestion-progress denominator
                # (_publish_ingestion_progress sums size_bytes across every
                # artifact) without ever contributing to the numerator,
                # permanently understating progress. Existing progress
                # math/query itself is untouched.
                duplicate.processed_bytes = duplicate.size_bytes

        for model in artifact_models:
            if model.status == "unsupported":
                model.processed_bytes = model.size_bytes

        db.commit()
        db.refresh(analysis)
    except Exception:
        db.rollback()
        raise

    # Best-effort, post-commit only: the DB has already durably established
    # canonical vs duplicate (including duplicate_of_artifact_id), so it is
    # now safe to reclaim the duplicate's redundant staged bytes. Metadata
    # (original_filename, duplicate_of_artifact_id, status) stays in the DB
    # either way - only the physical file is removed, and only the
    # duplicate's, never the canonical artifact's. A deletion failure is
    # logged and otherwise ignored: the investigation is already durably
    # correct without it, and redundant bytes left on disk are not worth
    # failing the upload over.
    for duplicate, _canonical in duplicates:
        _delete_staged_upload(duplicate.saved_file_path)

    return analysis


def _delete_staged_upload(saved_file_path: str) -> None:
    path = Path(saved_file_path)
    resolved = path.resolve(strict=False)

    if resolved.parent != _UPLOAD_ROOT:
        logger.warning(
            "Refusing to delete staged upload outside the upload root: %s",
            saved_file_path,
        )
        return

    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.exception(
            "Failed to delete duplicate artifact's staged file: %s",
            saved_file_path,
        )
