import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from sqlalchemy.orm import Session
from app.models.analysis import Analysis
from app.models.analysis_artifact import AnalysisArtifact
from app.models.user import User
from app.core.processing_config import MAX_ACTIVE_ANALYSES_PER_USER

logger = logging.getLogger(__name__)

_UPLOAD_ROOT = Path("uploads").resolve()

_ACTIVE_ANALYSIS_STATUSES = ("pending", "processing")


class ActiveAnalysisLimitReached(Exception):
    def __init__(self, limit: int = MAX_ACTIVE_ANALYSES_PER_USER) -> None:
        self.limit = limit
        super().__init__(
            f"You already have {limit} active investigations. "
            "Wait for one to finish or cancel one before starting another."
        )


def _active_analysis_query(db: Session, user_id: int):
    return (
        db.query(Analysis.id)
        .filter(
            Analysis.user_id == user_id,
            Analysis.status.in_(_ACTIVE_ANALYSIS_STATUSES),
        )
        .order_by(Analysis.id)
    )


def user_has_analysis_capacity(db: Session, user_id: int) -> bool:
    """Cheap upload preflight only. The authoritative race-safe check
    happens inside create_analysis()."""
    active_rows = (
        _active_analysis_query(db, user_id)
        .limit(MAX_ACTIVE_ANALYSES_PER_USER)
        .all()
    )
    return len(active_rows) < MAX_ACTIVE_ANALYSES_PER_USER


def _ensure_user_analysis_capacity(db: Session, user_id: int) -> None:
    # User is the per-account serialization row. No Analysis lifecycle path
    # takes User after taking Analysis, so this does not introduce the lock
    # inversion we spent half our natural lifespan removing earlier.
    locked_user_id = (
        db.query(User.id).filter(User.id == user_id).with_for_update().scalar()
    )
    if locked_user_id is None:
        raise ValueError("User does not exist")

    # This MUST be a locking/current read, not COUNT(*) from an earlier
    # request snapshot.
    #
    # Authentication has already read User through this Session. Under
    # MySQL's default transaction isolation a normal later SELECT may
    # therefore use that older consistent-read snapshot.
    #
    # FOR UPDATE is a current read. Combined with the User-row lock above,
    # concurrent Analysis creations for this SAME account serialize here.
    active_rows = (
        _active_analysis_query(db, user_id)
        .with_for_update()
        .limit(MAX_ACTIVE_ANALYSES_PER_USER)
        .all()
    )
    if len(active_rows) >= MAX_ACTIVE_ANALYSES_PER_USER:
        raise ActiveAnalysisLimitReached()


def create_analysis(
    db: Session,
    user_id: int,
    filename: str,
    saved_file_path: str,
    artifacts: Sequence[Mapping[str, Any]] | None = None,
    source_kind: str | None = None,
    source_reference: str | None = None,
    source_status: str | None = None,
    source_failure_reason: str | None = None,
) -> Analysis:
    try:
        _ensure_user_analysis_capacity(db, user_id)

        analysis = Analysis(
            user_id=user_id,
            original_filename=filename,
            saved_file_path=saved_file_path,
            source_kind=source_kind,
            source_reference=source_reference,
            source_status=source_status,
            source_failure_reason=source_failure_reason,
        )
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
           
            db.flush()

            for duplicate, canonical in duplicates:
                duplicate.status = "duplicate"
                duplicate.duplicate_of_artifact_id = canonical.id
                
                duplicate.processed_bytes = duplicate.size_bytes

        for model in artifact_models:
            if model.status == "unsupported":
                model.processed_bytes = model.size_bytes

        # Capture filesystem cleanup paths before commit. SQLAlchemy expires
        # ORM state on commit by default, so reading duplicate.saved_file_path
        # afterward could otherwise trigger an implicit database refresh in
        # the post-commit durability window.
        duplicate_staged_paths = [
            duplicate.saved_file_path for duplicate, _canonical in duplicates
        ]

        # Durability boundary: do not perform any database read after this
        # commit inside create_analysis(). If the commit succeeds but the DB
        # connection disappears immediately afterward, callers must treat the
        # Analysis/Artifact rows as durable and preserve staged inputs for
        # recovery instead of cleaning them up as though creation rolled back.
        db.commit()
    except Exception:
        db.rollback()
        raise

    for duplicate_staged_path in duplicate_staged_paths:
        _delete_staged_upload(duplicate_staged_path)

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
