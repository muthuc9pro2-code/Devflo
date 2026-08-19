import uuid
from pathlib import Path
from typing import Annotated
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session
from app.api.dependencies import get_current_verified_user
from app.core.processing_config import (
    MAX_INVESTIGATION_UPLOAD_BYTES,
    MAX_SOURCE_ARCHIVE_BYTES,
    UPLOAD_COPY_CHUNK_BYTES,
)
from app.crud.analysis import create_analysis
from app.db.database import get_db
from app.models.user import User
from app.schemas.analysis import AnalysisResponse
from app.services.source_archive import (
    SourceInputError,
    validate_github_url,
    validate_source_zip,
)
from app.services.upload_staging import UploadTooLarge, copy_upload
from app.services.artifact_detector import (
    detect_artifact_sample,
    is_supported_diagnostic_sample
)
from app.services.analysis_events import publish_artifact_outcome
from app.services.investigation_context import build_artifact_outcome_payload
from app.tasks.analysis import process_analysis

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

router = APIRouter(prefix="/analysis", tags=["Analysis"])


@router.post(
    "/upload",
    response_model=AnalysisResponse,
)
def upload_file(
    file: Annotated[list[UploadFile], File()],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_verified_user)],
    github_url: Annotated[str | None, Form()] = None,
    source_zip: Annotated[UploadFile | None, File()] = None,
):
    """Create one investigation from one or more repeated ``file`` parts."""
    uploads = file if isinstance(file, list) else [file]

    if not uploads:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one diagnostic artifact is required",
        )

    github_url = (github_url or "").strip() or None
    if github_url and source_zip:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either github_url or source_zip, not both",
        )

    upload_group = uuid.uuid4().hex
    staged_paths: list[Path] = []
    artifact_rows: list[dict[str, object]] = []
    total_bytes = 0
    source_kind = source_reference = None

    try:
        if github_url:
            source_kind, source_reference = "github", validate_github_url(github_url)
        elif source_zip:
            source_path = UPLOAD_DIR / f"{upload_group}_source.zip"
            staged_paths.append(source_path)
            copy_upload(
                source_zip,
                source_path,
                MAX_SOURCE_ARCHIVE_BYTES,
                "Source ZIP exceeds the configured archive limit",
                UPLOAD_COPY_CHUNK_BYTES,
            )
            validate_source_zip(source_path)
            source_kind, source_reference = "zip", str(source_path)

        for position, upload in enumerate(uploads):
            original_filename = _safe_original_filename(
                upload.filename,
                position,
            )
            storage_filename = _safe_storage_filename(original_filename)
            saved_path = UPLOAD_DIR / (f"{upload_group}_{position}_{storage_filename}")
            staged_paths.append(saved_path)
            artifact_size, sample, content_sha256 = copy_upload(
                upload,
                saved_path,
                MAX_INVESTIGATION_UPLOAD_BYTES - total_bytes,
                "Combined diagnostic upload exceeds 1 GiB",
                UPLOAD_COPY_CHUNK_BYTES,
            )
            total_bytes += artifact_size

            if not is_supported_diagnostic_sample(
                sample,
                filename=original_filename,
                mime_type=upload.content_type,
            ):
                # Unsupported artifacts are recorded (not silently dropped
                # and not aborting a batch that has other, valid artifacts)
                # so the frontend can report which file/why via the same
                # investigation_result.artifacts[] contract as every other
                # outcome - never dispatched for ingestion, so the bytes
                # are removed immediately like before.
                saved_path.unlink(missing_ok=True)

                artifact_rows.append(
                    {
                        "original_filename": original_filename,
                        "saved_file_path": str(saved_path),
                        "content_type": upload.content_type,
                        "size_bytes": artifact_size,
                        "detected_format": None,
                        "content_sha256": content_sha256,
                        "status": "unsupported",
                    }
                )
                continue

            artifact_format = detect_artifact_sample(
                sample,
                filename=original_filename,
                mime_type=upload.content_type,
            )

            artifact_rows.append(
                {
                    "original_filename": original_filename,
                    "saved_file_path": str(saved_path),
                    "content_type": upload.content_type,
                    "size_bytes": artifact_size,
                    "detected_format": artifact_format.value,
                    "content_sha256": content_sha256,
                }
            )

        if not any(row["detected_format"] is not None for row in artifact_rows):
            # Every uploaded file was unsupported - nothing to analyze at
            # all, same outcome as today's single-unsupported-file case.
            _remove_staged_uploads(staged_paths)
            unsupported_names = ", ".join(
                row["original_filename"] for row in artifact_rows
            )
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=f"Unsupported diagnostic artifact: {unsupported_names}",
            )

        first_artifact = next(
            row for row in artifact_rows if row["detected_format"] is not None
        )
        analysis = create_analysis(
            db=db,
            user_id=current_user.id,
            filename=str(first_artifact["original_filename"]),
            saved_file_path=str(first_artifact["saved_file_path"]),
            artifacts=artifact_rows,
            source_kind=source_kind,
            source_reference=source_reference,
        )
    except UploadTooLarge as error:
        _remove_staged_uploads(staged_paths)
        raise HTTPException(status_code=413, detail=str(error)) from error
    except SourceInputError as error:
        _remove_staged_uploads(staged_paths)
        raise HTTPException(status_code=400, detail=str(error)) from error
    except Exception:
        _remove_staged_uploads(staged_paths)
        raise

    # unsupported/duplicate are already deterministically resolved at this
    # point (staging classified unsupported; create_analysis's within-
    # analysis content-hash grouping established the canonical artifact for
    # any duplicate) - publish those outcomes now rather than making the
    # frontend wait for final correlation. Reuses the exact same
    # build_artifact_outcome_payload() the final investigation_result.
    # artifacts[] uses, so there is only ever one status/message
    # representation. Evidence-bearing artifacts are NOT published here -
    # only once their ingestion actually completes (_process_artifact).
    created_artifacts = list(analysis.artifacts)
    filename_by_artifact_id = {
        artifact.id: artifact.original_filename for artifact in created_artifacts
    }
    for artifact in created_artifacts:
        if artifact.status in ("unsupported", "duplicate"):
            publish_artifact_outcome(
                analysis.id,
                {
                    "analysis_id": analysis.id,
                    **build_artifact_outcome_payload(
                        artifact, 0, filename_by_artifact_id
                    ),
                },
            )

    process_analysis.delay(analysis.id)
    return analysis


def _safe_original_filename(filename: str | None, position: int) -> str:
    candidate = Path((filename or f"artifact-{position}.txt").replace("\\", "/")).name
    candidate = candidate.replace("\x00", "").strip()
    return (candidate or f"artifact-{position}.txt")[-255:]


def _safe_storage_filename(filename: str) -> str:
    return (
        filename.encode("utf-8")[-180:].decode("utf-8", errors="ignore") or "artifact"
    )


def _remove_staged_uploads(paths: list[Path]) -> None:
    upload_root = UPLOAD_DIR.resolve()

    for path in paths:
        resolved = path.resolve(strict=False)
        if resolved.parent != upload_root:
            continue
        path.unlink(missing_ok=True)
