import uuid
from pathlib import Path
from typing import Annotated
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session
from app.api.dependencies import get_current_verified_user
from app.core.processing_config import (
    MAX_INVESTIGATION_UPLOAD_BYTES,
    UPLOAD_COPY_CHUNK_BYTES,
)
from app.crud.analysis import create_analysis
from app.db.database import get_db
from app.models.user import User
from app.schemas.analysis import AnalysisResponse
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
):
    """Create one investigation from one or more repeated ``file`` parts."""
    uploads = file if isinstance(file, list) else [file]

    if not uploads:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one diagnostic artifact is required",
        )

    upload_group = uuid.uuid4().hex
    staged_paths: list[Path] = []
    artifact_rows: list[dict[str, object]] = []
    total_bytes = 0

    try:
        for position, upload in enumerate(uploads):
            original_filename = _safe_original_filename(
                upload.filename,
                position,
            )
            storage_filename = _safe_storage_filename(original_filename)
            saved_path = UPLOAD_DIR / (f"{upload_group}_{position}_{storage_filename}")
            artifact_size = 0

            with open(saved_path, "xb") as destination:
                staged_paths.append(saved_path)

                while True:
                    chunk = upload.file.read(UPLOAD_COPY_CHUNK_BYTES)
                    if not chunk:
                        break

                    if total_bytes + len(chunk) > MAX_INVESTIGATION_UPLOAD_BYTES:
                        raise HTTPException(
                            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                            detail="Combined diagnostic upload exceeds 1 GiB",
                        )

                    destination.write(chunk)
                    artifact_size += len(chunk)
                    total_bytes += len(chunk)

            artifact_rows.append(
                {
                    "original_filename": original_filename,
                    "saved_file_path": str(saved_path),
                    "content_type": upload.content_type,
                    "size_bytes": artifact_size,
                }
            )

        first_artifact = artifact_rows[0]
        analysis = create_analysis(
            db=db,
            user_id=current_user.id,
            filename=str(first_artifact["original_filename"]),
            saved_file_path=str(first_artifact["saved_file_path"]),
            artifacts=artifact_rows,
        )
    except Exception:
        _remove_staged_uploads(staged_paths)
        raise

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
