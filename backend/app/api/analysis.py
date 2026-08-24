import base64
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated
from fastapi import APIRouter, Depends, File, Form, HTTPException, Response, UploadFile, status
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session
from app.api.dependencies import get_current_verified_user
from app.core.processing_config import (
    MAX_DIAGNOSTIC_ARTIFACTS,
    MAX_INVESTIGATION_UPLOAD_BYTES,
    MAX_OCR_IMAGE_BYTES,
    MAX_OCR_IMAGES_PER_INVESTIGATION,
    MAX_SOURCE_ARCHIVE_BYTES,
    MEBIBYTE,
    UPLOAD_COPY_CHUNK_BYTES,
)
from app.crud.analysis import create_analysis
from app.db.database import get_db
from app.models.analysis import Analysis
from app.models.analysis_artifact import AnalysisArtifact
from app.models.user import User
from app.schemas.analysis import AnalysisHistoryItem, AnalysisHistoryPage, AnalysisResponse
from app.services.source_archive import (
    SourceInputError,
    validate_github_url,
    validate_source_zip,
)
from app.services.upload_staging import UploadTooLarge, copy_upload
from app.services.artifact_detector import (
    ArtifactFormat,
    SUPPORTED_IMAGE_MIME_TYPES,
    SUPPORTED_IMAGE_SUFFIXES,
    detect_artifact_sample,
    is_supported_diagnostic_sample
)
from app.services.analysis_events import publish_analysis_event, publish_artifact_outcome
from app.services.image_text_extractor import (
    InvalidOcrImageError,
    OcrImageTooLargeError,
    validate_ocr_image,
)
from app.services.investigation_context import build_artifact_outcome_payload
from app.tasks.analysis import (
    cancel_analysis_and_cleanup,
    compute_current_analysis_state,
    process_analysis,
)

HISTORY_DEFAULT_LIMIT = 20
HISTORY_MAX_LIMIT = 100

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

    if len(uploads) > MAX_DIAGNOSTIC_ARTIFACTS:
        raise HTTPException(
            status_code=413,
            detail=(
                f"At most {MAX_DIAGNOSTIC_ARTIFACTS} diagnostic artifacts "
                "are supported per investigation"
            ),
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
    image_count = 0
    source_kind = source_reference = None
    source_status = source_failure_reason = None

    try:
        if github_url:
            source_kind = "github"
            try:
                source_reference = validate_github_url(github_url)
            except SourceInputError:
                # Optional source enrichment: a malformed repository URL
                # detected synchronously here must degrade the exact same
                # way a later async acquisition failure already does (see
                # _prepare_source_task) - never abort otherwise-valid
                # diagnostic artifacts over it. source_kind stays "github"
                # so History/the final result can still say what kind of
                # source was attempted. Deliberately distinct wording from
                # _prepare_source_task's "could not be accessed or
                # prepared": this is a malformed URL Devflo can reliably
                # detect without ever attempting to reach GitHub - a later
                # genuine access/clone failure (private repo, network,
                # nonexistent repo, etc.) is a different, less certain
                # class of failure and must not be conflated with this one.
                source_status = "unavailable"
                source_failure_reason = "Invalid GitHub repository URL."
        elif source_zip:
            source_kind = "zip"
            source_path = UPLOAD_DIR / f"{upload_group}_source.zip"
            staged_paths.append(source_path)
            try:
                copy_upload(
                    source_zip,
                    source_path,
                    MAX_SOURCE_ARCHIVE_BYTES,
                    "Source ZIP exceeds the configured archive limit",
                    UPLOAD_COPY_CHUNK_BYTES,
                )
                validate_source_zip(source_path)
                source_reference = str(source_path)
            except (UploadTooLarge, SourceInputError) as error:
                # Same degradation as the github_url branch above. The
                # invalid/oversized staged ZIP is reclaimed immediately -
                # it will never be used, and the diagnostic artifacts
                # staged below are entirely unaffected by it.
                source_status = "unavailable"
                source_failure_reason = (
                    f"Uploaded source ZIP could not be prepared: {error}"
                )[:500]
                source_path.unlink(missing_ok=True)

        for position, upload in enumerate(uploads):
            original_filename = _safe_original_filename(
                upload.filename,
                position,
            )
            storage_filename = _safe_storage_filename(original_filename)
            saved_path = UPLOAD_DIR / (f"{upload_group}_{position}_{storage_filename}")
            staged_paths.append(saved_path)

            # MIME/extension is only an early resource hint here - a normal
            # 50 MiB PNG should not first be fully written to disk before
            # being rejected. The artifact detector below remains the
            # authoritative format decision, and a disguised image that
            # slips past this hint is still caught by the real
            # size/pixel validation (validate_ocr_image) once
            # detect_artifact_sample() confirms IMAGE.
            looks_like_image = (
                Path(original_filename).suffix.lower() in SUPPORTED_IMAGE_SUFFIXES
                or (upload.content_type or "").split(";", 1)[0].strip().lower()
                in SUPPORTED_IMAGE_MIME_TYPES
            )
            remaining_combined_budget = MAX_INVESTIGATION_UPLOAD_BYTES - total_bytes
            if looks_like_image and MAX_OCR_IMAGE_BYTES < remaining_combined_budget:
                copy_max_bytes = MAX_OCR_IMAGE_BYTES
                copy_detail = f"Image exceeds the {MAX_OCR_IMAGE_BYTES // MEBIBYTE} MiB per-image limit"
            else:
                copy_max_bytes = remaining_combined_budget
                copy_detail = "Combined diagnostic upload exceeds 1 GiB"

            artifact_size, sample, content_sha256 = copy_upload(
                upload,
                saved_path,
                copy_max_bytes,
                copy_detail,
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

            if artifact_format == ArtifactFormat.IMAGE:
                image_count += 1
                if image_count > MAX_OCR_IMAGES_PER_INVESTIGATION:
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            f"At most {MAX_OCR_IMAGES_PER_INVESTIGATION} images "
                            "are supported per investigation"
                        ),
                    )

                # Real size/pixel validation against the file actually
                # staged on disk - the same shared validator every OCR call
                # runs (image_text_extractor._run_ocr), so even a disguised
                # image that escaped the early filename/MIME hint above
                # cannot reach OCR unvalidated. A resource-invalid image is
                # an invalid investigation input, not zero-evidence
                # "unsupported" evidence - the whole request is rejected.
                try:
                    validate_ocr_image(saved_path)
                except OcrImageTooLargeError as error:
                    raise HTTPException(status_code=413, detail=str(error)) from error
                except InvalidOcrImageError as error:
                    raise HTTPException(status_code=415, detail=str(error)) from error

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
            source_status=source_status,
            source_failure_reason=source_failure_reason,
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


@router.get("/history", response_model=AnalysisHistoryPage)
def get_analysis_history(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_verified_user)],
    response: Response,
    limit: int = HISTORY_DEFAULT_LIMIT,
    cursor: str | None = None,
) -> AnalysisHistoryPage:
    """Bounded, keyset-paginated (created_at, id desc) list of only the
    current user's own analyses. Deliberately cheap and O(page size): no
    Evidence query, no correlation graph, no result_snapshot
    deserialization - only the small ai_analysis column (for title/
    summary) and one grouped artifact-count query, both scoped to exactly
    the page being returned, never the user's whole history.

    Cancelled analyses are excluded at the query level - not a general
    History-deletion feature, just the terminal-state equivalent of an
    upload that never really happened: its generated data was already
    discarded by cancel_analysis_and_cleanup(). pending/processing/
    completed/failed are all returned exactly as before.
    """
    response.headers["Cache-Control"] = "no-store"
    limit = max(1, min(limit, HISTORY_MAX_LIMIT))

    query = db.query(
        Analysis.id,
        Analysis.status,
        Analysis.created_at,
        Analysis.updated_at,
        Analysis.original_filename,
        Analysis.ai_analysis,
    ).filter(
        Analysis.user_id == current_user.id,
        Analysis.status != "cancelled",
    )

    if cursor:
        cursor_created_at, cursor_id = _decode_history_cursor(cursor)
        query = query.filter(
            or_(
                Analysis.created_at < cursor_created_at,
                and_(
                    Analysis.created_at == cursor_created_at,
                    Analysis.id < cursor_id,
                ),
            )
        )

    rows = (
        query.order_by(Analysis.created_at.desc(), Analysis.id.desc())
        .limit(limit + 1)
        .all()
    )

    has_more = len(rows) > limit
    rows = rows[:limit]

    analysis_ids = [row.id for row in rows]
    artifact_counts = (
        dict(
            db.query(AnalysisArtifact.analysis_id, func.count(AnalysisArtifact.id))
            .filter(AnalysisArtifact.analysis_id.in_(analysis_ids))
            .group_by(AnalysisArtifact.analysis_id)
            .all()
        )
        if analysis_ids
        else {}
    )

    items = [
        AnalysisHistoryItem(
            analysis_id=row.id,
            status=row.status,
            created_at=row.created_at,
            updated_at=row.updated_at,
            artifact_count=artifact_counts.get(row.id, 0),
            original_filename=row.original_filename,
            title=(row.ai_analysis or {}).get("title"),
            summary=(row.ai_analysis or {}).get("summary"),
        )
        for row in rows
    ]

    next_cursor = (
        _encode_history_cursor(rows[-1].created_at, rows[-1].id)
        if has_more and rows
        else None
    )

    return AnalysisHistoryPage(items=items, next_cursor=next_cursor)


@router.get("/{analysis_id}")
def get_analysis_detail(
    analysis_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_verified_user)],
) -> Response:
    """The durable REST counterpart to the live SSE stream - the exact
    same current-state contract (compute_current_analysis_state) a History
    click can use without ever opening an SSE connection: a processing
    analysis returns persisted byte progress, a completed one returns the
    persisted result_snapshot (or the legacy recompute fallback for a
    pre-migration row), a failed one just reports its status. No Gemini
    call and no correlation rerun either way when a snapshot exists.

    Ownership is enforced by filtering on BOTH analysis_id and
    current_user.id in one query - a mismatched owner and a nonexistent id
    are indistinguishable (both 404), so this endpoint never confirms
    whether another user's analysis id exists.

    Returned as a raw Response (json.dumps(..., default=str), not a
    pydantic response_model) so this matches the SSE stream's own
    serialization byte-for-byte - the same datetime/etc. coercion, not
    just the same field names.
    """
    analysis = (
        db.query(Analysis)
        .filter(Analysis.id == analysis_id, Analysis.user_id == current_user.id)
        .first()
    )
    if analysis is None:
        raise HTTPException(status_code=404, detail="Analysis not found")

    state = compute_current_analysis_state(db, analysis)
    return Response(
        content=json.dumps(state, separators=(",", ":"), default=str),
        media_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


@router.post("/{analysis_id}/cancel")
def cancel_analysis(
    analysis_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_verified_user)],
) -> dict:
    """Explicit user cancellation - the ONLY user-triggered way an analysis
    ever becomes "cancelled" (a closed browser tab/lost connection is
    never cancellation; see AnalysisPage's durable reconnect design).

    Runs entirely synchronously in this request - never depends on
    obtaining a free Celery worker slot, and is never itself dispatched as
    a Celery task: cancel_analysis_and_cleanup() commits the durable
    "cancelled" tombstone before doing any cleanup, so even if every
    worker is currently busy, this endpoint still durably wins.

    Ownership enforced the same way as get_analysis_detail (both
    analysis_id and current_user.id in one query - a mismatched owner and
    a nonexistent id are indistinguishable, both 404).
    """
    analysis = (
        db.query(Analysis)
        .filter(Analysis.id == analysis_id, Analysis.user_id == current_user.id)
        .first()
    )
    if analysis is None:
        raise HTTPException(status_code=404, detail="Analysis not found")

    if analysis.status == "cancelled":
        return {"analysis_id": analysis.id, "status": "cancelled"}  # idempotent

    if analysis.status == "completed":
        raise HTTPException(
            status_code=409, detail="Completed analyses cannot be cancelled"
        )

    if analysis.status == "failed":
        raise HTTPException(
            status_code=409, detail="Failed analyses cannot be cancelled"
        )

    # analysis.status in ("pending", "processing") at the read above -
    # cancellable as far as this request knows. cancel_analysis_and_cleanup
    # re-checks under its own transaction and may still lose the race (a
    # finalizer's completed/failed commit can land between the read above
    # and here) - it returns None in that case rather than cancelling
    # anything, so the response below must reflect what it actually
    # observed, not the stale read above.
    previous_status = cancel_analysis_and_cleanup(db, analysis_id)

    if previous_status is None:
        current_status = (
            db.query(Analysis.status).filter(Analysis.id == analysis_id).scalar()
        )
        if current_status == "cancelled":
            return {"analysis_id": analysis_id, "status": "cancelled"}  # idempotent
        if current_status == "completed":
            raise HTTPException(
                status_code=409, detail="Completed analyses cannot be cancelled"
            )
        if current_status == "failed":
            raise HTTPException(
                status_code=409, detail="Failed analyses cannot be cancelled"
            )
        # Analysis vanished or is in an unexpected state - ownership was
        # already confirmed above, so this only means the row is gone.
        raise HTTPException(status_code=404, detail="Analysis not found")

    # Best-effort live notification only (Part K) - DB is already
    # authoritative regardless of whether this is ever delivered; a lost
    # event is fully reconstructed by the next durable GET/reconnect via
    # compute_current_analysis_state's "cancelled" branch.
    publish_analysis_event(analysis_id, "cancelled", {"analysis_id": analysis_id})

    return {"analysis_id": analysis_id, "status": "cancelled"}


def _encode_history_cursor(created_at: datetime, analysis_id: int) -> str:
    raw = f"{created_at.isoformat()}|{analysis_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_history_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        created_at_raw, analysis_id_raw = raw.rsplit("|", 1)
        return datetime.fromisoformat(created_at_raw), int(analysis_id_raw)
    except Exception as error:
        raise HTTPException(status_code=400, detail="Invalid history cursor") from error


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
