import logging
from os.path import getsize
from pathlib import Path
from time import perf_counter

from sqlalchemy.orm import Session

from app.core.celery_app import celery_app
from app.db.database import sessionLocal
from app.models import Analysis, AnalysisArtifact
from app.services import (
    build_exception_fingerprint,
    create_batches,
    filter_important_events,
    persist_evidence_batch,
    persist_resolved_identities,
    process_persisted_timelines,
)
from app.services.artifact_detector import ArtifactFormat, detect_artifact
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.source_archive import prepare_source
from app.services.source_index import correlate_event

logger = logging.getLogger(__name__)


@celery_app.task
def process_analysis(analysis_id: int):
    # Checkpoints are committed once per batch. Keeping loaded state across those
    # commits avoids refreshing the analysis and artifact rows after every batch.
    db = sessionLocal(expire_on_commit=False)

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            logger.warning("Analysis %s not found", analysis_id)
            return

        if analysis.status == "completed":
            logger.info("Analysis %s is already completed", analysis_id)
            return

        total_start = perf_counter()
        analysis.status = "processing"
        db.commit()

        parsed_event_count = 0
        ingestion_start = perf_counter()
        artifacts = (
            db.query(AnalysisArtifact)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .order_by(AnalysisArtifact.position, AnalysisArtifact.id)
            .all()
        )

        if not artifacts:
            raise RuntimeError(
                f"Analysis {analysis_id} has no persisted diagnostic artifacts"
            )

        source_index = _prepare_source_index(analysis)
        for artifact in artifacts:
            if artifact.status == "completed":
                continue

            parsed_event_count += _process_artifact(
                db=db,
                analysis=analysis,
                artifact=artifact,
                source_index=source_index,
            )

        logger.info(
            "Analysis %s parsed | total_lines=%s | parsed_events=%s | artifacts=%s",
            analysis_id,
            analysis.last_processed_line,
            parsed_event_count,
            len(artifacts) or 1,
        )
        logger.info(
            "Analysis %s | ingestion completed in %.2fs",
            analysis_id,
            perf_counter() - ingestion_start,
        )

        identity_start = perf_counter()
        persist_resolved_identities(db=db, analysis_id=analysis_id)
        logger.info("Analysis %s | evidence identities resolved", analysis_id)
        logger.info(
            "Analysis %s | identity resolution completed in %.2fs",
            analysis_id,
            perf_counter() - identity_start,
        )

        timeline_start = perf_counter()
        process_persisted_timelines(db=db, analysis_id=analysis_id)
        logger.info("Analysis %s | persisted timelines processed", analysis_id)
        logger.info(
            "Analysis %s | timeline processing completed in %.2fs",
            analysis_id,
            perf_counter() - timeline_start,
        )

        analysis.status = "completed"
        db.commit()
        logger.info(
            "Analysis %s | TOTAL processing time %.2fs",
            analysis_id,
            perf_counter() - total_start,
        )
    except Exception:
        db.rollback()
        logger.exception("Analysis %s processing failed", analysis_id)
        _mark_analysis_failed(db, analysis_id)
        raise
    finally:
        db.close()


def _process_artifact(
    *,
    db: Session,
    analysis: Analysis,
    artifact: AnalysisArtifact,
    source_index=None,
) -> int:
    artifact_start = perf_counter()
    initial_size = getsize(artifact.saved_file_path)
    is_migrated_artifact = (
        artifact.status == "pending"
        and artifact.size_bytes == 0
        and artifact.detected_format in {None, ArtifactFormat.GENERIC.value}
        and initial_size >= artifact.processed_bytes
    )
    is_migrated_checkpoint = (
        is_migrated_artifact
        and artifact.detected_format == ArtifactFormat.GENERIC.value
        and artifact.processed_bytes > 0
    )

    if initial_size != artifact.size_bytes:
        if not is_migrated_artifact:
            raise RuntimeError(
                f"Artifact {artifact.id} changed after upload; refusing unsafe resume"
            )
        artifact.size_bytes = initial_size

    # Legacy checkpoints were byte/physical-line offsets produced by the generic
    # parser. Re-detecting one as structured JSON would reinterpret the stored
    # line count as a record count and make resume skip or replay evidence.
    artifact_format = (
        ArtifactFormat.GENERIC if is_migrated_checkpoint else _artifact_format(artifact)
    )
    artifact.detected_format = artifact_format.value
    artifact.status = "processing"
    db.commit()

    parsed_count = 0
    checkpoint_offset = artifact.processed_bytes

    records = stream_artifact_events(
        file_path=artifact.saved_file_path,
        artifact_format=artifact_format,
        source_file=artifact.original_filename,
        start_offset=artifact.processed_bytes,
        start_artifact_line=artifact.last_processed_line,
        global_line_number=analysis.last_processed_line,
    )

    for batch in create_batches(records):
        parsed_count += _persist_artifact_batch(
            db=db,
            analysis=analysis,
            artifact=artifact,
            batch=batch,
            source_index=source_index,
        )
        checkpoint_offset = artifact.processed_bytes

    actual_size = getsize(artifact.saved_file_path)
    if actual_size != artifact.size_bytes:
        raise RuntimeError(
            f"Artifact {artifact.id} changed during processing; checkpoint not completed"
        )

    remaining_bytes = max(artifact.size_bytes - checkpoint_offset, 0)
    artifact.processed_bytes = artifact.size_bytes
    artifact.status = "completed"
    analysis.processed_bytes += remaining_bytes
    db.commit()

    logger.info(
        "Analysis %s | artifact_position=%s | format=%s | events=%s | completed in %.2fs",
        analysis.id,
        artifact.position + 1,
        artifact_format.value,
        parsed_count,
        perf_counter() - artifact_start,
    )
    return parsed_count


def _persist_artifact_batch(
    *,
    db: Session,
    analysis: Analysis,
    artifact: AnalysisArtifact,
    batch,
    source_index=None,
) -> int:
    for record in batch:
        event = record.event
        event.artifact_id = artifact.id

    important_events = filter_important_events(record.event for record in batch)
    _correlate_source_events(important_events, source_index)
    _assign_batch_fingerprints(important_events)
    persist_evidence_batch(
        db=db,
        analysis_id=analysis.id,
        events=important_events,
        artifact_id=artifact.id,
    )

    last_record = batch[-1]
    previous_offset = artifact.processed_bytes
    new_offset = max(previous_offset, last_record.end_offset)
    artifact.processed_bytes = new_offset
    artifact.last_processed_line = last_record.artifact_line_number
    analysis.processed_bytes += new_offset - previous_offset
    analysis.last_processed_line = last_record.global_end_line_number
    db.commit()

    logger.debug(
        "Analysis %s | artifact=%s | processed batch | events=%s | important=%s",
        analysis.id,
        artifact.id,
        len(batch),
        len(important_events),
    )
    return len(batch)


def _correlate_source_events(events, source_index) -> None:
    if source_index is None:
        return

    for event in events:
        event.source_matches = correlate_event(event, source_index)


def _prepare_source_index(analysis: Analysis):
    if not analysis.source_kind or not analysis.source_reference:
        return None

    index = prepare_source(
        analysis.source_kind,
        analysis.source_reference,
        analysis.id,
    )
    if analysis.source_kind == "zip":
        _remove_staged_source_archive(analysis.source_reference)
    return index


def _remove_staged_source_archive(reference: str) -> None:
    path = Path(reference)
    if path.resolve(strict=False).parent == Path("uploads").resolve():
        path.unlink(missing_ok=True)


def _artifact_format(artifact: AnalysisArtifact) -> ArtifactFormat:
    if artifact.detected_format:
        try:
            return ArtifactFormat(artifact.detected_format)
        except ValueError:
            logger.warning(
                "Artifact %s has unknown stored format %s; detecting again",
                artifact.id,
                artifact.detected_format,
            )

    return detect_artifact(
        artifact.saved_file_path,
        filename=artifact.original_filename,
        mime_type=artifact.content_type,
    )


def _assign_batch_fingerprints(events) -> None:
    fingerprint_cache: dict[tuple[str | None, ...], str] = {}

    for event in events:
        cache_key: tuple[str | None, ...]
        if event.exception_type is None:
            cache_key = (None, event.level, event.raw_line)
        else:
            cache_key = (
                event.exception_type,
                event.exception_message,
            )

        fingerprint = fingerprint_cache.get(cache_key)
        if fingerprint is None:
            fingerprint = build_exception_fingerprint(event)
            fingerprint_cache[cache_key] = fingerprint
        event.fingerprint = fingerprint


def _mark_analysis_failed(db: Session, analysis_id: int) -> None:
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is not None:
            analysis.status = "failed"
            db.commit()
    except Exception:
        db.rollback()
        logger.exception("Could not mark analysis %s as failed", analysis_id)
