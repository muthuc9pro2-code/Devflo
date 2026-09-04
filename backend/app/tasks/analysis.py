import logging
from datetime import datetime, timedelta, timezone
from os.path import getsize
from pathlib import Path
from time import perf_counter
from time import time as wall_time
from celery import chain, chord, group
from celery.exceptions import Retry
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session
from app.core.celery_app import celery_app
from app.core.processing_config import (
    CORRELATED_MAX_CONTEXT_BYTES,
    CORRELATED_MAX_EVIDENCE_RECORDS,
    SIMPLE_FALLBACK_MAX_ARTIFACT_BYTES,
    SIMPLE_FALLBACK_MAX_TEXT_BYTES,
    SOURCE_INDEX_PROCESS_CACHE_MAX_ENTRIES,
)
from app.db.database import sessionLocal
from app.models import Analysis, AnalysisArtifact, Evidence
from app.services import (
    build_exception_fingerprint,
    create_batches,
    is_evidence_worthy,
    persist_evidence_batch,
    persist_resolved_identities,
)
from app.services.artifact_detector import ArtifactFormat, detect_artifact
from app.services.diagnostic_adapters import (
    ArtifactInputError,
    stream_artifact_events,
    stream_image_events_from_text,
)
from app.services.fallback_context import (
    capture_ocr_fallback_context,
    capture_text_fallback_context,
)
from app.services.image_text_extractor import (
    OcrProcessingError,
    extract_text_from_image_with_confidence,
)
from app.services.source_archive import (
    SourceInputError,
    SourceSubsystemError,
    cleanup_generation_source_temp,
    cleanup_prepared_source,
    load_ready_source_index,
    prepare_source,
)
from app.services.source_index import SourceIndexLimitError, correlate_event
from app.services.investigation_router import (
    choose_investigation_path,
    InvestigationPath,
)
from app.services.correlation_engine import prepare_correlation, run_correlation
from app.services.investigation_context import (
    build_artifact_outcome_payload,
    build_correlation_payload,
    build_fallback_llm_context,
    build_fallback_payload,
    build_llm_context,
    build_simple_llm_context,
    build_simple_payload,
    build_source_outcome_payload,
    build_zero_evidence_payload,
    select_bounded_evidence_from_db,
    select_evidence_counts_by_artifact,
)
from app.services.analysis_events import (
    publish_artifact_outcome,
    publish_investigation_result,
    publish_progress,
)
from app.utils.bounded_json import OversizedJsonScalarError
from app.services.gemini_service import (
    GeminiUnavailableError,
    generate_investigation_explanation,
)

logger = logging.getLogger(__name__)

_GLOBAL_LINE_NUMBER_STRIDE = 10**9

def _ready_zero_match_source_context(
    source_outcome: dict | None,
) -> dict[str, int | str] | None:
    if (
        source_outcome is not None
        and source_outcome["status"] == "ready"
        and source_outcome["match_count"] == 0
    ):
        return {"status": "ready", "match_count": 0}
    return None

def _safe_rollback(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        logger.warning("db.rollback() failed after a prior exception", exc_info=True)

def _is_analysis_cancelled(db: Session, analysis_id: int) -> bool:
    return (
        db.query(Analysis.status).filter(Analysis.id == analysis_id).scalar()
        == "cancelled"
    )

def _finalizer_owns_generation(db: Session, analysis_id: int, generation: int) -> bool:
    current = (
        db.query(
            Analysis.status,
            Analysis.processing_generation,
            Analysis.finalization_generation,
        )
        .filter(Analysis.id == analysis_id)
        .with_for_update()
        .first()
    )
    db.commit()

    if current is None:
        return False
    status, processing_generation, finalization_generation = current
    return (
        status == "processing"
        and processing_generation == generation
        and finalization_generation == generation
    )

def _finalize_commit_if_processing(
    db: Session,
    analysis: Analysis,
    *,
    generation: int,
    result_snapshot: dict,
    ai_analysis: dict | None,
    processed_bytes: int,
    last_processed_line: int,
    stage: str,
) -> bool:
    current_status, current_processing_generation, current_finalization_generation = (
        db.query(
            Analysis.status,
            Analysis.processing_generation,
            Analysis.finalization_generation,
        )
        .filter(Analysis.id == analysis.id)
        .with_for_update()
        .first()
    )
    if (
        current_status != "processing"
        or current_processing_generation != generation
        or current_finalization_generation != generation
    ):
        db.rollback()
        logger.info(
            "Analysis %s | status=%s generation=%s/%s at final persistence "
            "(%s); discarding completed result",
            analysis.id,
            current_status,
            current_processing_generation,
            current_finalization_generation,
            stage,
        )
        return False

    analysis.result_snapshot = result_snapshot
    analysis.ai_analysis = ai_analysis
    analysis.processed_bytes = processed_bytes
    analysis.last_processed_line = last_processed_line
    analysis.status = "completed"
    db.commit()
    return True

_HEARTBEAT_MIN_INTERVAL_SECONDS = 60.0

_last_heartbeat_write: dict[int, float] = {}
_HEARTBEAT_THROTTLE_CACHE_MAX_ENTRIES = 64

def _bump_processing_heartbeat(analysis_id: int, generation: int) -> None:
    throttle_key = (analysis_id, generation)
    now = perf_counter()
    last = _last_heartbeat_write.get(throttle_key, 0.0)
    if now - last < _HEARTBEAT_MIN_INTERVAL_SECONDS:
        return

    if len(_last_heartbeat_write) >= _HEARTBEAT_THROTTLE_CACHE_MAX_ENTRIES:
        oldest_key = next(iter(_last_heartbeat_write))
        del _last_heartbeat_write[oldest_key]
    _last_heartbeat_write[throttle_key] = now

    heartbeat_db = sessionLocal()
    try:
        heartbeat_db.execute(
            update(Analysis)
            .where(
                Analysis.id == analysis_id,
                Analysis.status == "processing",
                Analysis.processing_generation == generation,
            )
            .values(processing_heartbeat_at=datetime.now(timezone.utc))
            .execution_options(synchronize_session=False)
        )
        heartbeat_db.commit()
    except Exception:
        _safe_rollback(heartbeat_db)
        logger.debug("Analysis %s | heartbeat write failed", analysis_id, exc_info=True)
    finally:
        heartbeat_db.close()

def _return_analysis_to_pending_after_publish_failure(
    analysis_id: int, generation: int
) -> bool:
    demote_db = sessionLocal(expire_on_commit=False)
    try:
        stale_filter = and_(
            Analysis.id == analysis_id,
            Analysis.status == "processing",
            Analysis.processing_generation == generation,
        )
        claimed = _claim_and_demote_stale_processing(
            demote_db, stale_filter, datetime.now(timezone.utc)
        )
        return analysis_id in claimed
    finally:
        demote_db.close()

@celery_app.task
def process_analysis(analysis_id: int):
    db = sessionLocal(expire_on_commit=False)
    finalize_only = False
    generation = None

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            logger.warning("Analysis %s not found", analysis_id)
            return

        now = datetime.now(timezone.utc)
        claim = db.execute(
            update(Analysis)
            .where(Analysis.id == analysis_id, Analysis.status == "pending")
            .values(
                status="processing",
                processing_generation=Analysis.processing_generation + 1,
                processing_heartbeat_at=now,
                finalization_generation=None,
            )
            .execution_options(synchronize_session=False)
        )
        db.commit()

        if claim.rowcount != 1:
            logger.info(
                "Analysis %s | not claimed (status=%s); not (re)dispatching",
                analysis_id,
                analysis.status,
            )
            return

        db.refresh(analysis)
        generation = analysis.processing_generation

        publish_progress(
            analysis_id,
            "ingestion",
            "Diagnostic ingestion started",
            progress=0,
        )

        all_artifacts = (
            db.query(AnalysisArtifact.id, AnalysisArtifact.status)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .order_by(AnalysisArtifact.position, AnalysisArtifact.id)
            .all()
        )
        if not all_artifacts:
            raise RuntimeError(
                f"Analysis {analysis_id} has no persisted diagnostic artifacts"
            )

        artifact_ids = [
            row.id
            for row in all_artifacts
            if row.status
            not in ("unsupported", "duplicate", "resource_limited", "processing_error")
        ]

        finalize_only = all(
            row.status
            in (
                "completed",
                "unsupported",
                "duplicate",
                "resource_limited",
                "processing_error",
            )
            for row in all_artifacts
        )
        needs_source_prep = bool(analysis.source_kind) and not finalize_only

        if _is_analysis_cancelled(db, analysis_id):
            logger.info(
                "Analysis %s | cancelled before dispatch; not dispatching",
                analysis_id,
            )
            return
    except Exception:
        _safe_rollback(db)
        logger.exception("Analysis %s processing failed", analysis_id)
        if _mark_analysis_failed(db, analysis_id, generation=generation):
            _cleanup_files_after_terminal_failure(db, analysis_id)
        raise
    finally:
        db.close()

    dispatch_start = wall_time()

    try:
        if finalize_only:
            logger.info(
                "Analysis %s | every artifact already terminal; finalizing directly",
                analysis_id,
            )
            _finalize_analysis_task.delay([], analysis_id, generation, dispatch_start)
            return

        artifact_group = group(
            _process_artifact_task.si(analysis_id, artifact_id, generation)
            for artifact_id in artifact_ids
        )
        workflow = chord(
            artifact_group,
            _finalize_analysis_task.s(analysis_id, generation, dispatch_start),
        )
        if needs_source_prep:
            workflow = chain(_prepare_source_task.si(analysis_id, generation), workflow)

        workflow.apply_async()
    except Exception:
        logger.exception(
            "Analysis %s | generation %s could not publish its workflow to "
            "the broker; returning to pending for recovery",
            analysis_id,
            generation,
        )
        if _return_analysis_to_pending_after_publish_failure(analysis_id, generation):
            try:
                process_analysis.delay(analysis_id)
            except Exception:
                logger.exception(
                    "Analysis %s | immediate republish attempt also failed; "
                    "will be picked up by stale-analysis recovery",
                    analysis_id,
                )
        return

    logger.info(
        "Analysis %s | dispatched %s artifact task(s)%s (worker_concurrency=%s)",
        analysis_id,
        len(artifact_ids),
        " after source prep" if needs_source_prep else "",
        celery_app.conf.worker_concurrency,
    )

@celery_app.task
def _process_artifact_task(analysis_id: int, artifact_id: int, generation: int) -> int:
    db = sessionLocal(expire_on_commit=False)
    try:
        current = (
            db.query(Analysis.status, Analysis.processing_generation)
            .filter(Analysis.id == analysis_id)
            .with_for_update()
            .first()
        )

        if current is None:
            db.rollback()
            logger.warning(
                "Analysis %s not found while claiming artifact %s; skipping",
                analysis_id,
                artifact_id,
            )
            return 0

        current_status, current_generation = current
        if current_status != "processing" or current_generation != generation:
            db.rollback()
            logger.info(
                "Analysis %s | generation %s no longer owns artifact %s "
                "(status=%s, current generation=%s); skipping",
                analysis_id,
                generation,
                artifact_id,
                current_status,
                current_generation,
            )
            return 0

        artifact_status = (
            db.query(AnalysisArtifact.status)
            .filter(
                AnalysisArtifact.id == artifact_id,
                AnalysisArtifact.analysis_id == analysis_id,
            )
            .with_for_update()
            .scalar()
        )

        if artifact_status != "pending":
            db.rollback()
            logger.info(
                "Analysis %s | artifact %s not claimable (status=%s); skipping",
                analysis_id,
                artifact_id,
                artifact_status,
            )
            return 0

        claim = db.execute(
            update(AnalysisArtifact)
            .where(
                AnalysisArtifact.id == artifact_id,
                AnalysisArtifact.analysis_id == analysis_id,
                AnalysisArtifact.status == "pending",
            )
            .values(status="processing")
            .execution_options(synchronize_session=False)
        )
        if claim.rowcount != 1:
            db.rollback()
            logger.info(
                "Analysis %s | artifact %s claim lost; skipping",
                analysis_id,
                artifact_id,
            )
            return 0
        db.commit()

        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        artifact = (
            db.query(AnalysisArtifact)
            .filter(
                AnalysisArtifact.id == artifact_id,
                AnalysisArtifact.analysis_id == analysis_id,
            )
            .first()
        )

        if analysis is None or artifact is None:
            db.rollback()
            logger.warning(
                "Analysis %s | artifact %s disappeared after claim; skipping",
                analysis_id,
                artifact_id,
            )
            return 0

        if (
            analysis.status != "processing"
            or analysis.processing_generation != generation
            or artifact.status != "processing"
        ):
            db.rollback()
            logger.info(
                "Analysis %s | generation %s lost ownership after claiming "
                "artifact %s; skipping before source/parsing",
                analysis_id,
                generation,
                artifact_id,
            )
            return 0

        _bump_processing_heartbeat(analysis_id, generation)

        source_index = _load_ready_source_index_for_artifact(analysis, generation)
        if (
            source_index is None
            and getattr(analysis, "source_kind", None)
            and getattr(analysis, "source_status", None) == "ready"
        ):
            _record_optional_source_failure(
                db,
                analysis,
                SourceSubsystemError(
                    "Published source tree is unexpectedly unavailable"
                ),
                generation=generation,
                remove_prepared_source=False,
            )

        global_line_number = (
            artifact.position * _GLOBAL_LINE_NUMBER_STRIDE
            + artifact.last_processed_line
        )

        return _process_artifact(
            db=db,
            analysis=analysis,
            artifact=artifact,
            generation=generation,
            source_index=source_index,
            global_line_number=global_line_number,
        )
    except OversizedJsonScalarError:
        db.rollback()
        logger.warning(
            "Analysis %s | artifact %s exceeded the supported JSON scalar limit",
            analysis_id,
            artifact_id,
        )
        _record_controlled_artifact_failure(
            db=db,
            analysis_id=analysis_id,
            artifact_id=artifact_id,
            generation=generation,
            status="resource_limited",
            reason="JSON value exceeded the supported 1 MiB per-value limit.",
        )
        return 0
    except OcrProcessingError:
        db.rollback()
        logger.warning(
            "Analysis %s | artifact %s OCR processing could not be completed",
            analysis_id,
            artifact_id,
            exc_info=True,
        )
        _record_controlled_artifact_failure(
            db=db,
            analysis_id=analysis_id,
            artifact_id=artifact_id,
            generation=generation,
            status="processing_error",
            reason="Image OCR could not be completed.",
        )
        return 0
    except ArtifactInputError:
        db.rollback()
        logger.warning(
            "Analysis %s | artifact %s could not be parsed safely",
            analysis_id,
            artifact_id,
            exc_info=True,
        )
        _record_controlled_artifact_failure(
            db=db,
            analysis_id=analysis_id,
            generation=generation,
            artifact_id=artifact_id,
            status="processing_error",
            reason="Diagnostic file could not be processed safely.",
        )
        return 0
    except Exception:
        _safe_rollback(db)
        logger.exception(
            "Analysis %s | artifact %s processing failed",
            analysis_id,
            artifact_id,
        )
        if _mark_analysis_failed(db, analysis_id, generation=generation):
            _cleanup_files_after_terminal_failure(db, analysis_id)
        raise
    finally:
        db.close()

_SOURCE_PREPARING_RETRY_DELAY_SECONDS = 10

def _publish_source_for_current_generation(
    analysis_id: int, generation: int, publisher
):
    publish_db = sessionLocal(expire_on_commit=False)

    try:
        current = (
            publish_db.query(
                Analysis.status,
                Analysis.processing_generation,
                Analysis.source_status,
            )
            .filter(Analysis.id == analysis_id)
            .with_for_update()
            .first()
        )

        if current is None:
            publish_db.rollback()
            return None

        status, current_generation, source_status = current

        if (
            status != "processing"
            or current_generation != generation
            or source_status != "preparing"
        ):
            publish_db.rollback()
            return None

        index = publisher()

        if index is None:
            publish_db.rollback()
            return None

        ready = publish_db.execute(
            update(Analysis)
            .where(
                Analysis.id == analysis_id,
                Analysis.status == "processing",
                Analysis.processing_generation == generation,
                Analysis.source_status == "preparing",
            )
            .values(
                source_status="ready",
                source_failure_reason=None,
            )
            .execution_options(synchronize_session=False)
        )

        if ready.rowcount != 1:
            publish_db.rollback()
            return None

        publish_db.commit()
        return index

    except Exception:
        _safe_rollback(publish_db)
        raise

    finally:
        publish_db.close()

def _remove_staged_zip_after_ready(analysis: Analysis) -> None:
    if analysis.source_kind != "zip" or not analysis.source_reference:
        return

    try:
        _remove_staged_source_archive(analysis.source_reference)
    except OSError:
        logger.warning(
            "Analysis %s | source is durably ready but the staged ZIP could "
            "not be removed",
            analysis.id,
            exc_info=True,
        )

@celery_app.task(bind=True, max_retries=None)
def _prepare_source_task(self, analysis_id: int, generation: int) -> None:
    db = sessionLocal(expire_on_commit=False)

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            logger.warning("Analysis %s not found for source prep", analysis_id)
            return

        if (
            analysis.status != "processing"
            or analysis.processing_generation != generation
        ):
            logger.info(
                "Analysis %s | generation %s superseded (status=%s, current "
                "generation=%s); skipping source preparation",
                analysis_id,
                generation,
                analysis.status,
                analysis.processing_generation,
            )
            return

        if analysis.source_status == "unavailable":
            logger.info(
                "Analysis %s | optional source already unavailable; "
                "skipping source preparation",
                analysis_id,
            )
            return

        if analysis.source_status == "ready":
            _remove_staged_zip_after_ready(analysis)

            logger.info(
                "Analysis %s | optional source already ready; nothing to prepare",
                analysis_id,
            )
            return

        claim = db.execute(
            update(Analysis)
            .where(
                Analysis.id == analysis_id,
                Analysis.status == "processing",
                Analysis.processing_generation == generation,
                Analysis.source_status.is_(None),
            )
            .values(source_status="preparing")
            .execution_options(synchronize_session=False)
        )
        db.commit()

        if claim.rowcount != 1:
            current = (
                db.query(
                    Analysis.status,
                    Analysis.processing_generation,
                    Analysis.source_status,
                )
                .filter(Analysis.id == analysis_id)
                .first()
            )

            current_status, current_generation, current_source_status = (
                current if current is not None else (None, None, None)
            )

            if (
                current_status != "processing"
                or current_generation != generation
            ):
                logger.info(
                    "Analysis %s | generation %s superseded (status=%s) "
                    "before source preparation could be claimed",
                    analysis_id,
                    generation,
                    current_status,
                )
                return

            if current_source_status == "ready":
                _remove_staged_zip_after_ready(analysis)

                logger.info(
                    "Analysis %s | source already ready; nothing to prepare",
                    analysis_id,
                )
                return

            if current_source_status == "unavailable":
                logger.info(
                    "Analysis %s | source already unavailable; nothing to prepare",
                    analysis_id,
                )
                return

            try:
                ready_index = load_ready_source_index(analysis_id)
            except Exception:
                logger.warning(
                    "Analysis %s | filesystem-ready source could not yet be "
                    "loaded while another preparation delivery owns generation %s; "
                    "retrying without failing the analysis",
                    analysis_id,
                    generation,
                    exc_info=True,
                )
                ready_index = None

            if ready_index is not None:
                adopted = db.execute(
                    update(Analysis)
                    .where(
                        Analysis.id == analysis_id,
                        Analysis.status == "processing",
                        Analysis.processing_generation == generation,
                        Analysis.source_status == "preparing",
                    )
                    .values(
                        source_status="ready",
                        source_failure_reason=None,
                    )
                    .execution_options(synchronize_session=False)
                )
                db.commit()

                if adopted.rowcount == 1:
                    _cache_source_index(
                        (analysis_id, generation),
                        ready_index,
                    )
                    _remove_staged_zip_after_ready(analysis)

                    logger.info(
                        "Analysis %s | adopted complete filesystem-ready source "
                        "for generation %s after interrupted preparation",
                        analysis_id,
                        generation,
                    )
                    return

                current = (
                    db.query(
                        Analysis.status,
                        Analysis.processing_generation,
                        Analysis.source_status,
                    )
                    .filter(Analysis.id == analysis_id)
                    .first()
                )

                if current is None:
                    return

                (
                    current_status,
                    current_generation,
                    current_source_status,
                ) = current

                if (
                    current_status != "processing"
                    or current_generation != generation
                ):
                    return

                if current_source_status == "ready":
                    _remove_staged_zip_after_ready(analysis)
                    return

                if current_source_status == "unavailable":
                    return

            logger.info(
                "Analysis %s | source preparation for generation %s is still "
                "owned; retrying in %ss (attempt %s)",
                analysis_id,
                generation,
                _SOURCE_PREPARING_RETRY_DELAY_SECONDS,
                self.request.retries + 1,
            )

            raise self.retry(
                countdown=_SOURCE_PREPARING_RETRY_DELAY_SECONDS
            )

        _bump_processing_heartbeat(analysis_id, generation)

        db.refresh(analysis)

        source_prep_start = perf_counter()

        try:
            index = _acquire_source_index(
                analysis,
                generation,
                publish_callback=lambda publisher: (
                    _publish_source_for_current_generation(
                        analysis_id,
                        generation,
                        publisher,
                    )
                ),
            )
        except (SourceInputError, SourceSubsystemError) as error:
            db.rollback()
            _record_optional_source_failure(
                db,
                analysis,
                error,
                generation=generation,
            )
            return

        if index is None:
            logger.info(
                "Analysis %s | generation %s lost source-publication ownership; "
                "discarding private prepared state",
                analysis_id,
                generation,
            )
            _source_index_process_cache.pop(
                (analysis_id, generation),
                None,
            )
            return

        _remove_staged_zip_after_ready(analysis)

        logger.info(
            "Analysis %s | source prep (%s) completed in %.2fs",
            analysis_id,
            analysis.source_kind,
            perf_counter() - source_prep_start,
        )

    except Retry:
        raise

    except Exception:
        _safe_rollback(db)

        logger.exception(
            "Analysis %s | source preparation failed",
            analysis_id,
        )

        if _mark_analysis_failed(
            db,
            analysis_id,
            generation=generation,
        ):
            _cleanup_files_after_terminal_failure(
                db,
                analysis_id,
            )

        raise

    finally:
        db.close()

def _cleanup_prepared_source_after_completion(
    analysis_id: int, source_kind: str | None
) -> None:
    if not source_kind:
        return
    _invalidate_source_index_cache(analysis_id)
    try:
        cleanup_prepared_source(analysis_id)
    except OSError:
        logger.warning(
            "Analysis %s | could not clean up prepared source after "
            "successful completion",
            analysis_id,
            exc_info=True,
        )

_FINALIZE_RETRY_MAX = 40
_FINALIZE_RETRY_DELAY_SECONDS = 15

@celery_app.task(bind=True, max_retries=_FINALIZE_RETRY_MAX)
def _finalize_analysis_task(
    self,
    results,
    analysis_id: int,
    generation: int,
    dispatch_start: float | None = None,
) -> None:
    db = sessionLocal(expire_on_commit=False)
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is None:
            logger.warning("Analysis %s not found at finalize time", analysis_id)
            return

        if (
            analysis.status != "processing"
            or analysis.processing_generation != generation
        ):
            logger.info(
                "Analysis %s | generation %s superseded (status=%s, current "
                "generation=%s); skipping finalize",
                analysis_id,
                generation,
                analysis.status,
                analysis.processing_generation,
            )
            return

        incomplete = (
            db.query(AnalysisArtifact)
            .filter(
                AnalysisArtifact.analysis_id == analysis_id,
                AnalysisArtifact.status.notin_(
                    [
                        "completed",
                        "unsupported",
                        "duplicate",
                        "resource_limited",
                        "processing_error",
                    ]
                ),
            )
            .first()
        )
        if incomplete is not None and self.request.retries < _FINALIZE_RETRY_MAX:
            logger.warning(
                "Analysis %s | artifact %s not yet terminal (status=%s); "
                "retrying finalize in %ss (attempt %s/%s)",
                analysis_id,
                incomplete.id,
                incomplete.status,
                _FINALIZE_RETRY_DELAY_SECONDS,
                self.request.retries + 1,
                _FINALIZE_RETRY_MAX,
            )
            raise self.retry(countdown=_FINALIZE_RETRY_DELAY_SECONDS)
        if incomplete is not None:
            logger.warning(
                "Analysis %s | artifact %s not completed (status=%s); skipping finalize",
                analysis_id,
                incomplete.id,
                incomplete.status,
            )
            return

        claim = db.execute(
            update(Analysis)
            .where(
                Analysis.id == analysis_id,
                Analysis.status == "processing",
                Analysis.processing_generation == generation,
                Analysis.finalization_generation.is_(None),
            )
            .values(finalization_generation=generation)
            .execution_options(synchronize_session=False)
        )
        db.commit()
        if claim.rowcount != 1:
            logger.info(
                "Analysis %s | finalization for generation %s already claimed "
                "or superseded; skipping duplicate finalize",
                analysis_id,
                generation,
            )
            return
        db.refresh(analysis)

        _bump_processing_heartbeat(analysis_id, generation)

        total_start = perf_counter()
        parsed_event_count = sum(results) if results else 0

        def _true_total_seconds() -> float:
            if dispatch_start is not None:
                return wall_time() - dispatch_start
            return perf_counter() - total_start

        position_and_lines = (
            db.query(AnalysisArtifact.position, AnalysisArtifact.last_processed_line)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .all()
        )
        artifact_count = len(position_and_lines)

        total_lines = sum(
            last_processed_line for _position, last_processed_line in position_and_lines
        )
        final_processed_bytes = (
            db.query(func.sum(AnalysisArtifact.processed_bytes))
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .scalar()
            or 0
        )
        final_last_processed_line = max(
            (
                position * _GLOBAL_LINE_NUMBER_STRIDE + last_processed_line
                for position, last_processed_line in position_and_lines
            ),
            default=0,
        )

        logger.info(
            "Analysis %s parsed | total_lines=%s | parsed_events=%s | artifacts=%s",
            analysis_id,
            total_lines,
            parsed_event_count,
            artifact_count or 1,
        )
        if dispatch_start is not None:
            logger.info(
                "Analysis %s | concurrent ingestion wall-clock: %.2fs",
                analysis_id,
                wall_time() - dispatch_start,
            )

        publish_progress(
            analysis_id,
            "ingestion",
            "Evidence extraction completed",
            progress=99,
        )

        evidence_count = (
            db.query(func.count(Evidence.id))
            .filter(Evidence.analysis_id == analysis_id)
            .scalar()
            or 0
        )

        if not _finalizer_owns_generation(db, analysis_id, generation):
            logger.info(
                "Analysis %s | generation %s ownership lost before correlation; "
                "stopping finalize",
                analysis_id,
                generation,
            )
            return

        if evidence_count == 0:

            zero_evidence_artifacts = (
                db.query(AnalysisArtifact)
                .filter(AnalysisArtifact.analysis_id == analysis_id)
                .all()
            )
            fallback_artifacts = [
                artifact
                for artifact in zero_evidence_artifacts
                if artifact.fallback_context
            ]

            if fallback_artifacts:
                fallback_payload = build_fallback_payload(
                    analysis_id,
                    fallback_artifacts,
                    artifacts=zero_evidence_artifacts,
                )
                source_outcome = build_source_outcome_payload(analysis)
                source_context = _ready_zero_match_source_context(source_outcome)
                fallback_llm_context = build_fallback_llm_context(
                    analysis_id,
                    fallback_artifacts,
                    artifacts=zero_evidence_artifacts,
                    source_context=source_context,
                )
                if not _finalizer_owns_generation(db, analysis_id, generation):
                    logger.info(
                        "Analysis %s | generation %s ownership lost before "
                        "Gemini (fallback); stopping finalize",
                        analysis_id,
                        generation,
                    )
                    return
                final_ai_analysis: dict | None
                try:
                    gemini_result = generate_investigation_explanation(
                        fallback_llm_context
                    )

                    if not _finalizer_owns_generation(db, analysis_id, generation):
                        logger.info(
                            "Analysis %s | generation %s ownership lost after "
                            "Gemini (fallback); discarding result, stopping finalize",
                            analysis_id,
                            generation,
                        )
                        return
                    fallback_payload["ai_analysis"] = gemini_result.model_dump()
                    final_ai_analysis = fallback_payload["ai_analysis"]
                except GeminiUnavailableError:

                    logger.warning(
                        "Analysis %s | Gemini explanation unavailable; "
                        "completing without an explanation",
                        analysis_id,
                    )
                    final_ai_analysis = None

                _bump_processing_heartbeat(analysis_id, generation)
                if source_outcome is not None:
                    fallback_payload["source"] = source_outcome

                if not _finalizer_owns_generation(db, analysis_id, generation):
                    logger.info(
                        "Analysis %s | generation %s ownership lost before final "
                        "persistence (fallback); discarding result, stopping finalize",
                        analysis_id,
                        generation,
                    )
                    return
                if not _finalize_commit_if_processing(
                    db,
                    analysis,
                    generation=generation,
                    result_snapshot=fallback_payload,
                    ai_analysis=final_ai_analysis,
                    processed_bytes=final_processed_bytes,
                    last_processed_line=final_last_processed_line,
                    stage="fallback",
                ):
                    return
                _cleanup_completed_diagnostic_files(db, analysis_id)
                _cleanup_prepared_source_after_completion(
                    analysis_id, analysis.source_kind
                )

                logger.info(
                    "Analysis %s | zero structured evidence, %s artifact(s) "
                    "with a usable unstructured fallback context",
                    analysis_id,
                    len(fallback_artifacts),
                )
                logger.info(
                    "Analysis %s | TOTAL processing time %.2fs",
                    analysis_id,
                    _true_total_seconds(),
                )

                publish_investigation_result(analysis_id, fallback_payload)
                return

            zero_evidence_payload = build_zero_evidence_payload(
                analysis_id,
                artifacts=zero_evidence_artifacts,
            )
            source_outcome = build_source_outcome_payload(analysis)
            if source_outcome is not None:
                zero_evidence_payload["source"] = source_outcome
            if not _finalize_commit_if_processing(
                db,
                analysis,
                generation=generation,
                result_snapshot=zero_evidence_payload,
                ai_analysis=None,
                processed_bytes=final_processed_bytes,
                last_processed_line=final_last_processed_line,
                stage="zero-evidence",
            ):
                return
            _cleanup_completed_diagnostic_files(db, analysis_id)
            _cleanup_prepared_source_after_completion(analysis_id, analysis.source_kind)

            logger.info(
                "Analysis %s | no meaningful diagnostic evidence found",
                analysis_id,
            )
            logger.info(
                "Analysis %s | TOTAL processing time %.2fs",
                analysis_id,
                _true_total_seconds(),
            )

            publish_progress(
                analysis_id,
                "completed",
                "No meaningful diagnostic evidence found",
                progress=99,
            )

            publish_investigation_result(analysis_id, zero_evidence_payload)

            return

        if not _finalizer_owns_generation(db, analysis_id, generation):
            logger.info(
                "Analysis %s | generation %s ownership lost before identity "
                "persistence; stopping finalize",
                analysis_id,
                generation,
            )
            return

        identity_start = perf_counter()
        if not persist_resolved_identities(
            db=db, analysis_id=analysis_id, generation=generation
        ):
            logger.info(
                "Analysis %s | generation %s ownership lost during identity "
                "persistence; stopping finalize",
                analysis_id,
                generation,
            )
            return
        logger.info("Analysis %s | evidence identities resolved", analysis_id)
        logger.info(
            "Analysis %s | identity resolution completed in %.2fs",
            analysis_id,
            perf_counter() - identity_start,
        )

        if not _finalizer_owns_generation(db, analysis_id, generation):
            logger.info(
                "Analysis %s | generation %s ownership lost after identity "
                "persistence; stopping finalize",
                analysis_id,
                generation,
            )
            return

        evidence_rows, total_evidence_count = select_bounded_evidence_from_db(
            db,
            analysis_id=analysis_id,
            max_records=CORRELATED_MAX_EVIDENCE_RECORDS,
            max_context_bytes=CORRELATED_MAX_CONTEXT_BYTES,
        )
        if total_evidence_count > len(evidence_rows):
            logger.warning(
                "Analysis %s | %s evidence rows exceed "
                "CORRELATED_MAX_EVIDENCE_RECORDS (%s); using a "
                "deterministically-selected bounded working set of %s",
                analysis_id,
                total_evidence_count,
                CORRELATED_MAX_EVIDENCE_RECORDS,
                len(evidence_rows),
            )

        evidence_counts_by_artifact = select_evidence_counts_by_artifact(
            db, analysis_id=analysis_id
        )

        correlation_preparation = prepare_correlation(evidence_rows)
        investigation_path = choose_investigation_path(
            evidence_rows,
            preparation=correlation_preparation,
        )
        logger.info(
            "Analysis %s | investigation_path=%s",
            analysis_id,
            investigation_path.value,
        )
        if investigation_path == InvestigationPath.CORRELATED:
            publish_progress(
                analysis_id,
                "identity",
                "Evidence identity resolution completed",
                progress=99,
            )

            publish_progress(
                analysis_id,
                "correlation",
                "Correlation analysis started",
                progress=99,
            )

            correlation_start = perf_counter()

            correlation_run = run_correlation(
                analysis_id=analysis_id,
                evidence_rows=evidence_rows,
                preparation=correlation_preparation,
            )

            artifact_outcomes = (
                db.query(
                    AnalysisArtifact.id,
                    AnalysisArtifact.position,
                    AnalysisArtifact.original_filename,
                    AnalysisArtifact.detected_format,
                    AnalysisArtifact.status,
                    AnalysisArtifact.duplicate_of_artifact_id,
                    AnalysisArtifact.fallback_context,
                    AnalysisArtifact.failure_reason,
                )
                .filter(AnalysisArtifact.analysis_id == analysis_id)
                .all()
            )

            artifact_ids_with_evidence = set(evidence_counts_by_artifact.keys())
            supplemental_artifacts = [
                artifact
                for artifact in artifact_outcomes
                if artifact.fallback_context
                and artifact.id not in artifact_ids_with_evidence
            ]

            correlation_payload = build_correlation_payload(
                correlation_run,
                evidence_rows,
                artifacts=artifact_outcomes,
                evidence_counts_by_artifact=evidence_counts_by_artifact,
                total_evidence_count=total_evidence_count,
                supplemental_artifacts=supplemental_artifacts,
            )

            logger.info(
                "Analysis %s | correlation completed | components=%s | in %.2fs",
                analysis_id,
                len(correlation_run.result.components),
                perf_counter() - correlation_start,
            )

            publish_progress(
                analysis_id,
                "correlation",
                "Deterministic correlation completed",
                progress=99,
            )

            source_outcome = build_source_outcome_payload(analysis, evidence_rows)
            source_context = _ready_zero_match_source_context(source_outcome)
            llm_context = build_llm_context(
                correlation_run,
                evidence_rows,
                total_evidence_count=total_evidence_count,
                evidence_counts_by_artifact=evidence_counts_by_artifact,
                artifacts=artifact_outcomes,
                supplemental_artifacts=supplemental_artifacts,
                source_context=source_context,
            )

            if not _finalizer_owns_generation(db, analysis_id, generation):
                logger.info(
                    "Analysis %s | generation %s ownership lost before Gemini "
                    "(correlated); stopping finalize",
                    analysis_id,
                    generation,
                )
                return
            final_ai_analysis: dict | None
            try:
                gemini_result = generate_investigation_explanation(llm_context)

                if not _finalizer_owns_generation(db, analysis_id, generation):
                    logger.info(
                        "Analysis %s | generation %s ownership lost after Gemini "
                        "(correlated); discarding result, stopping finalize",
                        analysis_id,
                        generation,
                    )
                    return
                correlation_payload["ai_analysis"] = gemini_result.model_dump()
                final_ai_analysis = correlation_payload["ai_analysis"]
            except GeminiUnavailableError:
                logger.warning(
                    "Analysis %s | Gemini explanation unavailable; "
                    "completing without an explanation",
                    analysis_id,
                )
                final_ai_analysis = None

            _bump_processing_heartbeat(analysis_id, generation)
            if source_outcome is not None:
                correlation_payload["source"] = source_outcome

            if not _finalizer_owns_generation(db, analysis_id, generation):
                logger.info(
                    "Analysis %s | generation %s ownership lost before final "
                    "persistence (correlated); discarding result, stopping finalize",
                    analysis_id,
                    generation,
                )
                return
            if not _finalize_commit_if_processing(
                db,
                analysis,
                generation=generation,
                result_snapshot=correlation_payload,
                ai_analysis=final_ai_analysis,
                processed_bytes=final_processed_bytes,
                last_processed_line=final_last_processed_line,
                stage="correlated",
            ):
                return
            _cleanup_completed_diagnostic_files(db, analysis_id)
            _cleanup_prepared_source_after_completion(analysis_id, analysis.source_kind)
            logger.info(
                "Analysis %s | TOTAL processing time %.2fs",
                analysis_id,
                _true_total_seconds(),
            )

            publish_investigation_result(
                analysis_id,
                correlation_payload,
            )
        else:

            simple_artifacts = (
                db.query(
                    AnalysisArtifact.id,
                    AnalysisArtifact.position,
                    AnalysisArtifact.original_filename,
                    AnalysisArtifact.detected_format,
                    AnalysisArtifact.status,
                    AnalysisArtifact.duplicate_of_artifact_id,
                    AnalysisArtifact.fallback_context,
                    AnalysisArtifact.failure_reason,
                )
                .filter(AnalysisArtifact.analysis_id == analysis_id)
                .all()
            )

            simple_artifact_ids_with_evidence = set(evidence_counts_by_artifact.keys())
            simple_supplemental_artifacts = [
                artifact
                for artifact in simple_artifacts
                if artifact.fallback_context
                and artifact.id not in simple_artifact_ids_with_evidence
            ]

            simple_payload = build_simple_payload(
                analysis_id,
                evidence_rows,
                total_evidence_count=total_evidence_count,
                evidence_counts_by_artifact=evidence_counts_by_artifact,
                artifacts=simple_artifacts,
                supplemental_artifacts=simple_supplemental_artifacts,
            )

            source_outcome = build_source_outcome_payload(analysis, evidence_rows)
            source_context = _ready_zero_match_source_context(source_outcome)
            simple_llm_context = build_simple_llm_context(
                analysis_id,
                evidence_rows,
                total_evidence_count=total_evidence_count,
                evidence_counts_by_artifact=evidence_counts_by_artifact,
                artifacts=simple_artifacts,
                supplemental_artifacts=simple_supplemental_artifacts,
                source_context=source_context,
            )

            if not _finalizer_owns_generation(db, analysis_id, generation):
                logger.info(
                    "Analysis %s | generation %s ownership lost before Gemini "
                    "(simple); stopping finalize",
                    analysis_id,
                    generation,
                )
                return
            final_ai_analysis: dict | None
            try:
                gemini_result = generate_investigation_explanation(simple_llm_context)

                if not _finalizer_owns_generation(db, analysis_id, generation):
                    logger.info(
                        "Analysis %s | generation %s ownership lost after Gemini "
                        "(simple); discarding result, stopping finalize",
                        analysis_id,
                        generation,
                    )
                    return
                simple_payload["ai_analysis"] = gemini_result.model_dump()
                final_ai_analysis = simple_payload["ai_analysis"]
            except GeminiUnavailableError:

                logger.warning(
                    "Analysis %s | Gemini explanation unavailable; "
                    "completing without an explanation",
                    analysis_id,
                )
                final_ai_analysis = None

            _bump_processing_heartbeat(analysis_id, generation)
            if source_outcome is not None:
                simple_payload["source"] = source_outcome

            if not _finalizer_owns_generation(db, analysis_id, generation):
                logger.info(
                    "Analysis %s | generation %s ownership lost before final "
                    "persistence (simple); discarding result, stopping finalize",
                    analysis_id,
                    generation,
                )
                return
            if not _finalize_commit_if_processing(
                db,
                analysis,
                generation=generation,
                result_snapshot=simple_payload,
                ai_analysis=final_ai_analysis,
                processed_bytes=final_processed_bytes,
                last_processed_line=final_last_processed_line,
                stage="simple",
            ):
                return
            _cleanup_completed_diagnostic_files(db, analysis_id)
            _cleanup_prepared_source_after_completion(analysis_id, analysis.source_kind)
            logger.info(
                "Analysis %s | TOTAL processing time %.2fs",
                analysis_id,
                _true_total_seconds(),
            )

            publish_investigation_result(
                analysis_id,
                simple_payload,
            )

    except Retry:
        raise
    except Exception:
        _safe_rollback(db)
        logger.exception("Analysis %s finalize processing failed", analysis_id)
        if _mark_analysis_failed(db, analysis_id, generation=generation):
            _cleanup_files_after_terminal_failure(db, analysis_id)
        raise
    finally:
        db.close()

def _capture_small_text_artifact_fallback(saved_file_path: str) -> dict | None:
    with open(saved_file_path, "rb") as handle:
        raw = handle.read(SIMPLE_FALLBACK_MAX_TEXT_BYTES)
    return capture_text_fallback_context(raw.decode("utf-8", errors="ignore"))

def _artifact_mutation_authorized(
    db: Session,
    analysis_id: int,
    artifact_id: int,
    generation: int,
) -> bool:
    current = (
        db.query(
            Analysis.status,
            Analysis.processing_generation,
        )
        .filter(Analysis.id == analysis_id)
        .with_for_update()
        .first()
    )

    if current is None or current[0] != "processing" or current[1] != generation:
        return False

    artifact_status = (
        db.query(AnalysisArtifact.status)
        .filter(
            AnalysisArtifact.id == artifact_id,
            AnalysisArtifact.analysis_id == analysis_id,
        )
        .with_for_update()
        .scalar()
    )

    return artifact_status == "processing"

def _process_artifact(
    *,
    db: Session,
    analysis: Analysis,
    artifact: AnalysisArtifact,
    generation: int,
    source_index=None,
    global_line_number: int | None = None,
) -> int:
    if global_line_number is None:
        global_line_number = getattr(analysis, "last_processed_line", 0)

    artifact_start = perf_counter()
    initial_size = getsize(artifact.saved_file_path)
    is_migrated_artifact = (
        artifact.status in ("pending", "processing")
        and artifact.size_bytes == 0
        and artifact.detected_format in {None, ArtifactFormat.GENERIC.value}
        and initial_size >= artifact.processed_bytes
    )
    is_migrated_checkpoint = (
        is_migrated_artifact
        and artifact.detected_format == ArtifactFormat.GENERIC.value
        and artifact.processed_bytes > 0
    )

    new_size = artifact.size_bytes

    if initial_size != artifact.size_bytes:
        if not is_migrated_artifact:
            raise RuntimeError(
                f"Artifact {artifact.id} changed after upload; refusing unsafe resume"
            )
        new_size = initial_size

    artifact_format = (
        ArtifactFormat.GENERIC if is_migrated_checkpoint else _artifact_format(artifact)
    )

    if not _artifact_mutation_authorized(
        db,
        analysis.id,
        artifact.id,
        generation,
    ):
        db.rollback()
        logger.info(
            "Analysis %s | artifact %s | generation %s ownership lost before "
            "setup commit; stopping",
            analysis.id,
            artifact.id,
            generation,
        )
        return 0

    artifact.size_bytes = new_size
    artifact.detected_format = artifact_format.value
    artifact.status = "processing"
    db.commit()

    parsed_count = 0
    last_published_progress = -1
    progress_query_step = _progress_query_step_bytes(artifact.size_bytes)
    last_progress_query_offset = artifact.processed_bytes

    if artifact_format == ArtifactFormat.IMAGE:
        extracted_text, ocr_confidence = extract_text_from_image_with_confidence(
            artifact.saved_file_path
        )
        fallback_context = capture_ocr_fallback_context(extracted_text, ocr_confidence)
        if fallback_context is not None:
            if not _artifact_mutation_authorized(
                db,
                analysis.id,
                artifact.id,
                generation,
            ):
                db.rollback()
                logger.info(
                    "Analysis %s | artifact %s | generation %s ownership lost "
                    "before OCR fallback-context commit; stopping",
                    analysis.id,
                    artifact.id,
                    generation,
                )
                return parsed_count

            artifact.fallback_context = fallback_context
            db.commit()
        records = stream_image_events_from_text(
            extracted_text=extracted_text,
            ocr_confidence=ocr_confidence,
            source_file=artifact.original_filename,
            global_line_number=global_line_number,
        )
    else:
        if artifact.size_bytes <= SIMPLE_FALLBACK_MAX_ARTIFACT_BYTES:
            fallback_context = _capture_small_text_artifact_fallback(
                artifact.saved_file_path
            )
            if fallback_context is not None:
                if not _artifact_mutation_authorized(
                    db,
                    analysis.id,
                    artifact.id,
                    generation,
                ):
                    db.rollback()
                    logger.info(
                        "Analysis %s | artifact %s | generation %s ownership lost "
                        "before fallback-context commit; stopping",
                        analysis.id,
                        artifact.id,
                        generation,
                    )
                    return parsed_count

                artifact.fallback_context = fallback_context
                db.commit()
        records = stream_artifact_events(
            file_path=artifact.saved_file_path,
            artifact_format=artifact_format,
            source_file=artifact.original_filename,
            start_offset=artifact.processed_bytes,
            start_artifact_line=artifact.last_processed_line,
            global_line_number=global_line_number,
        )

    for batch in create_batches(records):
        batch_result = _persist_artifact_batch(
            db=db,
            analysis=analysis,
            artifact=artifact,
            generation=generation,
            batch=batch,
            source_index=source_index,
        )
        if batch_result is None:
            logger.info(
                "Analysis %s | artifact %s | generation %s no longer current; stopping",
                analysis.id,
                artifact.id,
                generation,
            )
            return parsed_count
        parsed_count += batch_result

        if getattr(analysis, "source_status", None) == "unavailable":
            source_index = None

        _bump_processing_heartbeat(analysis.id, generation)
        if (
            artifact.processed_bytes - last_progress_query_offset >= progress_query_step
            or artifact.processed_bytes >= artifact.size_bytes
        ):
            last_published_progress = _publish_ingestion_progress(
                db=db,
                analysis_id=analysis.id,
                last_published=last_published_progress,
            )
            last_progress_query_offset = artifact.processed_bytes

    if artifact.processed_bytes > last_progress_query_offset:
        _publish_ingestion_progress(
            db=db,
            analysis_id=analysis.id,
            last_published=last_published_progress,
        )

    actual_size = getsize(artifact.saved_file_path)
    if actual_size != artifact.size_bytes:
        raise RuntimeError(
            f"Artifact {artifact.id} changed during processing; checkpoint not completed"
        )

    current_status, current_generation = (
        db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis.id)
        .with_for_update(read=True)
        .first()
    )
    if current_status != "processing" or current_generation != generation:
        db.rollback()
        logger.info(
            "Analysis %s | artifact %s | generation %s no longer current at "
            "terminal commit (status=%s); not marking completed",
            analysis.id,
            artifact.id,
            generation,
            current_status,
        )
        return parsed_count

    artifact.processed_bytes = artifact.size_bytes
    artifact.status = "completed"
    db.commit()

    logger.info(
        "Analysis %s | artifact_position=%s | format=%s | events=%s | completed in %.2fs",
        analysis.id,
        artifact.position + 1,
        artifact_format.value,
        parsed_count,
        perf_counter() - artifact_start,
    )

    evidence_count = (
        db.query(func.count(Evidence.id))
        .filter(Evidence.artifact_id == artifact.id)
        .scalar()
        or 0
    )
    if evidence_count == 0:
        publish_artifact_outcome(
            analysis.id,
            {
                "analysis_id": analysis.id,
                **build_artifact_outcome_payload(artifact, 0),
            },
        )

    return parsed_count

def _ingestion_percentage(processed_bytes: int | None, total_bytes: int) -> int:
    return min(98, max(0, int((processed_bytes or 0) * 100 // total_bytes)))

def _progress_query_step_bytes(total_bytes: int) -> int:
    return max(1, total_bytes // 100)

def _publish_ingestion_progress(
    *,
    db: Session,
    analysis_id: int,
    last_published: int,
) -> int:
    try:
        totals = (
            db.query(
                func.sum(AnalysisArtifact.processed_bytes),
                func.sum(AnalysisArtifact.size_bytes),
            )
            .filter(
                AnalysisArtifact.analysis_id == analysis_id,
                AnalysisArtifact.status.notin_(
                    [
                        "unsupported",
                        "duplicate",
                        "resource_limited",
                        "processing_error",
                    ]
                ),
            )
            .first()
        )
        processed_bytes, total_bytes = totals if totals is not None else (None, None)
    except Exception:
        logger.debug(
            "Analysis %s | ingestion progress aggregate query failed",
            analysis_id,
            exc_info=True,
        )
        return last_published

    if not total_bytes:
        return last_published

    percentage = _ingestion_percentage(processed_bytes, total_bytes)

    if percentage <= last_published:
        return last_published

    publish_progress(
        analysis_id,
        "ingestion",
        "Diagnostic ingestion in progress",
        progress=percentage,
    )
    return percentage

def _persist_artifact_batch(
    *,
    db: Session,
    analysis: Analysis,
    artifact: AnalysisArtifact,
    generation: int,
    batch,
    source_index=None,
) -> int | None:

    important_events = []
    important_append = important_events.append

    for record in batch:
        event = record.event

        if event is None:
            continue

        event.artifact_id = artifact.id

        if is_evidence_worthy(event):
            important_append(event)

    if source_index is not None:
        current_source_status = (
            db.query(Analysis.source_status).filter(Analysis.id == analysis.id).scalar()
        )
        if current_source_status == "unavailable":
            source_index = None

    source_matching_succeeded = _correlate_source_events(important_events, source_index)
    _assign_batch_fingerprints(important_events)

    current_status, current_generation = (
        db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis.id)
        .with_for_update(read=True)
        .first()
    )
    if current_status != "processing" or current_generation != generation:
        db.rollback()
        return None

    persist_evidence_batch(
        db=db,
        analysis_id=analysis.id,
        events=important_events,
        artifact_id=artifact.id,
    )

    last_record = batch[-1]
    if getattr(artifact, "detected_format", None) != ArtifactFormat.IMAGE.value:
        previous_offset = artifact.processed_bytes
        new_offset = max(previous_offset, last_record.end_offset)
        artifact.processed_bytes = new_offset
        artifact.last_processed_line = last_record.artifact_line_number
    db.commit()

    if not source_matching_succeeded:
        reason = (
            "Source matching became unavailable; diagnostic evidence was "
            "retained without source enrichment."
        )

        db.execute(
            update(Analysis)
            .where(
                Analysis.id == analysis.id,
                Analysis.status == "processing",
                Analysis.processing_generation == generation,
            )
            .values(
                source_status="unavailable",
                source_failure_reason=reason,
            )
            .execution_options(synchronize_session=False)
        )
        db.commit()

        _source_index_process_cache.pop((analysis.id, generation), None)

        db.expire(
            analysis,
            [
                "source_status",
                "source_failure_reason",
            ],
        )

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Analysis %s | artifact=%s | processed batch | events=%s | important=%s",
            analysis.id,
            artifact.id,
            len(batch),
            len(important_events),
        )
    return len(batch)

def _correlate_source_events(events, source_index) -> bool:
    if source_index is None:
        return True

    try:
        matches_by_event = [correlate_event(event, source_index) for event in events]
    except Exception:
        logger.warning(
            "Optional source matching failed; retaining diagnostic evidence "
            "without source enrichment",
            exc_info=True,
        )
        for event in events:
            event.source_matches = []
        return False

    for event, source_matches in zip(events, matches_by_event, strict=True):
        event.source_matches = source_matches
    return True

_source_index_process_cache: dict[tuple[int, int], object] = {}

def _invalidate_source_index_cache(analysis_id: int) -> None:
    for key in [key for key in _source_index_process_cache if key[0] == analysis_id]:
        del _source_index_process_cache[key]

def _cache_source_index(cache_key: tuple[int, int], index) -> None:
    if len(_source_index_process_cache) >= SOURCE_INDEX_PROCESS_CACHE_MAX_ENTRIES:
        oldest_key = next(iter(_source_index_process_cache))
        del _source_index_process_cache[oldest_key]
    _source_index_process_cache[cache_key] = index

def _acquire_source_index(
    analysis: Analysis,
    generation: int,
    publish_callback=None,
):
    if (
        not analysis.source_kind
        or not analysis.source_reference
        or getattr(analysis, "source_status", None) == "unavailable"
    ):
        return None

    cache_key = (analysis.id, generation)
    cached = _source_index_process_cache.get(cache_key)

    if cached is not None:
        return cached

    try:
        if publish_callback is None:
            index = prepare_source(
                analysis.source_kind,
                analysis.source_reference,
                analysis.id,
                generation,
            )
        else:
            index = prepare_source(
                analysis.source_kind,
                analysis.source_reference,
                analysis.id,
                generation,
                publish_callback=publish_callback,
            )

    except SourceInputError:
        raise

    except SourceIndexLimitError as error:
        raise SourceInputError(str(error)) from error

    except Exception as error:
        logger.exception(
            "Analysis %s | optional source acquisition/indexing failed",
            analysis.id,
        )

        raise SourceSubsystemError(
            "Optional source acquisition or indexing failed"
        ) from error

    if index is None:
        return None

    _cache_source_index(cache_key, index)

    return index

def _load_ready_source_index_for_artifact(analysis: Analysis, generation: int):
    if (
        not analysis.source_kind
        or not analysis.source_reference
        or getattr(analysis, "source_status", None) != "ready"
    ):
        return None

    cache_key = (analysis.id, generation)
    cached = _source_index_process_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        index = load_ready_source_index(analysis.id)
    except Exception:
        logger.warning(
            "Analysis %s | ready optional source index could not be loaded; "
            "continuing without source enrichment",
            analysis.id,
            exc_info=True,
        )
        return None
    if index is None:
        return None

    _cache_source_index(cache_key, index)

    return index

def _record_optional_source_failure(
    db: Session,
    analysis: Analysis,
    error: SourceInputError | SourceSubsystemError,
    *,
    generation: int,
    remove_prepared_source: bool = True,
) -> bool:
    detail = (
        str(error)
        if isinstance(error, SourceInputError)
        else "Optional source processing failed"
    )
    source_kind = analysis.source_kind
    if source_kind == "zip":
        reason = f"Uploaded source ZIP could not be prepared: {detail}"
    elif source_kind == "github":
        reason = f"Source repository could not be accessed or prepared: {detail}"
    else:
        reason = f"Source code could not be prepared: {detail}"

    claim = db.execute(
        update(Analysis)
        .where(
            Analysis.id == analysis.id,
            Analysis.status == "processing",
            Analysis.processing_generation == generation,
        )
        .values(source_status="unavailable", source_failure_reason=reason[:500])
        .execution_options(synchronize_session=False)
    )
    db.commit()
    if claim.rowcount != 1:
        logger.info(
            "Analysis %s | generation %s superseded; ignoring source "
            "preparation failure",
            analysis.id,
            generation,
        )
        return False

    _source_index_process_cache.pop((analysis.id, generation), None)

    if remove_prepared_source:
        try:
            cleanup_prepared_source(analysis.id)
        except OSError:
            logger.warning(
                "Analysis %s | could not clean failed optional source preparation",
                analysis.id,
                exc_info=True,
            )

    if source_kind == "zip" and analysis.source_reference:
        try:
            _remove_staged_source_archive(analysis.source_reference)
        except OSError:
            logger.warning(
                "Analysis %s | could not remove staged source ZIP after "
                "source preparation failure",
                analysis.id,
                exc_info=True,
            )

    logger.warning(
        "Analysis %s | optional source unavailable (%s); continuing "
        "diagnostic analysis without source correlation",
        analysis.id,
        source_kind,
    )
    return True

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

def _record_controlled_artifact_failure(
    *,
    db: Session,
    analysis_id: int,
    artifact_id: int,
    generation: int,
    status: str,
    reason: str,
) -> None:
    current_status, current_generation = (
        db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis_id)
        .with_for_update()
        .first()
    )
    if current_status != "processing" or current_generation != generation:
        db.rollback()
        logger.info(
            "Analysis %s | generation %s superseded (status=%s); not "
            "recording controlled failure for artifact %s",
            analysis_id,
            generation,
            current_status,
            artifact_id,
        )
        return

    artifact = (
        db.query(AnalysisArtifact)
        .filter(
            AnalysisArtifact.id == artifact_id,
            AnalysisArtifact.analysis_id == analysis_id,
        )
        .with_for_update()
        .first()
    )
    if artifact is None:
        db.rollback()
        raise RuntimeError(
            f"Artifact {artifact_id} disappeared while recording controlled failure"
        )
    if artifact.status != "processing":
        db.rollback()
        logger.info(
            "Analysis %s | artifact %s ownership lost (status=%s); not "
            "recording controlled failure",
            analysis_id,
            artifact_id,
            artifact.status,
        )
        return

    db.query(Evidence).filter(
        Evidence.analysis_id == analysis_id,
        Evidence.artifact_id == artifact_id,
    ).delete(synchronize_session=False)

    artifact.status = status
    artifact.failure_reason = reason[:500]
    artifact.processed_bytes = 0
    artifact.last_processed_line = 0
    artifact.fallback_context = None
    db.commit()

    _cleanup_diagnostic_artifact_file(artifact.saved_file_path)

    payload = build_artifact_outcome_payload(
        artifact,
        evidence_count=0,
    )
    publish_artifact_outcome(analysis_id, payload)

_UPLOAD_ROOT = Path("uploads").resolve()

def _cleanup_diagnostic_artifact_file(saved_file_path: str) -> None:
    try:
        path = Path(saved_file_path)
        if path.resolve(strict=False).parent != _UPLOAD_ROOT:
            return
        path.unlink(missing_ok=True)
    except Exception:
        logger.warning(
            "Could not remove diagnostic artifact file %s",
            saved_file_path,
            exc_info=True,
        )

def _cleanup_completed_diagnostic_files(db: Session, analysis_id: int) -> None:
    try:
        all_paths = [
            row[0]
            for row in db.query(AnalysisArtifact.saved_file_path)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .all()
        ]
    except Exception:
        logger.warning(
            "Analysis %s | could not list diagnostic artifacts for cleanup",
            analysis_id,
            exc_info=True,
        )
        return

    for saved_file_path in all_paths:
        _cleanup_diagnostic_artifact_file(saved_file_path)

    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    if analysis is None or not analysis.source_kind:
        return

    _invalidate_source_index_cache(analysis_id)
    try:
        cleanup_prepared_source(analysis_id)
    except OSError:
        logger.warning(
            "Analysis %s | could not clean up prepared source after "
            "successful completion",
            analysis_id,
            exc_info=True,
        )

    if analysis.source_kind == "zip" and analysis.source_reference:
        try:
            _remove_staged_source_archive(analysis.source_reference)
        except OSError:
            logger.warning(
                "Analysis %s | could not remove staged source ZIP after "
                "successful completion",
                analysis_id,
                exc_info=True,
            )

def cancel_analysis_and_cleanup(db: Session, analysis_id: int) -> str | None:
    previous_status = (
        db.query(Analysis.status).filter(Analysis.id == analysis_id).scalar()
    )
    if previous_status not in ("pending", "processing"):
        return None

    claim = db.execute(
        update(Analysis)
        .where(
            Analysis.id == analysis_id, Analysis.status.in_(("pending", "processing"))
        )
        .values(status="cancelled")
        .execution_options(synchronize_session=False)
    )
    if claim.rowcount != 1:
        db.rollback()
        return None

    db.query(Evidence).filter(Evidence.analysis_id == analysis_id).delete(
        synchronize_session=False
    )
    db.query(Analysis).filter(Analysis.id == analysis_id).update(
        {
            "ai_analysis": None,
            "result_snapshot": None,
            "processed_bytes": 0,
            "last_processed_line": 0,
            "finalization_generation": None,
        },
        synchronize_session=False,
    )
    db.query(AnalysisArtifact).filter(
        AnalysisArtifact.analysis_id == analysis_id,
    ).update({"fallback_context": None}, synchronize_session=False)
    db.query(AnalysisArtifact).filter(
        AnalysisArtifact.analysis_id == analysis_id,
        AnalysisArtifact.status.in_(["pending", "processing"]),
    ).update(
        {
            "processed_bytes": 0,
            "last_processed_line": 0,
            "failure_reason": None,
        },
        synchronize_session=False,
    )
    db.commit()

    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

    _invalidate_source_index_cache(analysis_id)
    try:
        saved_paths = [
            row[0]
            for row in db.query(AnalysisArtifact.saved_file_path)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .all()
        ]
        for saved_file_path in saved_paths:
            _cleanup_diagnostic_artifact_file(saved_file_path)
    except Exception:
        logger.warning(
            "Analysis %s | could not clean up diagnostic files after cancellation",
            analysis_id,
            exc_info=True,
        )

    if analysis.source_kind:
        try:
            cleanup_prepared_source(analysis_id)
        except OSError:
            logger.warning(
                "Analysis %s | could not clean up prepared source after cancellation",
                analysis_id,
                exc_info=True,
            )

        if analysis.source_kind == "zip" and analysis.source_reference:
            try:
                _remove_staged_source_archive(analysis.source_reference)
            except OSError:
                logger.warning(
                    "Analysis %s | could not remove staged source ZIP after cancellation",
                    analysis_id,
                    exc_info=True,
                )

    logger.info(
        "Analysis %s | cancelled by user request (was %s)",
        analysis_id,
        previous_status,
    )
    return previous_status

def _mark_analysis_failed(
    db: Session, analysis_id: int, *, generation: int | None = None
) -> bool:
    try:
        conditions = [
            Analysis.id == analysis_id,
            Analysis.status.in_(("pending", "processing")),
        ]
        if generation is not None:
            conditions.append(Analysis.processing_generation == generation)
        claim = db.execute(
            update(Analysis)
            .where(*conditions)
            .values(status="failed")
            .execution_options(synchronize_session=False)
        )
        if claim.rowcount != 1:
            db.rollback()
            return False

        db.query(Evidence).filter(Evidence.analysis_id == analysis_id).delete(
            synchronize_session=False
        )
        db.query(Analysis).filter(Analysis.id == analysis_id).update(
            {
                "ai_analysis": None,
                "result_snapshot": None,
                "processed_bytes": 0,
                "last_processed_line": 0,
                "finalization_generation": None,
            },
            synchronize_session=False,
        )
        db.query(AnalysisArtifact).filter(
            AnalysisArtifact.analysis_id == analysis_id,
        ).update({"fallback_context": None}, synchronize_session=False)
        db.query(AnalysisArtifact).filter(
            AnalysisArtifact.analysis_id == analysis_id,
            AnalysisArtifact.status.in_(["pending", "processing"]),
        ).update(
            {
                "processed_bytes": 0,
                "last_processed_line": 0,
                "failure_reason": None,
            },
            synchronize_session=False,
        )
        db.commit()
        return True
    except Exception:
        _safe_rollback(db)
        logger.exception("Could not mark analysis %s as failed", analysis_id)
        return False

def _cleanup_files_after_terminal_failure(db: Session, analysis_id: int) -> None:
    _invalidate_source_index_cache(analysis_id)
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is None:
            return

        saved_paths = [
            row[0]
            for row in db.query(AnalysisArtifact.saved_file_path)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .all()
        ]
        for saved_file_path in saved_paths:
            _cleanup_diagnostic_artifact_file(saved_file_path)
    except Exception:
        logger.warning(
            "Analysis %s | could not clean up diagnostic files after failure",
            analysis_id,
            exc_info=True,
        )
        return

    if analysis.source_kind:
        try:
            cleanup_prepared_source(analysis_id)
        except OSError:
            logger.warning(
                "Analysis %s | could not clean up prepared source after failure",
                analysis_id,
                exc_info=True,
            )

        if analysis.source_kind == "zip" and analysis.source_reference:
            try:
                _remove_staged_source_archive(analysis.source_reference)
            except OSError:
                logger.warning(
                    "Analysis %s | could not remove staged source ZIP after failure",
                    analysis_id,
                    exc_info=True,
                )

def reconstruct_current_investigation_result(
    db: Session,
    analysis_id: int,
    *,
    ai_analysis: dict | None = None,
    result_snapshot: dict | None = None,
) -> dict:
    if result_snapshot is not None:
        return result_snapshot

    evidence_count = (
        db.query(func.count(Evidence.id))
        .filter(Evidence.analysis_id == analysis_id)
        .scalar()
        or 0
    )

    artifacts = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis_id)
        .all()
    )

    if evidence_count == 0:
        fallback_artifacts = [a for a in artifacts if a.fallback_context]
        if fallback_artifacts:
            payload = build_fallback_payload(
                analysis_id, fallback_artifacts, artifacts=artifacts
            )
            if ai_analysis is not None:
                payload["ai_analysis"] = ai_analysis
            return payload
        return build_zero_evidence_payload(analysis_id, artifacts=artifacts)

    evidence_rows, total_evidence_count = select_bounded_evidence_from_db(
        db,
        analysis_id=analysis_id,
        max_records=CORRELATED_MAX_EVIDENCE_RECORDS,
        max_context_bytes=CORRELATED_MAX_CONTEXT_BYTES,
    )
    legacy_evidence_counts_by_artifact = select_evidence_counts_by_artifact(
        db, analysis_id=analysis_id
    )
    legacy_supplemental_artifacts = [
        artifact
        for artifact in artifacts
        if artifact.fallback_context
        and artifact.id not in legacy_evidence_counts_by_artifact
    ]

    correlation_preparation = prepare_correlation(evidence_rows)
    investigation_path = choose_investigation_path(
        evidence_rows,
        preparation=correlation_preparation,
    )

    if investigation_path == InvestigationPath.CORRELATED:
        correlation_run = run_correlation(
            analysis_id=analysis_id,
            evidence_rows=evidence_rows,
            preparation=correlation_preparation,
        )
        payload = build_correlation_payload(
            correlation_run,
            evidence_rows,
            artifacts=artifacts,
            total_evidence_count=total_evidence_count,
            evidence_counts_by_artifact=legacy_evidence_counts_by_artifact,
            supplemental_artifacts=legacy_supplemental_artifacts,
        )
    else:
        payload = build_simple_payload(
            analysis_id,
            evidence_rows,
            total_evidence_count=total_evidence_count,
            evidence_counts_by_artifact=legacy_evidence_counts_by_artifact,
            artifacts=artifacts,
            supplemental_artifacts=legacy_supplemental_artifacts,
        )

    if ai_analysis is not None:
        payload["ai_analysis"] = ai_analysis

    return payload

def compute_current_analysis_state(db: Session, analysis: Analysis) -> dict:
    if analysis.status == "failed":
        return {"analysis_id": analysis.id, "status": "failed"}

    if analysis.status == "cancelled":
        return {"analysis_id": analysis.id, "status": "cancelled"}

    if analysis.status == "completed":
        return {
            "analysis_id": analysis.id,
            "status": "completed",
            "progress": 99,
            "investigation_result": reconstruct_current_investigation_result(
                db,
                analysis.id,
                ai_analysis=analysis.ai_analysis,
                result_snapshot=analysis.result_snapshot,
            ),
        }

    rows = (
        db.query(AnalysisArtifact)
        .filter(AnalysisArtifact.analysis_id == analysis.id)
        .all()
    )

    ingestion_done = bool(rows) and all(
        row.status
        in (
            "unsupported",
            "duplicate",
            "completed",
            "resource_limited",
            "processing_error",
        )
        for row in rows
    )

    if ingestion_done:
        progress = 99
    else:
        dispatchable = [
            row
            for row in rows
            if row.status
            not in ("unsupported", "duplicate", "resource_limited", "processing_error")
        ]
        total_bytes = sum(row.size_bytes for row in dispatchable)
        processed_bytes = sum(row.processed_bytes for row in dispatchable)
        progress = (
            _ingestion_percentage(processed_bytes, total_bytes) if total_bytes else 0
        )

    return {
        "analysis_id": analysis.id,
        "status": analysis.status,
        "progress": progress,
        "artifacts": _known_terminal_artifact_outcomes(db, analysis.id, rows),
    }

def _known_terminal_artifact_outcomes(
    db: Session, analysis_id: int, artifacts: list[AnalysisArtifact]
) -> list[dict]:
    terminal = [
        artifact
        for artifact in artifacts
        if artifact.status
        in (
            "unsupported",
            "duplicate",
            "completed",
            "resource_limited",
            "processing_error",
        )
    ]
    if not terminal:
        return []

    evidence_counts = dict(
        db.query(Evidence.artifact_id, func.count(Evidence.id))
        .filter(Evidence.analysis_id == analysis_id)
        .group_by(Evidence.artifact_id)
        .all()
    )
    filename_by_artifact_id = {
        artifact.id: artifact.original_filename for artifact in artifacts
    }

    return [
        build_artifact_outcome_payload(
            artifact,
            evidence_counts.get(artifact.id, 0),
            filename_by_artifact_id,
        )
        for artifact in terminal
    ]

_STALE_ANALYSIS_THRESHOLD_SECONDS = 300

_PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS = 30 * 60

_RECOVERY_SCAN_BATCH_LIMIT = 25

def _has_active_processing_artifact(analysis_id_column):
    return analysis_id_column.in_(
        select(AnalysisArtifact.analysis_id).where(
            AnalysisArtifact.status == "processing"
        )
    )

def _claim_stale_pending(db: Session, stale_filter, claimed_at: datetime) -> list[int]:
    candidate_ids = [
        row[0]
        for row in db.query(Analysis.id)
        .filter(stale_filter)
        .order_by(Analysis.id)
        .limit(_RECOVERY_SCAN_BATCH_LIMIT)
        .all()
    ]

    claimed_ids: list[int] = []
    for analysis_id in candidate_ids:
        result = db.execute(
            update(Analysis)
            .where(Analysis.id == analysis_id, stale_filter)
            .values(processing_heartbeat_at=claimed_at)
            .execution_options(synchronize_session=False)
        )
        db.commit()
        if result.rowcount == 1:
            claimed_ids.append(analysis_id)
    return claimed_ids

def _claim_and_demote_stale_processing(
    db: Session, stale_filter, claimed_at: datetime
) -> list[int]:
    candidates = (
        db.query(Analysis.id, Analysis.processing_generation, Analysis.source_status)
        .filter(stale_filter)
        .order_by(Analysis.id)
        .limit(_RECOVERY_SCAN_BATCH_LIMIT)
        .all()
    )

    claimed_ids: list[int] = []
    for analysis_id, stale_generation, stale_source_status in candidates:
        demotion_values = dict(
            status="pending",
            processing_heartbeat_at=claimed_at,
            finalization_generation=None,
        )
        if stale_source_status == "preparing":
            demotion_values["source_status"] = None
            demotion_values["source_failure_reason"] = None

        result = db.execute(
            update(Analysis)
            .where(Analysis.id == analysis_id, stale_filter)
            .values(**demotion_values)
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            db.rollback()
            continue

        stuck_artifacts = (
            db.query(AnalysisArtifact.id, AnalysisArtifact.detected_format)
            .filter(
                AnalysisArtifact.analysis_id == analysis_id,
                AnalysisArtifact.status == "processing",
            )
            .all()
        )
        image_artifact_ids = [
            row.id
            for row in stuck_artifacts
            if row.detected_format == ArtifactFormat.IMAGE.value
        ]
        if image_artifact_ids:
            db.query(Evidence).filter(
                Evidence.artifact_id.in_(image_artifact_ids)
            ).delete(synchronize_session=False)
            db.query(AnalysisArtifact).filter(
                AnalysisArtifact.id.in_(image_artifact_ids)
            ).update(
                {
                    "status": "pending",
                    "processed_bytes": 0,
                    "last_processed_line": 0,
                    "fallback_context": None,
                },
                synchronize_session=False,
            )

        db.query(AnalysisArtifact).filter(
            AnalysisArtifact.analysis_id == analysis_id,
            AnalysisArtifact.status == "processing",
        ).update({"status": "pending"}, synchronize_session=False)
        db.commit()
        if stale_source_status == "preparing":
            try:
                cleanup_generation_source_temp(
                    analysis_id,
                    stale_generation,
                )
            except OSError:
                logger.warning(
                    "Analysis %s | could not clean up generation %s's "
                    "abandoned source staging directory",
                    analysis_id,
                    stale_generation,
                    exc_info=True,
                )

        claimed_ids.append(analysis_id)
    return claimed_ids

@celery_app.task
def recover_stale_analyses() -> int:
    db = sessionLocal()
    processing_fast_claimed: list[int] = []
    processing_queue_claimed: list[int] = []
    pending_claimed: list[int] = []
    try:
        now = datetime.now(timezone.utc)
        fast_cutoff = now - timedelta(seconds=_STALE_ANALYSIS_THRESHOLD_SECONDS)
        queue_cutoff = now - timedelta(
            seconds=_PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS
        )

        has_active_artifact = _has_active_processing_artifact(Analysis.id)
        has_active_finalizer = and_(
            Analysis.finalization_generation.isnot(None),
            Analysis.finalization_generation == Analysis.processing_generation,
        )
        has_active_work = or_(has_active_artifact, has_active_finalizer)

        processing_fast_stale = and_(
            Analysis.status == "processing",
            has_active_work,
            or_(
                Analysis.processing_heartbeat_at.is_(None),
                Analysis.processing_heartbeat_at < fast_cutoff,
            ),
        )
        processing_queue_stale = and_(
            Analysis.status == "processing",
            ~has_active_work,
            or_(
                Analysis.processing_heartbeat_at.is_(None),
                Analysis.processing_heartbeat_at < queue_cutoff,
            ),
        )
        pending_stale = and_(
            Analysis.status == "pending",
            func.coalesce(Analysis.processing_heartbeat_at, Analysis.created_at)
            < queue_cutoff,
        )

        processing_fast_claimed = _claim_and_demote_stale_processing(
            db, processing_fast_stale, now
        )
        processing_queue_claimed = _claim_and_demote_stale_processing(
            db, processing_queue_stale, now
        )
        pending_claimed = _claim_stale_pending(db, pending_stale, now)
    except Exception:
        _safe_rollback(db)
        logger.exception("Stale analysis recovery scan failed")
        raise
    finally:
        db.close()

    for analysis_id in processing_fast_claimed:
        logger.warning(
            "Analysis %s | reclaimed stale processing analysis (active work "
            "stopped heartbeating) after over %ss; redispatching",
            analysis_id,
            _STALE_ANALYSIS_THRESHOLD_SECONDS,
        )
        process_analysis.delay(analysis_id)

    for analysis_id in processing_queue_claimed:
        logger.warning(
            "Analysis %s | reclaimed processing analysis with no active "
            "work after queue age exceeded %ss; redispatching",
            analysis_id,
            _PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS,
        )
        process_analysis.delay(analysis_id)

    for analysis_id in pending_claimed:
        logger.warning(
            "Analysis %s | reclaimed long-pending analysis after queue age "
            "exceeded %ss; redispatching",
            analysis_id,
            _PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS,
        )
        process_analysis.delay(analysis_id)

    return (
        len(processing_fast_claimed)
        + len(processing_queue_claimed)
        + len(pending_claimed)
    )
