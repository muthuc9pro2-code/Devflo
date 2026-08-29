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
    cleanup_prepared_source,
    prepare_source,
)
from app.services.source_index import correlate_event
from app.services.investigation_router import choose_investigation_path, InvestigationPath
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

def _bail_if_cancelled(db: Session, analysis_id: int, stage: str) -> bool:
    """Fresh, cheap (single indexed column) re-check at a finalize-stage
    boundary where real time may have passed since analysis.status was
    last read (Gemini calls in particular). Returns True (and logs) when
    finalize must stop here WITHOUT touching Analysis/Evidence at all -
    the cancel endpoint has already durably reset/cleared everything for
    this analysis; stale finalization must never re-create result data or
    resurrect cancelled -> completed."""
    if _is_analysis_cancelled(db, analysis_id):
        logger.info(
            "Analysis %s | cancelled; stopping finalize at %s", analysis_id, stage
        )
        return True
    return False


def _finalize_commit_if_processing(
    db: Session,
    analysis: Analysis,
    *,
    generation: int,
    result_snapshot: dict,
    stage: str,
) -> bool:
    """The final transactional fence for every completed-persistence branch
    in _finalize_analysis_task() (fallback/zero-evidence/correlated/simple).

    The ordinary _bail_if_cancelled() checkpoints above are plain unlocked
    SELECTs - useful early exits, but each still leaves a check-then-commit
    gap a concurrent cancel_analysis_and_cleanup() commit can land in
    between the check and this function's own commit. This closes that gap
    the same way _persist_artifact_batch's cancel-vs-Evidence fence already
    does it (see its "Cancel-vs-Evidence-commit race fence" comment): a
    locking read (SELECT ... FOR UPDATE) on this one Analysis row, which
    either already observes a committed non-"processing" status, or blocks
    until the cancel endpoint's own UPDATE commits/rolls back and then
    observes its result - never a stale snapshot read. Also requires both
    processing_generation and finalization_generation to still match this
    exact execution - the durable finalization claim earlier in
    _finalize_analysis_task already makes a second finalizer for the same
    generation impossible, but this is the last line of defense in case
    that invariant is ever violated.

    Returns False (after rolling back any pending ORM changes made earlier
    in the branch, e.g. analysis.ai_analysis set after a Gemini call) if
    cancellation (or, defensively, any other terminal transition) already
    won by the time this runs - the caller must then return immediately
    without persisting or publishing a completed result. Returns True only
    after result_snapshot/status="completed" have been committed.
    """
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
    analysis.status = "completed"
    db.commit()
    return True


# --- Recovery / orphan detection --------------------------------------
#
# Analysis.processing_heartbeat_at is a throttled liveness signal, never a
# second progress-tracking system: ordinary per-batch artifact commits
# deliberately do not dirty the shared Analysis row, so this
# is the one place that does, and only rarely.
_HEARTBEAT_MIN_INTERVAL_SECONDS = 60.0

# Process-local (Celery workers are separate OS processes, same caveat as
# _source_index_process_cache below) throttle state - bounded with the
# same simple FIFO eviction so a long-lived worker touching many analyses
# cannot grow this unboundedly. Never the source of truth: a missed/lost
# throttle entry only means one extra (still cheap, still correct) write.
_last_heartbeat_write: dict[int, float] = {}
_HEARTBEAT_THROTTLE_CACHE_MAX_ENTRIES = 64


def _bump_processing_heartbeat(analysis_id: int, generation: int) -> None:
    """Best-effort, throttled liveness signal for orphan recovery only -
    NOT correctness-critical (persisted Evidence/AnalysisArtifact
    checkpoints remain the real resume state regardless of whether this
    write ever lands), so a failure here is logged and swallowed, never
    retried, never allowed to affect the caller.

    Deliberately owns its OWN short-lived session/transaction rather than
    reusing the caller's: a caller's Session.commit() flushes every dirty
    ORM object in that session, not only this UPDATE - a finalizer that has
    already computed (but not yet durably committed under its own fence)
    result_snapshot/ai_analysis on its in-memory `analysis` object must
    never have those fields leak out early through what is meant to be an
    unrelated liveness write. Using a separate session/transaction here
    makes that structurally impossible.

    The UPDATE is itself conditionally scoped to
    (id=analysis_id, status="processing", processing_generation=generation)
    - an old, superseded generation's heartbeat can never keep a new
    generation's row looking alive, and a terminal (completed/failed/
    cancelled) analysis can never be touched by a stale heartbeat either.

    Throttled to at most once per _HEARTBEAT_MIN_INTERVAL_SECONDS per
    (analysis_id, generation), independent of how often the caller invokes
    this - this is what keeps it from recreating the shared-Analysis-row
    write contention a per-batch heartbeat write would otherwise cause:
    even a burst of many batches within the throttle window writes the
    heartbeat at most once, not once per batch. Keyed by generation (not
    just analysis_id) so a fresh generation after recovery is never
    silently suppressed by a stale throttle entry the old generation left
    behind.
    """
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
        logger.debug(
            "Analysis %s | heartbeat write failed", analysis_id, exc_info=True
        )
    finally:
        heartbeat_db.close()


@celery_app.task
def process_analysis(analysis_id: int):
    """Dispatch bounded, per-artifact concurrent processing for one analysis.

    Each artifact is an independent unit of work processed by its own Celery
    task with its own DB session (_process_artifact_task) - no SQLAlchemy
    Session or mutable ORM instance is ever shared across artifacts. Source
    ZIP/GitHub prep (if any) runs once, before any artifact task starts:
    per-artifact evidence construction correlates against the source index
    inline (_correlate_source_events, inside _persist_artifact_batch), so
    the index must already exist before ANY artifact begins - a real
    dependency, not merely an optimization opportunity, so it is
    deliberately NOT run concurrently with the artifacts. Identity
    resolution, timeline reconstruction, and correlation run in
    _finalize_analysis_task, the chord callback that Celery guarantees only
    fires after every artifact task in the group has completed
    successfully. Global concurrency is bounded by celery_app's
    worker_concurrency setting (app/core/celery_app.py), not by anything in
    this function - this task never blocks waiting on its children, so it
    cannot itself consume a worker slot the children need.
    """
    db = sessionLocal(expire_on_commit=False)
    finalize_only = False
    generation = None

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            logger.warning("Analysis %s not found", analysis_id)
            return

        # The ONLY place a workflow may begin: an atomic pending->processing
        # claim. A plain read-status-then-write (the old approach) let two
        # concurrent invocations (Celery's own at-least-once broker
        # redelivery, or two Beat-recovery redispatches) both observe
        # "processing is fine to proceed from" and both dispatch a complete
        # duplicate workflow - the real cause of the production duplicate-
        # dispatch/duplicate-Gemini-call incident. Only the invocation whose
        # UPDATE actually flips a still-"pending" row wins; every other
        # invocation (including one that sees this analysis already
        # "processing" from a workflow that is still healthy) returns here
        # without touching anything else. Establishes a fresh
        # processing_generation and clears any prior finalizer claim so
        # every child task below is unambiguously scoped to this one
        # execution.
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
           
            if row.status not in ("unsupported", "duplicate", "resource_limited", "processing_error")
        ]

        finalize_only = all(
            row.status in ("completed", "unsupported", "duplicate", "resource_limited", "processing_error")
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
        artifact_group, _finalize_analysis_task.s(analysis_id, generation, dispatch_start)
    )
    if needs_source_prep:
        workflow = chain(_prepare_source_task.si(analysis_id, generation), workflow)

    workflow.apply_async()

    logger.info(
        "Analysis %s | dispatched %s artifact task(s)%s (worker_concurrency=%s)",
        analysis_id,
        len(artifact_ids),
        " after source prep" if needs_source_prep else "",
        celery_app.conf.worker_concurrency,
    )


@celery_app.task
def _process_artifact_task(analysis_id: int, artifact_id: int, generation: int) -> int:
    """Process exactly one artifact. Independent unit of work: its own DB
    session, only ever reads/writes this artifact's own AnalysisArtifact row
    and inserts evidence scoped to its own artifact_id, so it is safe to run
    concurrently with any other artifact task (same analysis or a different
    one) up to celery_app's worker_concurrency bound.

    `generation` is the processing_generation this task was dispatched
    with. Every durable mutation below re-verifies the analysis is still
    "processing" under that exact generation before proceeding - this is
    what makes a zombie invocation (Celery broker redelivery while the
    original run is still alive, or a task from an execution
    recovery has since superseded) provably unable to touch anything: it
    fails the very first fence and returns 0 immediately.
    """
    db = sessionLocal(expire_on_commit=False)
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        artifact = (
            db.query(AnalysisArtifact)
            .filter(AnalysisArtifact.id == artifact_id)
            .first()
        )

        if analysis is None or artifact is None:
            logger.warning(
                "Analysis %s | artifact %s not found; skipping",
                analysis_id,
                artifact_id,
            )
            return 0

        if analysis.status != "processing" or analysis.processing_generation != generation:
            logger.info(
                "Analysis %s | generation %s superseded (status=%s, "
                "current generation=%s); skipping artifact %s",
                analysis_id,
                generation,
                analysis.status,
                analysis.processing_generation,
                artifact_id,
            )
            return 0

        if artifact.status in ("completed", "resource_limited", "processing_error"):
            return 0

        # Atomic artifact claim: only a still-"pending" artifact can be
        # claimed. Two concurrent invocations for the same artifact
        # (broker redelivery, or a recovery redispatch racing a still-live
        # original) can therefore never both start streaming/parsing the
        # same byte range - the loser's UPDATE affects zero rows and it
        # returns without touching Evidence or the checkpoint at all.
        claim = db.execute(
            update(AnalysisArtifact)
            .where(AnalysisArtifact.id == artifact_id, AnalysisArtifact.status == "pending")
            .values(status="processing")
            .execution_options(synchronize_session=False)
        )
        db.commit()
        if claim.rowcount != 1:
            logger.info(
                "Analysis %s | artifact %s not claimed (status=%s); "
                "another execution already owns it",
                analysis_id,
                artifact_id,
                artifact.status,
            )
            return 0
        db.refresh(artifact)

        _bump_processing_heartbeat(analysis_id, generation)

        try:
            source_index = _prepare_source_index(analysis, generation)
        except (SourceInputError, SourceSubsystemError) as error:
            db.rollback()
            if not _record_optional_source_failure(db, analysis, error, generation=generation):
                return 0
            source_index = None

        global_line_number = artifact.position * _GLOBAL_LINE_NUMBER_STRIDE

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


@celery_app.task
def _prepare_source_task(analysis_id: int, generation: int) -> None:
    """Run source ZIP/GitHub prep+indexing exactly once, before any artifact
    task starts (see process_analysis docstring for why this cannot safely
    run concurrently with artifact processing).
    """
    db = sessionLocal(expire_on_commit=False)
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is None:
            logger.warning("Analysis %s not found for source prep", analysis_id)
            return

        if analysis.status != "processing" or analysis.processing_generation != generation:
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

        _bump_processing_heartbeat(analysis_id, generation)

        source_prep_start = perf_counter()
        _prepare_source_index(analysis, generation)

        current_status, current_generation = (
            db.query(Analysis.status, Analysis.processing_generation)
            .filter(Analysis.id == analysis_id)
            .first()
        )
        if current_status != "processing" or current_generation != generation:
            logger.info(
                "Analysis %s | generation %s superseded (status=%s) during "
                "source preparation; discarding",
                analysis_id,
                generation,
                current_status,
            )
            _source_index_process_cache.pop((analysis_id, generation), None)
            try:
                cleanup_prepared_source(analysis_id)
            except OSError:
                logger.warning(
                    "Analysis %s | could not clean up source prepared for a "
                    "since-superseded execution",
                    analysis_id,
                    exc_info=True,
                )
            return

        analysis.source_status = "ready"
        analysis.source_failure_reason = None
        db.commit()
        logger.info(
            "Analysis %s | source prep (%s) completed in %.2fs",
            analysis_id,
            analysis.source_kind,
            perf_counter() - source_prep_start,
        )
    except (SourceInputError, SourceSubsystemError) as error:
        db.rollback()
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is None:
            return
        _record_optional_source_failure(db, analysis, error, generation=generation)
        return
    except Exception:
        _safe_rollback(db)
        logger.exception("Analysis %s | source preparation failed", analysis_id)
        if _mark_analysis_failed(db, analysis_id, generation=generation):
            _cleanup_files_after_terminal_failure(db, analysis_id)
        raise
    finally:
        db.close()


_FINALIZE_RETRY_MAX = 8
_FINALIZE_RETRY_DELAY_SECONDS = 5


@celery_app.task(bind=True, max_retries=_FINALIZE_RETRY_MAX)
def _finalize_analysis_task(
    self, results, analysis_id: int, generation: int, dispatch_start: float | None = None
) -> None:
    """Chord callback for process_analysis: Celery only invokes this after
    every task in the artifact group has completed successfully. As a
    second, independent safeguard against relying on chord-callback edge
    cases alone, this also explicitly re-checks that every artifact for
    this analysis is actually terminal and that the analysis is still
    "processing" under the exact generation this finalizer was dispatched
    with, before doing any identity/timeline/correlation/Gemini work.

    Then claims finalization for this generation via one atomic conditional
    UPDATE - only the winning invocation proceeds past this point, so a
    duplicate finalizer (a second chord dispatch, or a raw broker
    redelivery of this exact task) can reach Evidence-shaped work but never
    a second Gemini call, and never a second final persistence (the
    existing _finalize_commit_if_processing fence, extended to also check
    both generations, remains as defense-in-depth against redelivery of an
    already-completed finalization).
    """
    db = sessionLocal(expire_on_commit=False)
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is None:
            logger.warning("Analysis %s not found at finalize time", analysis_id)
            return

        if analysis.status != "processing" or analysis.processing_generation != generation:
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

        # Durable finalization claim: only one invocation for this
        # generation may ever proceed past this point to identity
        # resolution/correlation/Gemini. A second, distinct finalizer
        # invocation (a duplicate chord dispatch, or a raw broker
        # redelivery of an abandoned finalizer) finds finalization_generation
        # already set and returns here, before any expensive work - not
        # merely before the final DB write. A genuinely-abandoned finalizer
        # is only ever un-stuck by stale-analysis recovery advancing the
        # generation (see recover_stale_analyses), never by letting a
        # redelivered message re-claim the same generation.
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

        if analysis.source_kind:
            _invalidate_source_index_cache(analysis_id)
            try:
                cleanup_prepared_source(analysis_id)
            except OSError:
                logger.warning(
                    "Analysis %s | could not clean up prepared source",
                    analysis_id,
                    exc_info=True,
                )

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
        analysis.processed_bytes = (
            db.query(func.sum(AnalysisArtifact.processed_bytes))
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .scalar()
            or 0
        )
        analysis.last_processed_line = max(
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

       
        if _bail_if_cancelled(db, analysis_id, "before correlation"):
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
                fallback_llm_context = build_fallback_llm_context(
                    analysis_id,
                    fallback_artifacts,
                    artifacts=zero_evidence_artifacts,
                )
                if _bail_if_cancelled(db, analysis_id, "before Gemini (fallback)"):
                    return
                try:
                    gemini_result = generate_investigation_explanation(fallback_llm_context)
                    
                    if _bail_if_cancelled(db, analysis_id, "after Gemini (fallback)"):
                        return
                    fallback_payload["ai_analysis"] = gemini_result.model_dump()
                   
                    analysis.ai_analysis = fallback_payload["ai_analysis"]
                except GeminiUnavailableError:
                   
                    logger.warning(
                        "Analysis %s | Gemini explanation unavailable; "
                        "completing without an explanation",
                        analysis_id,
                    )
                    analysis.ai_analysis = None
               
                _bump_processing_heartbeat(analysis_id, generation)
                source_outcome = build_source_outcome_payload(analysis)
                if source_outcome is not None:
                    fallback_payload["source"] = source_outcome
               
                if not _finalize_commit_if_processing(
                    db, analysis, generation=generation, result_snapshot=fallback_payload, stage="fallback"
                ):
                    return
                _cleanup_completed_diagnostic_files(db, analysis_id)

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
                db, analysis, generation=generation, result_snapshot=zero_evidence_payload, stage="zero-evidence"
            ):
                return
            _cleanup_completed_diagnostic_files(db, analysis_id)

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

        identity_start = perf_counter()
        persist_resolved_identities(db=db, analysis_id=analysis_id)
        logger.info("Analysis %s | evidence identities resolved", analysis_id)
        logger.info(
            "Analysis %s | identity resolution completed in %.2fs",
            analysis_id,
            perf_counter() - identity_start,
        )

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
                if artifact.fallback_context and artifact.id not in artifact_ids_with_evidence
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

            llm_context = build_llm_context(
                correlation_run,
                evidence_rows,
                total_evidence_count=total_evidence_count,
                evidence_counts_by_artifact=evidence_counts_by_artifact,
                artifacts=artifact_outcomes,
                supplemental_artifacts=supplemental_artifacts,
            )

            if _bail_if_cancelled(db, analysis_id, "before Gemini (correlated)"):
                return
            try:
                gemini_result = generate_investigation_explanation(llm_context)
                
                if _bail_if_cancelled(db, analysis_id, "after Gemini (correlated)"):
                    return
                correlation_payload["ai_analysis"] = gemini_result.model_dump()
              
                analysis.ai_analysis = correlation_payload["ai_analysis"]
            except GeminiUnavailableError:
                logger.warning(
                    "Analysis %s | Gemini explanation unavailable; "
                    "completing without an explanation",
                    analysis_id,
                )
                analysis.ai_analysis = None
            
            _bump_processing_heartbeat(analysis_id, generation)
            source_outcome = build_source_outcome_payload(analysis, evidence_rows)
            if source_outcome is not None:
                correlation_payload["source"] = source_outcome
           
            if not _finalize_commit_if_processing(
                db, analysis, generation=generation, result_snapshot=correlation_payload, stage="correlated"
            ):
                return
            _cleanup_completed_diagnostic_files(db, analysis_id)
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

            # Mirrors the CORRELATED branch's llm_context above.
            simple_llm_context = build_simple_llm_context(
                analysis_id,
                evidence_rows,
                total_evidence_count=total_evidence_count,
                evidence_counts_by_artifact=evidence_counts_by_artifact,
                artifacts=simple_artifacts,
                supplemental_artifacts=simple_supplemental_artifacts,
            )

            if _bail_if_cancelled(db, analysis_id, "before Gemini (simple)"):
                return
            try:
                gemini_result = generate_investigation_explanation(simple_llm_context)
               
                if _bail_if_cancelled(db, analysis_id, "after Gemini (simple)"):
                    return
                simple_payload["ai_analysis"] = gemini_result.model_dump()
               
                analysis.ai_analysis = simple_payload["ai_analysis"]
            except GeminiUnavailableError:
                
                logger.warning(
                    "Analysis %s | Gemini explanation unavailable; "
                    "completing without an explanation",
                    analysis_id,
                )
                analysis.ai_analysis = None

            _bump_processing_heartbeat(analysis_id, generation)
            source_outcome = build_source_outcome_payload(analysis, evidence_rows)
            if source_outcome is not None:
                simple_payload["source"] = source_outcome
           
            if not _finalize_commit_if_processing(
                db, analysis, generation=generation, result_snapshot=simple_payload, stage="simple"
            ):
                return
            _cleanup_completed_diagnostic_files(db, analysis_id)
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
        # self.retry() above (incomplete artifacts, bounded wait for the
        # chord's own children to become durably terminal) - genuine
        # control flow, never a real failure; must reach Celery's task
        # machinery untouched, not be treated as an analysis-wide failure.
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
    """A bounded (<= SIMPLE_FALLBACK_MAX_TEXT_BYTES) prefix read of an
    already-confirmed-small (<= SIMPLE_FALLBACK_MAX_ARTIFACT_BYTES, a few
    MiB at most) text artifact - never the cost of reopening a full 1 GiB
    artifact, and never a second pass over the artifact's
    real parsing/retention path (stream_artifact_events runs exactly once,
    immediately after this, unaffected by it)."""
    with open(saved_file_path, "rb") as handle:
        raw = handle.read(SIMPLE_FALLBACK_MAX_TEXT_BYTES)
    return capture_text_fallback_context(raw.decode("utf-8", errors="ignore"))


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

    if initial_size != artifact.size_bytes:
        if not is_migrated_artifact:
            raise RuntimeError(
                f"Artifact {artifact.id} changed after upload; refusing unsafe resume"
            )
        artifact.size_bytes = initial_size

    artifact_format = (
        ArtifactFormat.GENERIC if is_migrated_checkpoint else _artifact_format(artifact)
    )
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

        # A source-matcher failure is recorded by _persist_artifact_batch on
        # the shared Analysis row.  Stop invoking that optional matcher for
        # later batches in this same artifact; parsing/evidence persistence
        # continue normally without repeated failures or source work.
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

    # Terminal-commit fence: the per-batch fence in _persist_artifact_batch
    # already re-verifies the generation before every intermediate commit,
    # but a real (if narrow) window remains between the last batch's fence
    # and this final artifact-completion write. Re-verify once more here so
    # a superseded execution can never mark an artifact "completed" after
    # cancellation/failure/a new generation has already won.
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
    """The one formula both live SSE ingestion progress
    (_publish_ingestion_progress) and current-state reconstruction
    (compute_current_analysis_state) use - extracted so the two can never
    drift apart. Caller is responsible for only calling this with a
    truthy total_bytes; this only computes the clamped ratio."""
    return min(98, max(0, int((processed_bytes or 0) * 100 // total_bytes)))


def _progress_query_step_bytes(total_bytes: int) -> int:
    """At most roughly 100 aggregate refreshes per active artifact task.

    Small investigations retain responsive progress because the threshold can
    fall below a normal batch size; large investigations stop issuing one SUM
    query for every parser batch.
    """
    return max(1, total_bytes // 100)


def _publish_ingestion_progress(
    *,
    db: Session,
    analysis_id: int,
    last_published: int,
) -> int:
    """Aggregate ingestion progress across every artifact of this analysis,
    derived from the same persisted AnalysisArtifact.processed_bytes/
    size_bytes checkpoint accounting each batch commit already maintains -
    not a second, competing progress tracker.

    Safe under Task 1's concurrent artifact tasks: this is a plain read
    (SUM aggregate), and each concurrent task only ever writes its OWN
    artifact row (see _persist_artifact_batch), so there is no shared-state
    read-modify-write race to guard against here.

    Deduplication is deliberately local to this one artifact's processing
    loop (no new DB column or Redis key): only publishes when the computed
    integer percentage is a genuine advance over what this call chain has
    already published, so a single large artifact's many batch commits
    don't flood the SSE stream with repeated identical percentages. Two
    concurrently-running artifact tasks (bounded by worker_concurrency)
    computing the same percentage independently can each publish it once -
    at most a 2x duplicate, not the unbounded flooding this guards against.

    Clamped to [0, 98]: 99 is reserved for the post-ingestion stage
    (identity/timeline/correlation), published separately once ingestion
    for the whole analysis is confirmed complete.
    """
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
    """Returns the number of raw records processed, or None specifically to
    signal "this execution is no longer current - stop, nothing was
    persisted" to the caller's batch loop (see _process_artifact). A real
    batch is never empty (create_batches never yields one), so len(batch)
    is always >= 1 - None is otherwise unambiguous as that sentinel.
    """

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
        # A worker's process-local source_index reference does not
        # invalidate itself when a DIFFERENT worker durably marks this
        # analysis's source "unavailable" (its own matcher failure, or a
        # source-prep failure discovered after this worker already cached
        # a good index). Re-checking the durable flag at every batch
        # boundary - a cheap single-column read, no lock needed since a
        # few milliseconds of staleness here is harmless - means that
        # observation reliably stops NEW source matching going forward
        # without ever blocking or delaying diagnostic Evidence
        # persistence itself, which proceeds unconditionally below either
        # way.
        current_source_status = (
            db.query(Analysis.source_status)
            .filter(Analysis.id == analysis.id)
            .scalar()
        )
        if current_source_status == "unavailable":
            source_index = None

    source_matching_succeeded = _correlate_source_events(
        important_events, source_index
    )
    _assign_batch_fingerprints(important_events)

    # Cancel/generation-vs-Evidence-commit race fence. A plain unlocked
    # SELECT here would still leave a check-then-commit gap a concurrent
    # cancel (or a recovery invalidating this generation) could land in.
    # with_for_update(read=True) instead takes a SHARED row lock on this
    # one Analysis row (MySQL: LOCK IN SHARE MODE), held only until this
    # same transaction's commit/rollback below:
    #   - multiple concurrent artifact-batch transactions taking a SHARED
    #     lock never block each other - this is NOT the shared-Analysis-
    #     row UPDATE contention a per-batch write would cause, and that
    #     stays avoided; under normal operation (no cancellation in
    #     flight) this never blocks anything and costs one extra indexed
    #     single-row read.
    #   - it DOES block against the cancel endpoint's own brief, rare
    #     UPDATE (an EXCLUSIVE lock), and only for the handful of
    #     milliseconds that commit takes - once unblocked, it is
    #     guaranteed to observe the true, latest COMMITTED status.
    # Net effect: if cancellation (or a new generation) has already
    # committed by the time this runs, this batch's Evidence/checkpoint
    # writes are rolled back instead of committed. Any batch that commits
    # BEFORE the cancellation tombstone wins is still caught by the cancel
    # endpoint's own Evidence cleanup, which runs AFTER its tombstone
    # commits and deletes everything present at that moment, including
    # this kind of just-committed batch.
    #
    # Deliberately does NOT also write analysis.source_status here (see
    # below): holding this SHARED lock and then attempting to upgrade it to
    # EXCLUSIVE in the same transaction is exactly the shape of a MySQL
    # lock-upgrade deadlock once two concurrent artifact tasks for the same
    # analysis both hit a source-matching failure around the same time.
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
    # IMAGE (OCR) records carry a logical line position within the
    # extracted text as end_offset, never a byte offset into the encoded
    # image - persisting it into processed_bytes would masquerade as a
    # real byte checkpoint. OCR has no resumable checkpoint (see
    # _claim_and_demote_stale_processing): processed_bytes must stay 0 for
    # an IMAGE artifact until the single terminal write sets it to
    # size_bytes on full completion.
    if getattr(artifact, "detected_format", None) != ArtifactFormat.IMAGE.value:
        previous_offset = artifact.processed_bytes
        new_offset = max(previous_offset, last_record.end_offset)
        artifact.processed_bytes = new_offset
        artifact.last_processed_line = last_record.artifact_line_number
    db.commit()

    if not source_matching_succeeded:
        # A separate, short transaction - deliberately not part of the
        # fenced Evidence/checkpoint commit above (see that comment).  Two
        # concurrent artifact tasks each reaching this after their own
        # independent commit just each write the same value in their own
        # turn; neither holds a lock the other is waiting on, so this
        # cannot deadlock the way sharing one transaction would.
        analysis.source_status = "unavailable"
        analysis.source_failure_reason = (
            "Source matching became unavailable; diagnostic evidence was "
            "retained without source enrichment."
        )
        _source_index_process_cache.pop((analysis.id, generation), None)
        db.commit()

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
        matches_by_event = [
            correlate_event(event, source_index) for event in events
        ]
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


# Keyed by (analysis_id, processing_generation) - never analysis_id alone.
# A worker process can outlive one generation (recovery demotes a stale
# "processing" analysis back to "pending" and a later process_analysis
# call establishes a brand-new generation for the SAME analysis_id); keying
# on analysis_id alone would let that worker keep serving an old
# generation's SourceIndex object as if it belonged to the new one.
_source_index_process_cache: dict[tuple[int, int], object] = {}


def _invalidate_source_index_cache(analysis_id: int) -> None:
    """Drop every cached SourceIndex entry for this analysis_id regardless
    of which processing_generation it was built for - used at genuine
    terminal/discard points where no cached entry for this analysis, from
    any generation, may ever be reused again."""
    for key in [key for key in _source_index_process_cache if key[0] == analysis_id]:
        del _source_index_process_cache[key]


def _prepare_source_index(analysis: Analysis, generation: int):
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
        index = prepare_source(
            analysis.source_kind,
            analysis.source_reference,
            analysis.id,
        )
    except SourceInputError:
        raise
    except Exception as error:
        logger.exception(
            "Analysis %s | optional source acquisition/indexing failed",
            analysis.id,
        )
        raise SourceSubsystemError(
            "Optional source acquisition or indexing failed"
        ) from error

    # Deliberately its own try/except, OUTSIDE the block above: prepare_source()
    # already fully succeeded by this point (tree + index + manifest all
    # durably complete), so a staged-ZIP-unlink OSError here is a
    # housekeeping failure, not a source-availability failure - it must
    # never be converted into a SourceSubsystemError that would mark this
    # perfectly good source "unavailable".
    if analysis.source_kind == "zip":
        try:
            _remove_staged_source_archive(analysis.source_reference)
        except OSError:
            logger.warning(
                "Analysis %s | source preparation succeeded but the staged "
                "ZIP could not be removed; source remains ready",
                analysis.id,
                exc_info=True,
            )

    if len(_source_index_process_cache) >= SOURCE_INDEX_PROCESS_CACHE_MAX_ENTRIES:
        oldest_key = next(iter(_source_index_process_cache))
        del _source_index_process_cache[oldest_key]
    _source_index_process_cache[cache_key] = index

    return index


def _record_optional_source_failure(
    db: Session,
    analysis: Analysis,
    error: SourceInputError | SourceSubsystemError,
    *,
    generation: int,
) -> bool:
    """Durably degrade only the optional source subsystem.

    Returns False when cancellation, or a superseding generation, already
    won.  Database reads/commits are deliberately outside any
    source-specific catch so core persistence failures still propagate as
    analysis-wide failures.
    """
    current_status, current_generation = (
        db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis.id)
        .first()
    )
    if current_status != "processing" or current_generation != generation:
        logger.info(
            "Analysis %s | generation %s superseded (status=%s); ignoring "
            "source preparation failure",
            analysis.id,
            generation,
            current_status,
        )
        return False

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

    analysis.source_status = "unavailable"
    analysis.source_failure_reason = reason[:500]
    db.commit()
    _source_index_process_cache.pop((analysis.id, generation), None)

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
    """Persist one expected artifact-level failure without poisoning the
    investigation.

    Earlier parser batches may already have committed Evidence, so every
    Evidence row belonging to this artifact is removed before the artifact is
    made terminal. This is deliberately different from relationship_status
    "partially_linked": partially linked is valid, fully ingested evidence;
    this helper is only for an artifact whose ingestion itself did not finish.

    Authorized by the current durable Analysis state, not a stale read: a
    superseded execution (cancelled/failed/a new generation already won)
    must never report a controlled artifact failure for the generation it
    no longer owns.
    """
    current_status, current_generation = (
        db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis_id)
        .first()
    )
    if current_status != "processing" or current_generation != generation:
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
        .first()
    )
    if artifact is None:
        raise RuntimeError(
            f"Artifact {artifact_id} disappeared while recording controlled failure"
        )

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
    """Best-effort removal of one diagnostic artifact's raw staged bytes.
    Never the source of truth: AnalysisArtifact.status/failure_reason and
    persisted Evidence already fully describe the outcome on their own.
    Scoped strictly to files directly inside _UPLOAD_ROOT; anything else is
    silently left alone rather than deleted. A failure here is logged and
    must never propagate - see callers (_record_controlled_artifact_failure,
    _cleanup_completed_diagnostic_files), both already past their own
    durable commit by the time this runs."""
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
    """Best-effort removal of every "completed" diagnostic artifact's raw
    staged bytes for this analysis - called only AFTER the final durable
    investigation result (result_snapshot + status="completed") is already
    committed. From that point on, Evidence + result_snapshot are the sole
    source of truth: nothing in finalization, reconnect, or History ever
    re-reads a diagnostic artifact's saved_file_path (parsing, fallback
    capture, OCR, and source correlation all already ran, once, during this
    artifact's own ingestion pass). unsupported/duplicate artifacts already
    had their files removed at upload time; resource_limited/
    processing_error artifacts already had theirs removed by
    _record_controlled_artifact_failure - only "completed" remains here.

    Failures are logged and never raised: a failure to delete temporary
    files must never turn an already-successful, already-persisted
    investigation into a failed one. Also reclaims the prepared optional
    source tree (working tree, .ready marker, index manifest, and any
    staged source ZIP) the same way the failure/cancellation cleanup paths
    already do - gated on source_kind so a log-only analysis (the common
    case) never pays for a SOURCE_STORAGE_ROOT existence check it has no
    way of needing.
    """
    try:
        completed_paths = [
            row[0]
            for row in db.query(AnalysisArtifact.saved_file_path)
            .filter(
                AnalysisArtifact.analysis_id == analysis_id,
                AnalysisArtifact.status == "completed",
            )
            .all()
        ]
    except Exception:
        logger.warning(
            "Analysis %s | could not list completed diagnostic artifacts for cleanup",
            analysis_id,
            exc_info=True,
        )
        return

    for saved_file_path in completed_paths:
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
    """Durably cancel one analysis and best-effort reclaim everything
    generated for it. Called synchronously from the HTTP cancel endpoint
    (FastAPI, not Celery) - never depends on a free Celery worker slot:
    the tombstone commit below is the entire "cancellation
    happened" guarantee, independent of whether/when any in-flight worker
    ever observes it.

    The ordering below is the whole safety story here:
      1. an atomic conditional claim (UPDATE ... WHERE status IN
         (pending, processing)) establishes the durable tombstone
         (status="cancelled") - only a genuinely cancellable row is ever
         claimed, race-safe against a concurrent finalizer/failure commit;
      2. Evidence deletion, ai_analysis/result_snapshot clearing, and
         abandoned-artifact-checkpoint reset all happen in that SAME
         transaction as step 1's tombstone, so a crash between them cannot
         leave "cancelled" durably committed next to stale Evidence/result
         that a later crash-recovery pass would have to notice and repair -
         either the whole transaction commits, or none of it does;
      3. staged-file/source cleanup runs last, OUTSIDE that transaction,
         reusing the existing safe primitives
         (_cleanup_diagnostic_artifact_file, cleanup_prepared_source) -
         never a parallel cleanup system.
    Step 3's filesystem operations are best-effort: a failure there is
    logged and swallowed, never propagated, and never turns "cancelled"
    into anything else - the tombstone from step 1/2 already durably won,
    and no downstream code path here can revert it. A repeated call
    against an already-cancelled analysis (e.g. to retry filesystem
    cleanup) safely returns None at the claim step without re-running the
    DB cleanup, since idempotent filesystem primitives (missing_ok
    unlinks, exists-checked rmtree) make a second cleanup pass safe
    wherever it is invoked from.

    Returns the analysis's ORIGINAL status ("pending"/"processing") on a
    real transition, or None if the analysis does not exist or was not in
    a cancellable state (the caller - the API endpoint - is expected to
    have already produced the right HTTP response for "already cancelled"/
    "completed"/"failed"/not-found; this only guards the rare race where
    status changed between that read and this call).
    """
    previous_status = db.query(Analysis.status).filter(Analysis.id == analysis_id).scalar()
    if previous_status not in ("pending", "processing"):
        return None

    claim = db.execute(
        update(Analysis)
        .where(Analysis.id == analysis_id, Analysis.status.in_(("pending", "processing")))
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
    # Every artifact's generated fallback content (a bounded diagnostic/OCR
    # excerpt) is investigation-generated output, same as Evidence/result -
    # cleared for a cancelled investigation even for an artifact that had
    # already reached "completed" before the cancel request landed.
    db.query(AnalysisArtifact).filter(
        AnalysisArtifact.analysis_id == analysis_id,
    ).update({"fallback_context": None}, synchronize_session=False)
    # Only an artifact still pending/processing had its own in-flight
    # checkpoint abandoned mid-work - a terminal artifact's processed_bytes/
    # last_processed_line/failure_reason remain a truthful historical
    # record of how it actually finished and are never touched here.
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

    # 3: staged-file/source cleanup - best-effort, reusing the existing
    # safe primitives, never re-raised.
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
    """Atomically transition pending/processing -> failed, exactly once,
    authorized by the CURRENT durable status rather than a stale read -
    two concurrent callers (e.g. two artifact tasks failing around the
    same time) can never both "win" this, and neither can ever overwrite
    an already-committed cancellation or completion.

    When `generation` is given, the failure claim is additionally scoped to
    that processing_generation: a superseded worker from an OLD generation
    (still unwinding after an exception) must never fail a NEWER generation's
    in-flight work. `generation=None` (only used where no generation is in
    scope, e.g. before process_analysis's own claim can possibly succeed)
    skips that extra check.

    On a real transition, also clears partial generated state (Evidence,
    result_snapshot, ai_analysis, abandoned nonterminal artifact
    checkpoints, and every artifact's fallback_context - including already-
    completed artifacts, since a failed analysis never resumes and any
    previously generated excerpt is meaningless once the whole analysis is
    dead) in the SAME transaction. Returns True only if this call actually
    won the transition - callers use that to decide whether best-effort
    filesystem cleanup may run afterward. If the transition itself could not
    be durably committed (returns False, including on a DB error), raw
    staged files must be left alone: they may still be needed by
    stale-analysis recovery/checkpoint resume.
    """
    try:
        conditions = [Analysis.id == analysis_id, Analysis.status.in_(("pending", "processing"))]
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
    """Best-effort filesystem reclaim, called only after
    _mark_analysis_failed() reports a real, durably-committed transition -
    reuses the same safe primitives cancel_analysis_and_cleanup() does.
    Never raises: a failure here is logged and swallowed, and must never
    turn an already-durably-failed analysis into anything else."""
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
    """The final investigation_result payload for a completed analysis, for
    a client that reconnects/re-enters after the analysis already
    finished and missed (or never opened) the live SSE event.

    result_snapshot is the caller's already-loaded Analysis.result_snapshot
    - the exact bounded payload persisted BEFORE it was published (see
    _finalize_analysis_task's persist-before-publish ordering). When
    present it is returned as-is: no correlation is rerun and no Gemini
    call is made, and - critically for History - a later change to
    correlation/scoring logic can never silently alter what a historical
    analysis is shown to have concluded (result immutability).

    result_snapshot is only None for analyses finalized before that column
    existed, in which case this falls back to recomputing the payload from
    persisted Evidence/AnalysisArtifact rows (the legacy compatibility
    path) and reattaching ai_analysis (the caller's already-loaded
    Analysis.ai_analysis) exactly as before - never a second Gemini call
    either way, just recomputed on demand rather than read from a stored
    blob.
    """
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
        if artifact.fallback_context and artifact.id not in legacy_evidence_counts_by_artifact
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
    """What the frontend needs to render immediately on page load/SSE
    (re)connect, derived only from already-persisted state - Analysis.
    status and AnalysisArtifact.processed_bytes/size_bytes/status (the
    same checkpoint columns ingestion already maintains). No Redis, no
    in-memory global, no second progress-tracking system. Uses the exact
    same percentage formula/caps as the live SSE stream
    (_ingestion_percentage) and the exact same "past ingestion, into
    identity/timeline/correlation" semantics that already publish 99 live.
    """
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
        row.status in ("unsupported", "duplicate", "completed", "resource_limited", "processing_error")
        for row in rows
    )

    if ingestion_done:
        progress = 99
    else:
        dispatchable = [
            row for row in rows
            if row.status not in ("unsupported", "duplicate", "resource_limited", "processing_error")
        ]
        total_bytes = sum(row.size_bytes for row in dispatchable)
        processed_bytes = sum(row.processed_bytes for row in dispatchable)
        progress = _ingestion_percentage(processed_bytes, total_bytes) if total_bytes else 0

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
        if artifact.status in (
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
    filename_by_artifact_id = {artifact.id: artifact.original_filename for artifact in artifacts}

    return [
        build_artifact_outcome_payload(
            artifact,
            evidence_counts.get(artifact.id, 0),
            filename_by_artifact_id,
        )
        for artifact in terminal
    ]


# --- Stale/orphan recovery ----------------------------------------------
#
# Conservative on purpose: this must never fire during a normal single-
# stage pause (large parsing - refreshed every persisted batch, see
# _process_artifact's _bump_processing_heartbeat call -, one image's OCR,
# a GitHub clone bounded by GITHUB_CLONE_TIMEOUT_SECONDS (60s) plus bounded
# ZIP extraction/indexing, or a Gemini call/retry - refreshed once it
# resolves, see _finalize_analysis_task's post-Gemini heartbeat calls,
# since the google-genai client has no configured request timeout) and
# must never duplicate a perfectly healthy in-flight workflow. 300 seconds
# (5 minutes) is comfortably above every one of those individual stage
# durations - each is either itself bounded well under a minute, or gets
# its own heartbeat refresh once it resolves - while still being short
# enough that a genuinely-orphaned analysis (worker killed, Redis/broker
# work lost, machine interrupted) does not sit unrecoverable indefinitely.
_STALE_ANALYSIS_THRESHOLD_SECONDS = 300

# Pending is fundamentally different from processing and must never share
# the same 300-second window: a "pending" Analysis simply means the durable
# row exists but its top-level process_analysis task may still be healthy
# and normally waiting in the broker - worker_concurrency is intentionally
# small (2), so multiple legitimate investigations can sit queued behind
# an earlier, large one for well beyond 5 minutes. Treating queue backlog
# as orphaned caused real duplicate dispatch/duplicate Gemini calls in
# production (a healthy queued process_analysis plus a second, falsely
# "recovered" copy both eventually ran). A pending row therefore gets a
# much more conservative grace window before recovery ever redispatches
# it, referenced against COALESCE(processing_heartbeat_at, created_at):
# a never-claimed pending row has no heartbeat yet, so its own queue-entry
# time (created_at) is the right age reference; once recovery does claim
# one, the fresh processing_heartbeat_at it writes becomes the new
# reference so an immediately-following Beat tick can't reclaim it again.
_PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS = 30 * 60

# Bounded per scan tick - recovery redispatches at most this many stale
# analyses of EACH kind per Beat firing, so even a large backlog after an
# extended outage cannot flood the worker pool or the DB in one tick; the
# next scheduled tick picks up whatever remains.
_RECOVERY_SCAN_BATCH_LIMIT = 25


def _has_active_processing_artifact(analysis_id_column):
    """True when some artifact of this analysis is actually mid-stream
    right now. This - not merely status="processing" on the parent row -
    is what "a stage has actually started and should be heartbeating"
    means: process_analysis flips the parent to "processing" the instant
    its own claim wins, which can be well before any child task actually
    gets a worker slot under worker_concurrency=2."""
    return analysis_id_column.in_(
        select(AnalysisArtifact.analysis_id).where(AnalysisArtifact.status == "processing")
    )


def _claim_stale_pending(db: Session, stale_filter, claimed_at: datetime) -> list[int]:
    """Select-then-atomically-claim a stale PENDING analysis: only bumps
    processing_heartbeat_at (the fresh dispatch-activity timestamp
    recover_stale_analyses's own redispatch produces), leaving status
    "pending" so process_analysis's own atomic pending->processing claim
    is what actually starts a (freshly generation-numbered) workflow. The
    exact same `stale_filter` used to select candidates is re-applied
    inside each candidate's conditional UPDATE, so a second concurrent
    scan (or this same task overlapping its own next Beat tick) can never
    also claim a row this call already claimed."""
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


def _claim_and_demote_stale_processing(db: Session, stale_filter, claimed_at: datetime) -> list[int]:
    """Select-then-atomically-claim a stale PROCESSING analysis. Unlike the
    pending case, this must fence the old execution BEFORE any replacement
    workflow can start: the claim demotes status back to "pending" (so a
    zombie task from the old execution fails every "status=='processing'"
    fence it checks - it can never again persist Evidence, bump a
    checkpoint, or mark anything terminal) and clears any finalizer claim
    for the generation being abandoned. Any artifact stuck "processing"
    for this analysis is also reset to "pending" IN THE SAME transaction -
    its last COMMITTED processed_bytes/last_processed_line checkpoint is
    left untouched (a truthful resume point), only the in-flight claim
    itself is released, so a future process_analysis (which will assign a
    brand-new processing_generation) can re-claim and resume it. Terminal
    artifacts (completed/unsupported/duplicate/resource_limited/
    processing_error) are never touched.

    The same `stale_filter` used for candidate selection is re-applied in
    the claim's WHERE clause, so two overlapping scans can never both
    demote (and therefore never both trigger a redispatch for) the same
    analysis."""
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
            .values(
                status="pending",
                processing_heartbeat_at=claimed_at,
                finalization_generation=None,
            )
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
        # IMAGE (OCR) artifacts cannot resume from a byte/line checkpoint:
        # the offsets recorded during OCR text reconstruction are logical
        # line positions within the extracted text, not byte offsets into
        # the encoded image, so replaying OCR from any partial state would
        # re-run the WHOLE extraction anyway and, if the previously
        # committed partial Evidence were left in place, double-count
        # occurrence_count on every event already persisted before the
        # crash. A reclaimed IMAGE artifact is therefore reset to a clean
        # restart: partial Evidence deleted, checkpoint and any OCR
        # fallback excerpt cleared - the raw image file itself is left
        # untouched so the next attempt can OCR it again from scratch.
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
        claimed_ids.append(analysis_id)
    return claimed_ids


@celery_app.task
def recover_stale_analyses() -> int:
    """Celery Beat periodic task (see celery_app.py's beat_schedule) - the
    ONLY place Devflo ever redispatches a pending/processing analysis after
    an unexpected interruption. Deliberately NOT run on FastAPI startup
    and NOT triggered by frontend page load: either would risk
    duplicating a perfectly healthy in-flight Celery workflow on a plain
    API-process restart, which has nothing to do with whether the actual
    worker/broker work is still alive.

    THREE independent notions of "stale", never conflated:

    PROCESSING + actually active work (some artifact of this analysis is
    itself "processing", or a finalizer has been durably claimed for the
    current processing_generation) - a stage has genuinely started and
    should be heartbeating: stale after
    _STALE_ANALYSIS_THRESHOLD_SECONDS (300s) with no heartbeat refresh.

    PROCESSING + no active work yet (the parent flipped to "processing"
    the instant its own claim won, but child dispatch may simply be
    sitting queued behind other work under worker_concurrency=2 - source
    preparation with no continuous heartbeat of its own falls in here
    too) - stale only after the much more conservative
    _PENDING_ANALYSIS_RECOVERY_THRESHOLD_SECONDS (30 minutes), referenced
    against processing_heartbeat_at (set once, exactly when the
    pending->processing claim won).

    PENDING (process_analysis has not even claimed it yet - normal broker
    backlog) - stale after the same 30-minute window, referenced against
    COALESCE(processing_heartbeat_at, created_at): a never-claimed row has
    no heartbeat yet, so its own queue-entry time is the right age
    reference; once recovery claims one, the fresh processing_heartbeat_at
    it writes becomes the new reference so an immediately-following Beat
    tick can't reclaim it again.

    cancelled/completed/failed analyses are never candidates for any of
    these (excluded by the status filter itself).

    Each notion gets its own select-then-atomic-claim pass using the
    identical predicate for both the candidate SELECT and the conditional
    claim UPDATE, so two overlapping scans can never double-claim the same
    row and at most one logical process_analysis() redispatch happens per
    orphaned analysis per genuinely-stale window. A reclaimed PROCESSING
    analysis is first atomically demoted back to "pending" (see
    _claim_and_demote_stale_processing) so the ordinary pending->processing
    claim in process_analysis is what establishes its fresh execution
    generation - never a direct in-place resume of the old one.
    """
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
            func.coalesce(
                Analysis.processing_heartbeat_at, Analysis.created_at
            ) < queue_cutoff,
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

    return len(processing_fast_claimed) + len(processing_queue_claimed) + len(pending_claimed)
