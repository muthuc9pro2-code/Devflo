import logging
from os.path import getsize
from pathlib import Path
from time import perf_counter
from time import time as wall_time
from celery import chain, chord, group
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.core.celery_app import celery_app
from app.db.database import sessionLocal
from app.models import Analysis, AnalysisArtifact, Evidence
from app.services import (
    build_exception_fingerprint,
    create_batches,
    persist_evidence_batch,
    persist_resolved_identities,
    process_persisted_timelines,
)
from app.services.artifact_detector import ArtifactFormat, detect_artifact
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.source_archive import prepare_source
from app.services.source_index import correlate_event
from app.services.investigation_router import choose_investigation_path, InvestigationPath
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import (
    build_correlation_payload,
    build_llm_context,
)
from app.services.analysis_events import (
    publish_correlation_result,
    publish_progress,
)

logger = logging.getLogger(__name__)

_IMPORTANT_LEVELS = frozenset({'WARNING', 'WARN', 'ERROR', 'CRITICAL'})

# ParsedEvent.line_number ("global_line_number" below) is internal
# bookkeeping only - never returned by any schema/API/frontend, and
# correlation_engine.py orders strictly by evidence.first_seen (a real
# timestamp), never by line number (confirmed by inspection). It used to be
# a true running total accumulated sequentially across artifacts in
# position order, which is meaningless once artifacts run concurrently (an
# artifact can't know "how many lines came before it" from siblings that
# haven't finished). Each artifact instead gets a fixed, deterministic band
# keyed only by its own position, reproducible regardless of completion
# order. 10**9 leaves generous headroom below any real artifact's line
# count before the next artifact's band begins.
_GLOBAL_LINE_NUMBER_STRIDE = 10**9

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

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            logger.warning("Analysis %s not found", analysis_id)
            return

        if analysis.status == "completed":
            logger.info("Analysis %s is already completed", analysis_id)
            return

        analysis.status = "processing"
        db.commit()

        publish_progress(
            analysis_id,
            "ingestion",
            "Diagnostic ingestion started",
            progress=0,
        )

        artifact_ids = [
            row.id
            for row in (
                db.query(AnalysisArtifact)
                .filter(AnalysisArtifact.analysis_id == analysis_id)
                .order_by(AnalysisArtifact.position, AnalysisArtifact.id)
                .all()
            )
        ]

        if not artifact_ids:
            raise RuntimeError(
                f"Analysis {analysis_id} has no persisted diagnostic artifacts"
            )

        needs_source_prep = bool(analysis.source_kind)
    except Exception:
        db.rollback()
        logger.exception("Analysis %s processing failed", analysis_id)
        _mark_analysis_failed(db, analysis_id)
        raise
    finally:
        db.close()

    dispatch_start = wall_time()
    artifact_group = group(
        _process_artifact_task.si(analysis_id, artifact_id)
        for artifact_id in artifact_ids
    )
    workflow = chord(artifact_group, _finalize_analysis_task.s(analysis_id, dispatch_start))
    if needs_source_prep:
        workflow = chain(_prepare_source_task.si(analysis_id), workflow)

    workflow.apply_async()

    logger.info(
        "Analysis %s | dispatched %s artifact task(s)%s (worker_concurrency=%s)",
        analysis_id,
        len(artifact_ids),
        " after source prep" if needs_source_prep else "",
        celery_app.conf.worker_concurrency,
    )


@celery_app.task
def _process_artifact_task(analysis_id: int, artifact_id: int) -> int:
    """Process exactly one artifact. Independent unit of work: its own DB
    session, only ever reads/writes this artifact's own AnalysisArtifact row
    and inserts evidence scoped to its own artifact_id, so it is safe to run
    concurrently with any other artifact task (same analysis or a different
    one) up to celery_app's worker_concurrency bound.
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

        if artifact.status == "completed":
            return 0

        source_index = _prepare_source_index(analysis)

        # See _GLOBAL_LINE_NUMBER_STRIDE above: deterministic per-position
        # band instead of a cross-artifact running total, since concurrent
        # artifacts have no well-defined "how many lines came before them".
        analysis.last_processed_line = artifact.position * _GLOBAL_LINE_NUMBER_STRIDE

        return _process_artifact(
            db=db,
            analysis=analysis,
            artifact=artifact,
            source_index=source_index,
        )
    except Exception:
        db.rollback()
        logger.exception(
            "Analysis %s | artifact %s processing failed",
            analysis_id,
            artifact_id,
        )
        _mark_analysis_failed(db, analysis_id)
        raise
    finally:
        db.close()


@celery_app.task
def _prepare_source_task(analysis_id: int) -> None:
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

        source_prep_start = perf_counter()
        _prepare_source_index(analysis)
        logger.info(
            "Analysis %s | source prep (%s) completed in %.2fs",
            analysis_id,
            analysis.source_kind,
            perf_counter() - source_prep_start,
        )
    except Exception:
        db.rollback()
        logger.exception("Analysis %s | source preparation failed", analysis_id)
        _mark_analysis_failed(db, analysis_id)
        raise
    finally:
        db.close()


@celery_app.task
def _finalize_analysis_task(results, analysis_id: int, dispatch_start: float | None = None) -> None:
    """Chord callback for process_analysis: Celery only invokes this after
    every task in the artifact group has completed successfully. As a
    second, independent safeguard against relying on chord-callback edge
    cases alone, this also explicitly re-checks that every artifact for
    this analysis is actually "completed" and that the analysis was not
    separately marked "failed" before doing any identity/timeline/
    correlation work.
    """
    db = sessionLocal(expire_on_commit=False)
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if analysis is None:
            logger.warning("Analysis %s not found at finalize time", analysis_id)
            return

        if analysis.status == "failed":
            logger.info(
                "Analysis %s | already marked failed; skipping finalize",
                analysis_id,
            )
            return

        incomplete = (
            db.query(AnalysisArtifact)
            .filter(
                AnalysisArtifact.analysis_id == analysis_id,
                AnalysisArtifact.status != "completed",
            )
            .first()
        )
        if incomplete is not None:
            logger.warning(
                "Analysis %s | artifact %s not completed (status=%s); skipping finalize",
                analysis_id,
                incomplete.id,
                incomplete.status,
            )
            return

        total_start = perf_counter()
        parsed_event_count = sum(results) if results else 0

        def _true_total_seconds() -> float:
            # dispatch_start is a wall-clock timestamp taken in
            # process_analysis before anything was dispatched, so this
            # reflects real end-to-end time (source prep + concurrent
            # artifact processing + this finalize task), not just this
            # task's own perf_counter span. Falls back to finalize-local
            # timing only if this task was ever invoked without it.
            if dispatch_start is not None:
                return wall_time() - dispatch_start
            return perf_counter() - total_start

        position_and_lines = (
            db.query(AnalysisArtifact.position, AnalysisArtifact.last_processed_line)
            .filter(AnalysisArtifact.analysis_id == analysis_id)
            .all()
        )
        artifact_count = len(position_and_lines)
        # AnalysisArtifact.last_processed_line is always the artifact's own
        # real local line count (ArtifactEvent.artifact_line_number) - never
        # stride-banded, unlike Analysis.last_processed_line below, which is
        # an internal, never-externally-exposed aggregate used only to keep
        # per-artifact global_line_number bands distinct (see
        # _GLOBAL_LINE_NUMBER_STRIDE). Summing the real per-artifact counts
        # here is what actually answers "how many lines were processed".
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

        if evidence_count == 0:
            analysis.status = "completed"
            db.commit()

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

            return

        investigation_path = choose_investigation_path(
            db=db,
            analysis_id=analysis_id,
        )
        logger.info(
            "Analysis %s | investigation_path=%s",
            analysis_id,
            investigation_path.value,
        )
        if investigation_path == InvestigationPath.CORRELATED:
            identity_start = perf_counter()
            persist_resolved_identities(db=db, analysis_id=analysis_id)
            logger.info("Analysis %s | evidence identities resolved", analysis_id)
            logger.info(
                "Analysis %s | identity resolution completed in %.2fs",
                analysis_id,
                perf_counter() - identity_start,
            )

            publish_progress(
                analysis_id,
                "identity",
                "Evidence identity resolution completed",
                progress=99,
            )

            timeline_start = perf_counter()
            process_persisted_timelines(db=db, analysis_id=analysis_id)
            logger.info("Analysis %s | persisted timelines processed", analysis_id)
            logger.info(
                "Analysis %s | timeline processing completed in %.2fs",
                analysis_id,
                perf_counter() - timeline_start,
            )
            publish_progress(
                analysis_id,
                "timeline",
                "Timeline reconstruction completed",
                progress=99,
            )

            publish_progress(
                analysis_id,
                "correlation",
                "Correlation analysis started",
                progress=99,
            )

            correlation_start = perf_counter()

            evidence_rows = (
                db.query(Evidence)
                .filter(Evidence.analysis_id == analysis_id)
                .order_by(Evidence.first_seen, Evidence.id)
                .all()
            )

            correlation_run = run_correlation(
                analysis_id=analysis_id,
                evidence_rows=evidence_rows,
            )

            correlation_payload = build_correlation_payload(
                correlation_run,
                evidence_rows,
            )

            llm_context = build_llm_context(
                correlation_run,
                evidence_rows,
            )

            publish_correlation_result(
                analysis_id,
                correlation_payload,
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

        analysis.status = "completed"
        db.commit()
        logger.info(
            "Analysis %s | TOTAL processing time %.2fs",
            analysis_id,
            _true_total_seconds(),
        )
    except Exception:
        db.rollback()
        logger.exception("Analysis %s finalize processing failed", analysis_id)
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
    last_published_progress = -1

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
        last_published_progress = _publish_ingestion_progress(
            db=db,
            analysis_id=analysis.id,
            last_published=last_published_progress,
        )

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
            .filter(AnalysisArtifact.analysis_id == analysis_id)
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

    percentage = min(98, max(0, int((processed_bytes or 0) * 100 // total_bytes)))

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
    batch,
    source_index=None,
) -> int:

    important_events = []
    important_append = important_events.append

    for record in batch:
        event = record.event

        if event is None:
            continue

        event.artifact_id = artifact.id

        if event.level in _IMPORTANT_LEVELS or (
            event.source_format == "opentelemetry"
            and (event.trace_id is not None or event.span_id is not None)
        ):
            important_append(event)
        
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

    if logger.isEnabledFor(logging.DEBUG):
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

