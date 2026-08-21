import logging
from os.path import getsize
from pathlib import Path
from time import perf_counter
from time import time as wall_time
from celery import chain, chord, group
from sqlalchemy import func
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
    stream_artifact_events,
    stream_image_events_from_text,
)
from app.services.fallback_context import (
    capture_ocr_fallback_context,
    capture_text_fallback_context,
)
from app.services.image_text_extractor import extract_text_from_image_with_confidence
from app.services.source_archive import prepare_source
from app.services.source_index import correlate_event
from app.services.investigation_router import choose_investigation_path, InvestigationPath
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import (
    build_artifact_outcome_payload,
    build_correlation_payload,
    build_fallback_llm_context,
    build_fallback_payload,
    build_llm_context,
    build_simple_llm_context,
    build_simple_payload,
    build_zero_evidence_payload,
    select_bounded_evidence_from_db,
    select_evidence_counts_by_artifact,
)
from app.services.analysis_events import (
    publish_artifact_outcome,
    publish_investigation_result,
    publish_progress,
)
from app.services.gemini_service import (
    GeminiUnavailableError,
    generate_investigation_explanation,
)


logger = logging.getLogger(__name__)

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
                .filter(
                    AnalysisArtifact.analysis_id == analysis_id,
                    # Unsupported/duplicate artifacts are already fully
                    # resolved at upload time (create_analysis) - they are
                    # never dispatched for ingestion and never produce their
                    # own Evidence set.
                    AnalysisArtifact.status.notin_(["unsupported", "duplicate"]),
                )
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
        # A plain local variable (Section 5) - not analysis.last_processed_line
        # - so this never dirties the shared Analysis row: concurrent
        # artifact tasks previously all wrote to that one row on every
        # batch commit (see _persist_artifact_batch), causing unnecessary
        # row-lock contention between them. Analysis.processed_bytes/
        # last_processed_line are recomputed once, from the authoritative
        # per-artifact rows, at _finalize_analysis_task.
        global_line_number = artifact.position * _GLOBAL_LINE_NUMBER_STRIDE

        return _process_artifact(
            db=db,
            analysis=analysis,
            artifact=artifact,
            source_index=source_index,
            global_line_number=global_line_number,
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
                # "unsupported"/"duplicate" are terminal, already-resolved
                # states set at upload time (create_analysis) - they are
                # never dispatched and so never reach "completed", but they
                # must not block finalize either.
                AnalysisArtifact.status.notin_(
                    ["completed", "unsupported", "duplicate"]
                ),
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
            # Every artifact reaching finalize is "completed", and since the
            # WHOLE analysis retained zero evidence, every one of them is
            # necessarily a zero-evidence artifact - one bounded query,
            # reused both for the artifacts[] outcome list and to check for
            # a captured Sections 9-12 fallback context (never a second
            # read/re-OCR - fallback_context was already captured during
            # each artifact's own ingestion pass in _process_artifact).
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
                try:
                    gemini_result = generate_investigation_explanation(fallback_llm_context)
                    fallback_payload["ai_analysis"] = gemini_result.model_dump()
                    # Persisted only after a valid Gemini schema result (see
                    # the CORRELATED/SIMPLE branches below) so reconnect can
                    # reattach it without a second Gemini call.
                    analysis.ai_analysis = fallback_payload["ai_analysis"]
                except GeminiUnavailableError:
                    # Gemini is only the explanation layer - a temporarily
                    # unavailable explanation must not fail this otherwise
                    # complete deterministic result (see the CORRELATED
                    # branch below for the full rationale). fallback_payload
                    # keeps no "ai_analysis" key, same as the zero-evidence
                    # payload below when there is nothing to explain.
                    logger.warning(
                        "Analysis %s | Gemini explanation unavailable; "
                        "completing without an explanation",
                        analysis_id,
                    )
                    analysis.ai_analysis = None
                # Persist-before-publish (locked requirement) - see the
                # CORRELATED branch below for the full rationale.
                analysis.result_snapshot = fallback_payload
                analysis.status = "completed"
                db.commit()

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
            # Persist-before-publish (locked requirement) - see the
            # CORRELATED branch below for the full rationale.
            analysis.result_snapshot = zero_evidence_payload
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

            publish_investigation_result(analysis_id, zero_evidence_payload)

            return

        # Section 4 (final hardening pass): persist identities once, then
        # select ONE bounded working Evidence set from MySQL - used
        # identically for routing, correlation (CORRELATED), and the
        # frontend/Gemini payload (SIMPLE), so route/graph/payload can
        # never silently disagree about which Evidence this investigation
        # actually covers. Never a second, unbounded Evidence .all() in
        # either branch below. Below CORRELATED_MAX_EVIDENCE_RECORDS
        # (== SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS) this is exactly one
        # query, identical to before. Evidence itself remains fully
        # persisted in MySQL either way; this only bounds what one
        # investigation response has to build and transmit.
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

        # REAL per-artifact Evidence counts for the WHOLE analysis (Section
        # 4 hardening) - the bounded working set above may not include
        # every row from every artifact, so "how much Evidence does this
        # artifact really have" (and, in each branch below, "does this
        # artifact have any structured Evidence at all") must never be
        # decided from that working subset alone. Bounded by
        # MAX_DIAGNOSTIC_ARTIFACTS, so always safe to materialize.
        evidence_counts_by_artifact = select_evidence_counts_by_artifact(
            db, analysis_id=analysis_id
        )

        investigation_path = choose_investigation_path(evidence_rows)
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
            )

            # Per-artifact outcome for the frontend (including artifacts that
            # were fully processed but retained zero evidence, e.g. an
            # unrelated-looking log with no diagnostic signal) - a single
            # bounded query over this analysis's own artifacts, not repeated
            # per artifact/batch. evidence_count per artifact is derived from
            # evidence_rows already loaded above, not a second evidence query.
            artifact_outcomes = (
                db.query(
                    AnalysisArtifact.id,
                    AnalysisArtifact.original_filename,
                    AnalysisArtifact.detected_format,
                    AnalysisArtifact.status,
                    AnalysisArtifact.duplicate_of_artifact_id,
                    AnalysisArtifact.fallback_context,
                )
                .filter(AnalysisArtifact.analysis_id == analysis_id)
                .all()
            )

            # Section 2: an artifact that retained zero structured Evidence
            # of its own but did capture useful fallback text/OCR content
            # during its own ingestion pass (never a second read/OCR) must
            # not silently disappear just because OTHER artifacts in this
            # same analysis have real structured Evidence - it becomes
            # bounded, clearly-non-causal supplemental context instead.
            # Section 4 hardening: membership comes from the REAL per-
            # artifact map, not the bounded evidence_rows working set - an
            # artifact whose Evidence exists in MySQL but did not survive
            # the bounded selection must never be mislabeled as
            # zero-structured-evidence supplemental context.
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

            # Small correlation-stage completion signal, published before
            # the Gemini call below (which can take a few seconds) so a
            # live client sees "deterministic correlation is done" without
            # waiting on the explanation layer - and, critically, BEFORE
            # the authoritative final result, not after it (a progress
            # event following the real final payload would misleadingly
            # read as "still working").
            publish_progress(
                analysis_id,
                "correlation",
                "Deterministic correlation completed",
                progress=99,
            )

            # artifacts= gives Gemini the same "nginx.log produced 8
            # evidence rows" provenance summary as the frontend payload,
            # without dumping raw lines - the deterministic engine remains
            # the sole authority on correlation/root-cause, this context
            # only adds explanatory provenance around it.
            llm_context = build_llm_context(
                correlation_run,
                evidence_rows,
                artifacts=artifact_outcomes,
                supplemental_artifacts=supplemental_artifacts,
            )

            try:
                gemini_result = generate_investigation_explanation(llm_context)
                correlation_payload["ai_analysis"] = gemini_result.model_dump()
                # Persisted only now that Gemini has returned a valid schema
                # result - lets a client reconnecting after completion see
                # the exact same explanation (reconstruct_current_
                # investigation_result attaches this same field), without a
                # second Gemini call.
                analysis.ai_analysis = correlation_payload["ai_analysis"]
            except GeminiUnavailableError:
                # Gemini is only the explanation layer sitting on top of an
                # already-complete deterministic investigation (graph,
                # timeline, roles, artifact outcomes, source matches are all
                # already computed above). Its temporary unavailability must
                # not turn a successful deterministic result into a failed
                # analysis - complete without an explanation instead;
                # correlation_payload keeps no "ai_analysis" key, exactly
                # like the zero-evidence payload above when there is
                # nothing to explain.
                logger.warning(
                    "Analysis %s | Gemini explanation unavailable; "
                    "completing without an explanation",
                    analysis_id,
                )
                analysis.ai_analysis = None
            # Persist-before-publish (locked requirement): the exact same
            # bounded payload about to be published as investigation_result
            # is committed to the DB FIRST. If the live SSE event is lost
            # immediately afterward (client disconnected, process crashed),
            # History/reconnect must already have the authoritative result
            # rather than racing a result that only ever went out over the
            # wire.
            analysis.result_snapshot = correlation_payload
            analysis.status = "completed"
            db.commit()
            logger.info(
                "Analysis %s | TOTAL processing time %.2fs",
                analysis_id,
                _true_total_seconds(),
            )

            # investigation_result is the single authoritative full final
            # payload for this analysis, published exactly once. There is
            # no separate correlation_result event: the previous "publish
            # both" contract transmitted the entire node/edge/evidence
            # graph over SSE twice for the same finished computation - it
            # never ran correlation twice, only sent the result twice. No
            # frontend code in this repository consumes correlation_result
            # (verified directly, not assumed).
            publish_investigation_result(
                analysis_id,
                correlation_payload,
            )
        else:
            # Reuses the SAME bounded working set selected above (before
            # routing) - never a second, unbounded Evidence .all() here.
            simple_artifacts = (
                db.query(
                    AnalysisArtifact.id,
                    AnalysisArtifact.original_filename,
                    AnalysisArtifact.detected_format,
                    AnalysisArtifact.status,
                    AnalysisArtifact.duplicate_of_artifact_id,
                    AnalysisArtifact.fallback_context,
                )
                .filter(AnalysisArtifact.analysis_id == analysis_id)
                .all()
            )

            # Section 2: same low-structure-survives-a-mixed-investigation
            # treatment as the CORRELATED branch above - membership from the
            # REAL per-artifact map (Section 4 hardening), not the bounded
            # evidence_rows working set.
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

            # Prepared for the Gemini integration to be wired in next -
            # mirrors the CORRELATED branch's llm_context above; no Gemini
            # SDK/API call exists in this codebase yet.
            simple_llm_context = build_simple_llm_context(
                analysis_id,
                evidence_rows,
                artifacts=simple_artifacts,
                supplemental_artifacts=simple_supplemental_artifacts,
            )

            try:
                gemini_result = generate_investigation_explanation(simple_llm_context)
                simple_payload["ai_analysis"] = gemini_result.model_dump()
                # See the CORRELATED branch above: persisted only after a
                # valid Gemini schema result, so reconnect can reattach it
                # without a second Gemini call.
                analysis.ai_analysis = simple_payload["ai_analysis"]
            except GeminiUnavailableError:
                # See the CORRELATED branch above: a temporarily unavailable
                # explanation must not fail this otherwise complete
                # deterministic result.
                logger.warning(
                    "Analysis %s | Gemini explanation unavailable; "
                    "completing without an explanation",
                    analysis_id,
                )
                analysis.ai_analysis = None
            # Persist-before-publish (locked requirement) - see the
            # CORRELATED branch above for the full rationale.
            analysis.result_snapshot = simple_payload
            analysis.status = "completed"
            db.commit()
            logger.info(
                "Analysis %s | TOTAL processing time %.2fs",
                analysis_id,
                _true_total_seconds(),
            )

            # investigation_result is the single authoritative full final
            # payload, published exactly once - no progress event follows
            # it (see the CORRELATED branch above and the SSE contract:
            # investigation_result must be the final meaningful event, not
            # followed by a progress message that would misleadingly read
            # as "still working" after the real result already arrived).
            publish_investigation_result(
                analysis_id,
                simple_payload,
            )

    except Exception:
        db.rollback()
        logger.exception("Analysis %s finalize processing failed", analysis_id)
        _mark_analysis_failed(db, analysis_id)
        raise
    finally:
        db.close()


def _capture_small_text_artifact_fallback(saved_file_path: str) -> dict | None:
    """A bounded (<= SIMPLE_FALLBACK_MAX_TEXT_BYTES) prefix read of an
    already-confirmed-small (<= SIMPLE_FALLBACK_MAX_ARTIFACT_BYTES, a few
    MiB at most) text artifact - not the "reopen a 1 GiB artifact" cost
    Section 9 warns against, and never a second pass over the artifact's
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
    source_index=None,
    global_line_number: int | None = None,
) -> int:
    # Section 5: an explicit local starting line number rather than reading
    # analysis.last_processed_line - the caller (_process_artifact_task) no
    # longer maintains that shared-row attribute at all. Falls back to
    # reading it only for callers that still seed it directly (e.g. legacy/
    # test call sites constructing a lightweight `analysis` object), never
    # itself writes it back.
    if global_line_number is None:
        global_line_number = getattr(analysis, "last_processed_line", 0)

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
    last_published_progress = -1

    if artifact_format == ArtifactFormat.IMAGE:
        # OCR still runs exactly once per image: this SAME extraction
        # feeds both the fallback context (Sections 9-12) and evidence
        # reconstruction below - stream_image_events_from_text() never
        # calls RapidOCR itself, unlike stream_artifact_events()'s normal
        # IMAGE dispatch, which is why this bypasses it here.
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
        parsed_count += _persist_artifact_batch(
            db=db,
            analysis=analysis,
            artifact=artifact,
            batch=batch,
            source_index=source_index,
        )
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

    # Section 5: only this artifact's own row is updated here - the shared
    # Analysis.processed_bytes/last_processed_line are never mutated during
    # per-artifact/per-batch processing (see _persist_artifact_batch),
    # only recomputed once from AnalysisArtifact rows at
    # _finalize_analysis_task.
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

    # This artifact's own ingestion is now fully done, so its retained
    # evidence count is deterministically known - one bounded query, once,
    # scoped to this single artifact (not per-batch). Only the zero-evidence
    # outcome is published live here; a successful outcome still has to go
    # through correlation before it means anything to the frontend, so it
    # is only reported in the final investigation_result.
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
                # Unsupported/duplicate artifacts are never dispatched for
                # ingestion and their size_bytes would otherwise inflate the
                # denominator with bytes that will never be processed,
                # permanently understating progress for the rest of the
                # analysis (e.g. one skipped 900MB artifact alongside a real
                # 10MB one would cap visible progress near 1%).
                AnalysisArtifact.status.notin_(["unsupported", "duplicate"]),
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

        if is_evidence_worthy(event):
            important_append(event)
        
    _correlate_source_events(important_events, source_index)
    _assign_batch_fingerprints(important_events)
    persist_evidence_batch(
        db=db,
        analysis_id=analysis.id,
        events=important_events,
        artifact_id=artifact.id,
    )

    # Section 5: only this artifact's own row is dirtied here (never the
    # shared Analysis row) - concurrent artifact tasks each commit their
    # own row without contending for a lock on one shared row per batch.
    # Analysis.processed_bytes/last_processed_line are recomputed once
    # from AnalysisArtifact rows at _finalize_analysis_task instead.
    last_record = batch[-1]
    previous_offset = artifact.processed_bytes
    new_offset = max(previous_offset, last_record.end_offset)
    artifact.processed_bytes = new_offset
    artifact.last_processed_line = last_record.artifact_line_number
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


# Section 6: a small, bounded, process-LOCAL cache of already-built
# SourceIndex objects, keyed by analysis_id. This is a pure optimization,
# never the sole correctness mechanism - Celery workers are separate OS
# processes with no shared memory, so a worker handling several artifacts
# from the SAME analysis sequentially skips even reading/parsing the
# persisted manifest (see source_archive.prepare_source) for every
# artifact after the first, while a DIFFERENT worker process still
# correctly reconstructs the same index from that manifest. Bounded
# (simple FIFO eviction) so a long-lived worker that has touched many
# different analyses cannot grow this unboundedly.
_source_index_process_cache: dict[int, object] = {}


def _prepare_source_index(analysis: Analysis):
    if not analysis.source_kind or not analysis.source_reference:
        return None

    cached = _source_index_process_cache.get(analysis.id)
    if cached is not None:
        return cached

    index = prepare_source(
        analysis.source_kind,
        analysis.source_reference,
        analysis.id,
    )
    if analysis.source_kind == "zip":
        _remove_staged_source_archive(analysis.source_reference)

    if len(_source_index_process_cache) >= SOURCE_INDEX_PROCESS_CACHE_MAX_ENTRIES:
        oldest_analysis_id = next(iter(_source_index_process_cache))
        del _source_index_process_cache[oldest_analysis_id]
    _source_index_process_cache[analysis.id] = index

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

    # Section 4 hardening: this read/reconstruction path gets the same
    # bounded-selection-before-route treatment as the live finalize path -
    # ONE bounded working Evidence set, used identically for routing,
    # correlation, and the SIMPLE payload (never a second, unbounded
    # Evidence .all()). Pure read logic: never persists identities or
    # mutates the historical analysis merely to reconstruct its view.
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

    investigation_path = choose_investigation_path(evidence_rows)

    if investigation_path == InvestigationPath.CORRELATED:
        correlation_run = run_correlation(
            analysis_id=analysis_id,
            evidence_rows=evidence_rows,
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

    if analysis.status == "completed":
        return {
            "analysis_id": analysis.id,
            "status": "completed",
            # Never 100 - matches the live stream's own contract. The
            # frontend's transition to the final UI is driven by the
            # investigation_result payload below (or the live event of the
            # same shape), never by this number reaching some sentinel.
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

    dispatchable = [
        row for row in rows if row.status not in ("unsupported", "duplicate")
    ]
    ingestion_done = bool(dispatchable) and all(
        row.status == "completed" for row in dispatchable
    )

    if ingestion_done:
        # Every artifact that will ever be ingested already has been - this
        # analysis is between ingestion and the finalize/investigation
        # stages, exactly the window the live stream reports as 99 for
        # (identity/timeline/correlation), before Analysis.status flips to
        # "completed".
        progress = 99
    else:
        # Same exclusion as the live _publish_ingestion_progress query:
        # unsupported/duplicate artifacts are never dispatched, so their
        # size_bytes must not sit in the denominator alongside real
        # processed_bytes that will never catch up to it.
        total_bytes = sum(row.size_bytes for row in dispatchable)
        processed_bytes = sum(row.processed_bytes for row in dispatchable)
        progress = _ingestion_percentage(processed_bytes, total_bytes) if total_bytes else 0

    return {
        "analysis_id": analysis.id,
        "status": analysis.status,  # "pending" or "processing"
        "progress": progress,
        # Redis pub/sub has no replay: a client that connects AFTER an
        # upload-time (unsupported/duplicate) or per-artifact
        # (zero-evidence) outcome was already published live would
        # otherwise never learn about it until the whole analysis
        # completes. Every artifact whose outcome is ALREADY
        # deterministically decided - never one still "pending"/
        # "processing" - is included here so the initial snapshot alone is
        # authoritative; future live artifact_outcome events for the
        # remaining artifacts still arrive normally.
        "artifacts": _known_terminal_artifact_outcomes(db, analysis.id, rows),
    }


def _known_terminal_artifact_outcomes(
    db: Session, analysis_id: int, artifacts: list[AnalysisArtifact]
) -> list[dict]:
    terminal = [
        artifact
        for artifact in artifacts
        if artifact.status in ("unsupported", "duplicate", "completed")
    ]
    if not terminal:
        return []

    # One bounded, artifact-count-sized query (never evidence-volume-sized)
    # for the "completed" rows' real evidence counts - unsupported/
    # duplicate artifacts never have evidence and don't need it.
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

