import logging
from app.core.celery_app import celery_app
from app.db.database import sessionLocal
from app.models import Analysis
from app.utils.file_reader import stream_text_lines
from app.services import (
    parse_log_line,
    filter_important_events,
    build_exception_fingerprint,
    create_batches,
    persist_evidence_batch,
    persist_resolved_identities
)

logger = logging.getLogger(__name__)


@celery_app.task
def process_analysis(analysis_id: int):

    db = sessionLocal()

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            logger.warning("Analysis %s not found", analysis_id)
            return

        analysis.status = "processing"
        db.commit()

        line_count = analysis.last_processed_line
        parsed_event_count = 0

        for batch in create_batches(
            stream_text_lines(
                analysis.saved_file_path,
                start_offset=analysis.last_processed_line,
            )
        ):
            batch_events = []

            for line, current_offset in batch:
                line_count += 1

                event = parse_log_line(
                    line=line,
                    line_number=line_count,
                )

                event.fingerprint = build_exception_fingerprint(event)

                batch_events.append(event)

                logger.debug(
                    "Analysis %s | line %s | %s",
                    analysis_id,
                    line_count,
                    line,
                )

            parsed_event_count += len(batch_events)

            logger.debug(
                "Analysis %s | processed batch | events=%s",
                analysis_id,
                len(batch_events),
            )

            important_batch_events = filter_important_events(batch_events)

            logger.debug(
                "Analysis %s | important_events in batch=%s",
                analysis_id,
                len(important_batch_events),
            )

            persist_evidence_batch(
                db=db,
                analysis_id=analysis_id,
                events=important_batch_events,
            )

            analysis.last_processed_line = line_count
            analysis.processed_bytes = current_offset

            db.commit()

        logger.info(
            "Analysis %s parsed | total_lines=%s | parsed_events=%s",
            analysis_id,
            line_count,
            parsed_event_count,
        )

        persist_resolved_identities(
            db=db,
            analysis_id=analysis_id,
        )

        logger.info(
            "Analysis %s | evidence identities resolved",
            analysis_id,
        )

        analysis.status = "completed"
        db.commit()

    finally:
        db.close()
