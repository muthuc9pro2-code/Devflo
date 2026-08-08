import logging
from app.core.celery_app import celery_app
from app.db.database import sessionLocal
from app.models import Analysis
from app.utils.file_reader import stream_text_lines
from app.services import (
    parse_log_line,
    filter_important_events,
    group_events_by_identity,
    build_timeline,
)

logger = logging.getLogger(__name__)


@celery_app.task
def process_analysis(analysis_id: int):

    db = sessionLocal()

    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()

        if analysis is None:
            print(f"processing analysis {analysis_id} not found")
            return

        analysis.status = "processing"
        db.commit()

        line_count = 0
        parsed_events = []

        for line in stream_text_lines(analysis.saved_file_path):
            line_count += 1

            event = parse_log_line(line=line, line_number=line_count)

            parsed_events.append(event)

            logger.debug(
                "Analysis %s | line %s | %s",
                analysis_id,
                line_count,
                line,
            )

        logger.info(
            "Analysis %s parsed | total line=%s | parsed_events=%s",
            analysis_id,
            line_count,
            len(parsed_events),
        )

        important_events = filter_important_events(parsed_events)

        logger.info(
            "Analysis %s | important_events=%s",
            analysis_id,
            len(important_events),
        )

        identity_groups = group_events_by_identity(important_events)

        logger.info(
            "Analysis %s | identity_groups=%s", analysis_id, len(identity_groups)
        )

        for group in identity_groups:
            logger.debug(
                "Analysis %s | identity=%s | events=%s | match_type=%s | confidence=%s",
                analysis_id,
                group.identity,
                len(events),
                group.match_type,
                group.confidence,
            )

        timelines = {}

        for identity, events in identity_groups.items():
            timelines[identity] = build_timeline(events)

            logger.debug(
                "Analysis %s | timeline=%s | events=%s",
                analysis_id,
                identity,
                len(timelines[identity]),
            )

        logger.info("Analysis %s | timeline_build=%s", analysis_id, len(timelines))

        analysis.status = "completed"
        db.commit()

    finally:
        db.close()
