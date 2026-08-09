from collections import defaultdict
from sqlalchemy.orm import Session
from app.models import Evidence


def persist_evidence_batch(
    db: Session,
    analysis_id: int,
    events,
) -> None:

    grouped_events = defaultdict(list)

    for event in events:
        key = (
            event.fingerprint,
            event.trace_id,
            event.request_id,
            )
        
        grouped_events[key].append(event)

    for key, fingerprint_events in grouped_events.items():
        fingerprint, trace_id, request_id = key
        first_event = fingerprint_events[0]
        last_event = fingerprint_events[-1]

        existing_evidence = (
                db.query(Evidence)
                .filter(
                    Evidence.analysis_id == analysis_id,
                    Evidence.fingerprint == fingerprint,
                    Evidence.trace_id == trace_id,
                    Evidence.request_id == request_id,
                )
                .first()
            )

        if existing_evidence:
            existing_evidence.occurrence_count += len(fingerprint_events)
            existing_evidence.last_seen = last_event.timestamp
            existing_evidence.last_line_number = last_event.line_number

        else:
            evidence = Evidence(
                analysis_id=analysis_id,
                fingerprint=fingerprint,
                event_type=first_event.exception_type,
                severity=first_event.level,
                trace_id=first_event.trace_id,
                request_id=first_event.request_id,
                first_seen=first_event.timestamp,
                last_seen=last_event.timestamp,
                occurrence_count=len(fingerprint_events),
                first_line_number=first_event.line_number,
                last_line_number=last_event.line_number,
                representative_line=first_event.raw_line,
            )

            db.add(evidence)

    