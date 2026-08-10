from collections import defaultdict
from sqlalchemy.dialects.mysql import insert
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

    if not grouped_events:
        return

    rows = []

    for key, fingerprint_events in grouped_events.items():
        fingerprint, trace_id, request_id = key

        first_event = fingerprint_events[0]
        last_event = fingerprint_events[-1]

        rows.append(
            {
                "analysis_id": analysis_id,
                "fingerprint": fingerprint,
                "event_type": first_event.exception_type,
                "severity": first_event.level,
                "trace_id": trace_id,
                "request_id": request_id,
                "first_seen": first_event.timestamp,
                "last_seen": last_event.timestamp,
                "occurrence_count": len(fingerprint_events),
                "first_line_number": first_event.line_number,
                "last_line_number": last_event.line_number,
                "representative_line": first_event.raw_line,
            }
        )

    statement = insert(Evidence).values(rows)

    statement = statement.on_duplicate_key_update(
        occurrence_count=(
            Evidence.occurrence_count
            + statement.inserted.occurrence_count
        ),
        last_seen=statement.inserted.last_seen,
        last_line_number=statement.inserted.last_line_number,
    )

    db.execute(statement)