from collections import defaultdict
from hashlib import sha256
from sqlalchemy import case, func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from app.core.processing_config import MAX_REPRESENTATIVE_LINE_BYTES
from app.models import AnalysisArtifact, Evidence

from .diagnostic_parser import parse_timestamp


def persist_evidence_batch(
    db: Session,
    analysis_id: int,
    events,
    *,
    artifact_id: int | None = None,
) -> None:
    if not isinstance(events, list):
        events = list(events)
    event_artifact_ids = {
        value
        for event in events
        if (value := getattr(event, "artifact_id", None)) is not None
    }

    if artifact_id is not None:
        mismatched_ids = event_artifact_ids - {artifact_id}
        if mismatched_ids:
            raise ValueError(
                "Evidence batch artifact id does not match its canonical events"
            )
    elif len(event_artifact_ids) > 1:
        raise ValueError("Evidence batch contains multiple analysis artifacts")
    elif event_artifact_ids:
        artifact_id = next(iter(event_artifact_ids))
    elif events:
        artifact_id = _legacy_analysis_artifact_id(db, analysis_id)

    grouped_events = defaultdict(list)

    for event in events:
        # Correlation and persisted identity must use the same schema-bounded
        # values; otherwise two overlong IDs can hash differently but truncate
        # to the same stored identity.
        trace_id = _bounded_text(event.trace_id, 255) or "__none__"
        request_id = _bounded_text(event.request_id, 255) or "__none__"
        span_id = _bounded_text(event.span_id, 255) or "__none__"
        if artifact_id is None:
            raise ValueError("Persisted events require an analysis artifact id")

        key = (
            event.fingerprint,
            trace_id,
            request_id,
            span_id,
            artifact_id,
        )

        grouped_events[key].append(event)

    if not grouped_events:
        return

    rows = []

    for key, fingerprint_events in grouped_events.items():
        fingerprint, trace_id, request_id, span_id, artifact_id = key

        # Trace/request identity is fully determined by fields already in
        # hand here (same precedence identity_persister.py applies), so
        # resolve it once at insert time instead of in a second full-table
        # UPDATE pass afterward. The only case that pass still has to handle
        # is "neither id present", whose resolved_identity string embeds the
        # DB-assigned row id and so can't be known before the insert.
        if trace_id != "__none__":
            resolved_identity = f"trace:{trace_id}"
            identity_match_type = "trace_id"
            identity_strength = 1.0
        elif request_id != "__none__":
            resolved_identity = f"request:{request_id}"
            identity_match_type = "request_id"
            identity_strength = 0.9
        else:
            resolved_identity = None
            identity_match_type = "unresolved"
            identity_strength = 0.0

        first_event = fingerprint_events[0]
        timestamps = [
            parsed_timestamp
            for event in fingerprint_events
            if (parsed_timestamp := parse_timestamp(event.timestamp)) is not None
        ]
        first_seen = min(timestamps) if timestamps else None
        last_seen = max(timestamps) if timestamps else None
        first_line_number = min(event.line_number for event in fingerprint_events)
        last_line_number = max(event.line_number for event in fingerprint_events)

        rows.append(
            {
                "analysis_id": analysis_id,
                "artifact_id": artifact_id,
                "fingerprint": fingerprint,
                "correlation_key": _correlation_key(
                    trace_id,
                    request_id,
                    span_id,
                ),
                "event_type": _bounded_text(
                    _first_present(fingerprint_events, "exception_type"),
                    100,
                ),
                "severity": _bounded_text(
                    _first_present(fingerprint_events, "level"),
                    50,
                ),
                "trace_id": _bounded_text(trace_id, 255),
                "request_id": _bounded_text(request_id, 255),
                "span_id": _bounded_text(
                    None if span_id == "__none__" else span_id,
                    255,
                ),
                "parent_span_id": _bounded_text(
                    _first_present(fingerprint_events, "parent_span_id"),
                    255,
                ),
                "service": _bounded_text(
                    _first_present(fingerprint_events, "service"),
                    255,
                ),
                "module": _bounded_text(
                    _first_present(fingerprint_events, "module"),
                    255,
                ),
                "host": _bounded_text(
                    _first_present(fingerprint_events, "host"),
                    255,
                ),
                "container": _bounded_text(
                    _first_present(fingerprint_events, "container"),
                    255,
                ),
                "pod": _bounded_text(
                    _first_present(fingerprint_events, "pod"),
                    255,
                ),
                "endpoint": _bounded_text(
                    _first_present(fingerprint_events, "endpoint"),
                    500,
                ),
                "http_status": _first_present(
                    fingerprint_events,
                    "http_status",
                ),
                "source_file": _bounded_text(
                    _first_present(fingerprint_events, "source_file"),
                    255,
                ),
                "source_format": _bounded_text(
                    _first_present(fingerprint_events, "source_format"),
                    50,
                ),
                "source_matches": _first_truthy(
                    fingerprint_events,
                    "source_matches",
                ),
                "resolved_identity": resolved_identity,
                "identity_match_type": identity_match_type,
                "identity_strength": identity_strength,
                "first_seen": first_seen,
                "last_seen": last_seen,
                "occurrence_count": len(fingerprint_events),
                "first_line_number": first_line_number,
                "last_line_number": last_line_number,
                "representative_line": _bounded_representative_line(
                    first_event.raw_line
                ),
            }
        )

    statement = insert(Evidence.__table__)

    statement = statement.on_duplicate_key_update(
        occurrence_count=(
            Evidence.occurrence_count + statement.inserted.occurrence_count
        ),
        first_seen=case(
            (Evidence.first_seen.is_(None), statement.inserted.first_seen),
            (statement.inserted.first_seen.is_(None), Evidence.first_seen),
            else_=func.least(Evidence.first_seen, statement.inserted.first_seen),
        ),
        last_seen=case(
            (Evidence.last_seen.is_(None), statement.inserted.last_seen),
            (statement.inserted.last_seen.is_(None), Evidence.last_seen),
            else_=func.greatest(Evidence.last_seen, statement.inserted.last_seen),
        ),
        first_line_number=func.least(
            Evidence.first_line_number,
            statement.inserted.first_line_number,
        ),
        last_line_number=func.greatest(
            Evidence.last_line_number,
            statement.inserted.last_line_number,
        ),
        span_id=func.coalesce(Evidence.span_id, statement.inserted.span_id),
        parent_span_id=func.coalesce(
            Evidence.parent_span_id,
            statement.inserted.parent_span_id,
        ),
        service=func.coalesce(Evidence.service, statement.inserted.service),
        module=func.coalesce(Evidence.module, statement.inserted.module),
        host=func.coalesce(Evidence.host, statement.inserted.host),
        container=func.coalesce(Evidence.container, statement.inserted.container),
        pod=func.coalesce(Evidence.pod, statement.inserted.pod),
        endpoint=func.coalesce(Evidence.endpoint, statement.inserted.endpoint),
        http_status=func.coalesce(
            Evidence.http_status,
            statement.inserted.http_status,
        ),
        source_file=func.coalesce(
            Evidence.source_file,
            statement.inserted.source_file,
        ),
        source_format=func.coalesce(
            Evidence.source_format,
            statement.inserted.source_format,
        ),
        source_matches=func.coalesce(
            Evidence.source_matches,
            statement.inserted.source_matches,
        ),
    )

    db.execute(statement, rows)


def _bounded_representative_line(value: str) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= MAX_REPRESENTATIVE_LINE_BYTES:
        return value
    return encoded[:MAX_REPRESENTATIVE_LINE_BYTES].decode(
        "utf-8",
        errors="ignore",
    )


def _correlation_key(trace_id: str, request_id: str, span_id: str) -> str:
    value = f"{trace_id}|{request_id}|{span_id}"
    return sha256(value.encode("utf-8")).hexdigest()


def _bounded_text(value: str | None, max_characters: int) -> str | None:
    if value is None:
        return None
    return value[:max_characters]


def _first_present(events, attribute: str):
    for event in events:
        value = getattr(event, attribute, None)
        if value is not None:
            return value
    return None


def _first_truthy(events, attribute: str):
    for event in events:
        if value := getattr(event, attribute, None):
            return value
    return None


def _legacy_analysis_artifact_id(db: Session, analysis_id: int) -> int:
    row = (
        db.query(AnalysisArtifact.id)
        .filter(AnalysisArtifact.analysis_id == analysis_id)
        .order_by(AnalysisArtifact.position, AnalysisArtifact.id)
        .first()
    )
    if row is None:
        raise ValueError(f"Analysis {analysis_id} has no persisted artifact")
    return int(row[0])
