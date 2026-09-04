from .log_praser import ParsedEvent

IMPORTANT_LEVELS = {
    "WARNING",
    "WARN",
    "ERROR",
    "CRITICAL",
}

def is_evidence_worthy(event: ParsedEvent) -> bool:
    if event.level in IMPORTANT_LEVELS:
        return True

    if event.exception_type is not None:
        return True

    if event.stack_frames:
        return True

    if event.http_status is not None and event.http_status >= 400:
        return True

    if event.source_format == "opentelemetry" and (
        event.trace_id is not None or event.span_id is not None
    ):
        return True

    return False

def filter_important_events(events: list[ParsedEvent]) -> list[ParsedEvent]:
    return [event for event in events if is_evidence_worthy(event)]
