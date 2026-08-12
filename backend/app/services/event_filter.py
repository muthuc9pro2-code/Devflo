from .log_praser import ParsedEvent

IMPORTANT_LEVELS = {
    "WARNING",
    "WARN",
    "ERROR",
    "CRITICAL",
}


def filter_important_events(events: list[ParsedEvent]) -> list[ParsedEvent]:

    important_events = []

    for event in events:
        has_explicit_otel_identity = event.source_format == "opentelemetry" and (
            event.trace_id is not None or event.span_id is not None
        )

        if event.level in IMPORTANT_LEVELS or has_explicit_otel_identity:
            important_events.append(event)

    return important_events
