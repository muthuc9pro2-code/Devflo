from .log_praser import ParsedEvent

IMPORTANT_LEVELS = {
    "WARNING",
    "WARN",
    "ERROR",
    "CRITICAL",
}


def is_evidence_worthy(event: ParsedEvent) -> bool:
    """The single canonical definition of "this parsed record is real
    diagnostic evidence" - the one place that decision is made, instead of
    slightly different retention definitions scattered across the
    adapter/persistence layers (previously: the actual final persistence
    gate in app/tasks/analysis.py only ever checked event.level, inlined
    rather than calling this module at all).

    A producer's own severity label is not always trustworthy: a real
    failure logged as "INFO" (e.g. a structured event whose "exception"
    field is genuine but whose "level" field the producer just got wrong)
    must not disappear merely because IMPORTANT_LEVELS doesn't include
    "INFO". Any ONE of these already-extracted, already-deterministic
    signals is sufficient - this never invents a field that wasn't
    already there:
      - severity resolves to WARNING/ERROR/CRITICAL;
      - a real exception/failure type was extracted;
      - real stack-frame/crash structure was extracted;
      - an HTTP 4xx/5xx status was extracted;
      - explicit OpenTelemetry trace/span identity (already-established
        special case, unchanged).
    Explicit fatal/panic/crash markers and database slow-query blocks are
    NOT re-checked here: diagnostic_adapters.py's per-format normalizers
    already promote those to a real WARNING/ERROR level upstream, so they
    are already covered by the severity check above.
    """
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
