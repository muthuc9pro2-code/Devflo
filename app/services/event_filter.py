from .log_praser import ParsedEvent

IMPORTANT_LEVELS = {
    "WARNING",
    "WARN",
    "ERROR",
    "CRITICAL",
}

def filter_important_events(
        events: list[ParsedEvent]
) -> list[ParsedEvent]:

    important_events = []

    for event in events:
        if event.level in IMPORTANT_LEVELS:
            important_events.append(event)

    return important_events

