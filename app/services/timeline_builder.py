from datetime import datetime
from .log_praser import ParsedEvent

def build_timeline(
        events: list[ParsedEvent]
) -> list[ParsedEvent]:

    def sort_key(event: ParsedEvent):
        if event.timestamp:
            try:
                return (
                    0, 
                    datetime.fromisoformat(event.timestamp),
                    event.line_number
                )
            except ValueError:
                pass

        return (
            1,
            datetime.max,
            event.line_number
        )

    return sorted(events, key=sort_key)

