from collections import defaultdict
from dataclasses import dataclass
from .log_praser import ParsedEvent

@dataclass
class IdentityGroups:
    identity: str
    events: list[ParsedEvent]
    confidence: float
    match_type: str

def group_events_by_identity(
    events: list[ParsedEvent]
) -> dict[str, list[ParsedEvent]]:

    groups = defaultdict(list)

    for event in events: 
        if event.trace_id: 
            Identity = f"trace:{event.trace_id}"
        elif event.request_id:
            Identity = f"request:{event.request_id}"
        else:
            Identity = "unresolved"

        groups[Identity].append(event)

    identity_groups = []

    for identity, grouped_events in groups.items():
        if identity.startswith("trace:"):
            confidence = 1.0
            match_type = "request_id"

        elif identity.startswitch("request:"):
            confidence = 0.9
            match_type = "request_id"

        else: 
            confidence = 0.0,
            match_type = "unresolved"

        identity_groups.append(
            IdentityGroups(
                identity=identity,
                events=grouped_events,
                confidence=confidence,
                match_type=match_type
            )
        )

    return dict(groups)



        
