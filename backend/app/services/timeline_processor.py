from typing import Any
from app.services.correlation_engine import (
    CorrelationComponent,
    RootCauseCandidate,
    _stable_node_key,
)

def build_component_timeline(
    component: CorrelationComponent,
    root_candidates: list[RootCauseCandidate],
) -> list[dict[str, Any]]:
    role_by_node_id = {candidate.node_id: candidate.role for candidate in root_candidates}

    timed_nodes = sorted(
        (node for node in component.nodes if node.first_seen is not None),
        key=lambda node: (node.first_seen, _stable_node_key(node)),
    )
    untimed_nodes = [node for node in component.nodes if node.first_seen is None]

    earliest = timed_nodes[0].first_seen if timed_nodes else None

    timeline = [
        {
            "node_id": node.id,
            "timestamp": node.first_seen.isoformat(),
            "relative_ms": (node.first_seen - earliest).total_seconds() * 1000.0,
            "service": node.service,
            "role": role_by_node_id.get(node.id, "uncorrelated"),
        }
        for node in timed_nodes
    ]
    timeline.extend(
        {
            "node_id": node.id,
            "timestamp": None,
            "relative_ms": None,
            "service": node.service,
            "role": role_by_node_id.get(node.id, "uncorrelated"),
        }
        for node in untimed_nodes
    )

    return timeline
