"""Section 18: a real per-component timeline, built from the correlation
nodes/roles that are already loaded in memory by the time correlation
finishes - never a second DB scan, never a persisted timeline subsystem.

The previous process_persisted_timelines() was a genuine no-op (an empty
function body) despite the live SSE stream publishing "Timeline
reconstruction completed" as if real work had happened - that call and its
progress stage are removed; this module now does the one thing worth doing
instead.
"""
from typing import Any

from app.services.correlation_engine import CorrelationComponent, RootCauseCandidate


def build_component_timeline(
    component: CorrelationComponent,
    root_candidates: list[RootCauseCandidate],
) -> list[dict[str, Any]]:
    """Chronological view of one correlated component's nodes. Only real
    timestamps are ever used for ordering/relative_ms - nodes with no
    first_seen at all are appended afterward with timestamp/relative_ms
    both explicitly None, never assigned a fabricated position. Nodes that
    share the exact same first_seen legitimately share the same
    relative_ms (0.0 or otherwise) - that is the honest ordering, not an
    artifact of this function.
    """
    role_by_node_id = {candidate.node_id: candidate.role for candidate in root_candidates}

    timed_nodes = sorted(
        (node for node in component.nodes if node.first_seen is not None),
        key=lambda node: (node.first_seen, node.id),
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
