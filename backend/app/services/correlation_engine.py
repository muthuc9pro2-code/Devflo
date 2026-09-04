import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from app.core.processing_config import (
    IDENTITY_CANDIDATE_MAX_NEIGHBORS,
    IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE,
    TEMPORAL_CANDIDATE_MAX_NEIGHBORS,
)
from app.models.evidence import Evidence
from time import perf_counter

logger = logging.getLogger(__name__)

def _stable_evidence_key(evidence: Evidence) -> tuple:
    return (
        evidence.first_line_number,
        evidence.fingerprint or "",
        evidence.correlation_key or "",
        evidence.source_file or "",
    )

def _stable_node_key(node: "CorrelationNode") -> tuple:
    first_line_number = getattr(node, "first_line_number", None)

    return (
        first_line_number
        if first_line_number is not None
        else float("inf"),
        getattr(node, "fingerprint", None) or "",
        getattr(node, "correlation_key", None) or "",
        getattr(node, "source_file", None) or "",
    )

class CorrelationSignal(str, Enum):
    PARENT_SPAN = "parent_span"
    SPAN_ID = "span_id"
    TRACE_ID = "trace_id"
    REQUEST_ID = "request_id"
    RESOLVED_IDENTITY = "resolved_identity"

    SERVICE = "service"
    MODULE = "module"
    HOST = "host"
    CONTAINER = "container"
    POD = "pod"

    ENDPOINT = "endpoint"
    HTTP_STATUS = "http_status"

    EXCEPTION = "exception"
    FINGERPRINT = "fingerprint"
    SOURCE = "source"
    TEMPORAL = "temporal"

class SignalStrength(float, Enum):
    VERY_HIGH = 1.0
    HIGH = 0.85
    MEDIUM = 0.60
    LOW = 0.30

@dataclass(slots=True)
class CorrelationNode:
    id: str
    service: str | None
    fingerprint: str | None
    first_seen: datetime | None
    last_seen: datetime | None
    occurrence_count: int = 1
    evidence_ids: list[int] = field(default_factory=list)
    artifact_id: int | None = None
    correlation_key: str | None = None
    trace_id: str | None = None
    request_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    resolved_identity: str | None = None
    event_type: str | None = None
    severity: str | None = None
    module: str | None = None
    host: str | None = None
    container: str | None = None
    pod: str | None = None
    endpoint: str | None = None
    http_status: int | None = None
    source_file: str | None = None
    source_format: str | None = None
    source_matches: list[dict] = field(default_factory=list)
    first_line_number: int | None = None
    last_line_number: int | None = None
    representative_line: str | None = None
    identity_match_type: str | None = None
    identity_strength: float | None = None
    diagnostic_attributes: dict | None = None

@dataclass(frozen=True, slots=True)
class CorrelationSignalMatch:
    signal: CorrelationSignal
    strength: SignalStrength

@dataclass(slots=True)
class CorrelationEdge:
    source_id: str
    target_id: str
    score: float
    delta_ms: float | None
    signals: list[CorrelationSignal] = field(default_factory=list)
    relationship_type: str | None = None
    direction_confidence: float | None = None

@dataclass(slots=True)
class CorrelationComponent:
    nodes: list[CorrelationNode] = field(default_factory=list)
    edges: list[CorrelationEdge] = field(default_factory=list)
    associations: list[CorrelationEdge] = field(default_factory=list)

@dataclass(slots=True)
class CorrelationResult:
    analysis_id: int
    components: list[CorrelationComponent] = field(default_factory=list)

@dataclass(slots=True)
class NodeGraphStats:
    incoming_count: int = 0
    outgoing_count: int = 0
    downstream_count: int = 0
    incoming_strength: float = 0.0
    outgoing_strength: float = 0.0

@dataclass(slots=True)
class RootCauseCandidate:
    node_id: str
    score: float
    graph_stats: NodeGraphStats
    role: str = "uncorrelated"

@dataclass(slots=True)
class CorrelationRun:
    result: CorrelationResult
    root_causes: dict[int, list[RootCauseCandidate]] = field(
        default_factory=dict
    )

def _shared_value(
    left: str | None,
    right: str | None,
) -> bool:
    return left is not None and left == right

def _pair_strength(
    left_format: str | None,
    right_format: str | None,
    signal: CorrelationSignal,
) -> SignalStrength | None:
    left_strength = signal_strength(left_format, signal)
    right_strength = signal_strength(right_format, signal)

    if left_strength is None or right_strength is None:
        return None

    return SignalStrength(
        min(left_strength.value, right_strength.value)
    )

def _append_shared_signal(
    matches: list[CorrelationSignalMatch],
    left_format: str | None,
    right_format: str | None,
    signal: CorrelationSignal,
    left_value: str | None,
    right_value: str | None,
) -> None:
    if not _shared_value(left_value, right_value):
        return

    strength = _pair_strength(
        left_format,
        right_format,
        signal,
    )

    if strength is not None:
        matches.append(
            CorrelationSignalMatch(
                signal=signal,
                strength=strength,
            )
        )

def _shared_source_location(left: Evidence, right: Evidence) -> bool:
    left_locations = {
        (match.get("relative_path"), match.get("line_number"))
        for match in (left.source_matches or [])
        if isinstance(match, dict) and match.get("relative_path") and match.get("line_number")
    }
    if not left_locations:
        return False

    return any(
        isinstance(match, dict)
        and (match.get("relative_path"), match.get("line_number")) in left_locations
        for match in (right.source_matches or [])
    )

def match_correlation_signals(
    left: Evidence,
    right: Evidence,
) -> list[CorrelationSignalMatch]:
    matches: list[CorrelationSignalMatch] = []

    shared_checks = (
        (CorrelationSignal.TRACE_ID, left.trace_id, right.trace_id),
        (CorrelationSignal.REQUEST_ID, left.request_id, right.request_id),
        (CorrelationSignal.SPAN_ID, left.span_id, right.span_id),
        (CorrelationSignal.SERVICE, left.service, right.service),
        (CorrelationSignal.MODULE, left.module, right.module),
        (CorrelationSignal.HOST, left.host, right.host),
        (CorrelationSignal.CONTAINER, left.container, right.container),
        (CorrelationSignal.POD, left.pod, right.pod),
        (CorrelationSignal.ENDPOINT, left.endpoint, right.endpoint),
        (CorrelationSignal.EXCEPTION, left.event_type, right.event_type),
        (
            CorrelationSignal.FINGERPRINT,
            left.fingerprint,
            right.fingerprint,
        ),
    )

    for signal, left_value, right_value in shared_checks:
        _append_shared_signal(
            matches,
            left.source_format,
            right.source_format,
            signal,
            left_value,
            right_value,
        )

    if not _shared_value(left.trace_id, right.trace_id) and not _shared_value(
        left.request_id, right.request_id
    ):
        _append_shared_signal(
            matches,
            left.source_format,
            right.source_format,
            CorrelationSignal.RESOLVED_IDENTITY,
            left.resolved_identity,
            right.resolved_identity,
        )

    if _shared_source_location(left, right):
        strength = _pair_strength(
            left.source_format,
            right.source_format,
            CorrelationSignal.SOURCE,
        )
        if strength is not None:
            matches.append(
                CorrelationSignalMatch(
                    signal=CorrelationSignal.SOURCE,
                    strength=strength,
                )
            )

    if (
        left.http_status is not None
        and right.http_status is not None
        and left.http_status == right.http_status
        and left.http_status >= 400
    ):
        strength = _pair_strength(
            left.source_format,
            right.source_format,
            CorrelationSignal.HTTP_STATUS,
        )
        if strength is not None:
            matches.append(
                CorrelationSignalMatch(
                    signal=CorrelationSignal.HTTP_STATUS,
                    strength=strength,
                )
            )

    return matches

def match_parent_span(
    parent: Evidence,
    child: Evidence,
) -> CorrelationSignalMatch | None:
    if parent.span_id is None or child.parent_span_id is None:
        return None

    if parent.span_id != child.parent_span_id:
        return None

    if (
        parent.trace_id is not None
        and child.trace_id is not None
        and parent.trace_id != child.trace_id
    ):
        return None

    strength = _pair_strength(
        parent.source_format,
        child.source_format,
        CorrelationSignal.PARENT_SPAN,
    )

    if strength is None:
        return None

    return CorrelationSignalMatch(
        signal=CorrelationSignal.PARENT_SPAN,
        strength=strength,
    )

@dataclass(slots=True)
class CorrelationIndexes:
    trace_ids: dict[str, list[Evidence]] = field(default_factory=dict)
    request_ids: dict[str, list[Evidence]] = field(default_factory=dict)
    span_ids: dict[str, list[Evidence]] = field(default_factory=dict)
    resolved_identities: dict[str, list[Evidence]] = field(default_factory=dict)

@dataclass(slots=True)
class CorrelationPreparation:

    indexes: CorrelationIndexes
    directed_edges: list[CorrelationEdge]
    associations: list[CorrelationEdge]

    @property
    def has_relationships(self) -> bool:
        return bool(self.directed_edges or self.associations)

def _append_index(
    index: dict[str, list[Evidence]],
    key: str | None,
    evidence: Evidence,
) -> None:
    if key is None:
        return

    index.setdefault(key, []).append(evidence)

def build_correlation_indexes(
    evidence_rows: list[Evidence],
) -> CorrelationIndexes:
    indexes = CorrelationIndexes()

    for evidence in evidence_rows:
        _append_index(indexes.trace_ids, evidence.trace_id, evidence)
        _append_index(indexes.request_ids, evidence.request_id, evidence)
        _append_index(
            indexes.resolved_identities,
            evidence.resolved_identity,
            evidence,
        )
        _append_index(indexes.span_ids, evidence.span_id, evidence)

    return indexes

def find_parent_span_candidate(
    child: Evidence,
    indexes: CorrelationIndexes,
) -> Evidence | None:
    if child.parent_span_id is None:
        return None

    candidates = indexes.span_ids.get(child.parent_span_id, [])

    compatible = [
        parent
        for parent in candidates
        if parent.id != child.id and match_parent_span(parent, child) is not None
    ]
    if not compatible:
        return None

    return min(compatible, key=_stable_evidence_key)

_IDENTITY_GROUP_MIN_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)

def _identity_group_sort_key(evidence: Evidence):
    first_seen = evidence.first_seen
    if first_seen is not None and first_seen.tzinfo is None:
        first_seen = first_seen.replace(tzinfo=timezone.utc)
    return (first_seen or _IDENTITY_GROUP_MIN_TIMESTAMP, *_stable_evidence_key(evidence))

def iter_identity_candidate_pairs(indexes: CorrelationIndexes):
    seen_pairs: set[frozenset[int]] = set()

    for index in (
        indexes.trace_ids,
        indexes.request_ids,
        indexes.resolved_identities,
        indexes.span_ids,
    ):
        for group in index.values():
            if len(group) < 2:
                continue

            ordered = sorted(group, key=_identity_group_sort_key)
            neighbor_span = (
                len(ordered)
                if len(ordered) <= IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE
                else IDENTITY_CANDIDATE_MAX_NEIGHBORS
            )

            for position, left in enumerate(ordered):
                for right in ordered[position + 1:position + 1 + neighbor_span]:
                    key = frozenset((left.id, right.id))
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)
                    yield left, right

def score_signal_matches(
    matches: list[CorrelationSignalMatch],
) -> float:
    if not matches:
        return 0.0

    miss_probability = 1.0

    for match in matches:
        miss_probability *= 1.0 - match.strength.value

    return 1.0 - miss_probability

def evidence_delta_ms(
    source: Evidence,
    target: Evidence,
) -> float | None:
    if source.first_seen is None or target.first_seen is None:
        return None

    return (
        target.first_seen - source.first_seen
    ).total_seconds() * 1000.0

def score_candidate_pair(
    source: Evidence,
    target: Evidence,
) -> tuple[float, float | None, list[CorrelationSignalMatch]]:
    matches = match_correlation_signals(source, target)

    parent_match = match_parent_span(source, target)

    if parent_match is not None:
        matches.append(parent_match)

    delta_ms = evidence_delta_ms(source, target)

    score = score_signal_matches(matches)

    if delta_ms is not None and delta_ms >= 0:
        temporal_support = temporal_score(delta_ms) * SignalStrength.LOW.value

        if temporal_support > 0.0:
            matches.append(
                CorrelationSignalMatch(
                    signal=CorrelationSignal.TEMPORAL,
                    strength=SignalStrength.LOW,
                )
            )
            score = 1.0 - ((1.0 - score) * (1.0 - temporal_support))

    return score, delta_ms, matches

_STRUCTURAL_SIGNALS = frozenset({
    CorrelationSignal.SERVICE,
    CorrelationSignal.MODULE,
    CorrelationSignal.HOST,
    CorrelationSignal.CONTAINER,
    CorrelationSignal.POD,
    CorrelationSignal.ENDPOINT,
    CorrelationSignal.EXCEPTION,
    CorrelationSignal.FINGERPRINT,
    CorrelationSignal.SOURCE,
})

_TEMPORAL_WINDOW_STRONG_MS = 5000.0
_TEMPORAL_WINDOW_MEDIUM_MS = 2500.0
_TEMPORAL_WINDOW_WEAK_MS = 1000.0
_TEMPORAL_WINDOW_NONE_MS = 0.0

def _adaptive_temporal_window_ms(
    candidate: Evidence,
    evidence: Evidence,
    max_window_ms: float,
) -> float:
    matches = match_correlation_signals(candidate, evidence)

    strongest = max(
        (
            match.strength.value
            for match in matches
            if match.signal in _STRUCTURAL_SIGNALS
        ),
        default=None,
    )

    if strongest is None:
        window_ms = _TEMPORAL_WINDOW_NONE_MS
    elif strongest >= SignalStrength.HIGH.value:
        window_ms = _TEMPORAL_WINDOW_STRONG_MS
    elif strongest >= SignalStrength.MEDIUM.value:
        window_ms = _TEMPORAL_WINDOW_MEDIUM_MS
    else:
        window_ms = _TEMPORAL_WINDOW_WEAK_MS

    return min(window_ms, max_window_ms)

def iter_temporal_candidates(
    evidence_rows: list[Evidence],
    window_ms: float = 5000.0,
):
    ordered = sorted(
        (
            evidence
            for evidence in evidence_rows
            if evidence.first_seen is not None
        ),
        key=lambda evidence: (evidence.first_seen, *_stable_evidence_key(evidence)),
    )

    left = 0

    for right, evidence in enumerate(ordered):
        while left < right:
            delta_ms = (
                evidence.first_seen - ordered[left].first_seen
            ).total_seconds() * 1000.0

            if delta_ms <= window_ms:
                break

            left += 1

        window_start = max(left, right - TEMPORAL_CANDIDATE_MAX_NEIGHBORS)
        for index in range(window_start, right):
            candidate = ordered[index]

            if (
                candidate.trace_id is not None
                and candidate.trace_id == evidence.trace_id
            ):
                continue

            if (
                candidate.request_id is not None
                and candidate.request_id == evidence.request_id
            ):
                continue

            if (
                candidate.resolved_identity is not None
                and candidate.resolved_identity
                == evidence.resolved_identity
            ):
                continue

            if (
                candidate.span_id is not None
                and candidate.span_id == evidence.span_id
            ):
                continue

            pair_delta_ms = (
                evidence.first_seen - candidate.first_seen
            ).total_seconds() * 1000.0

            adaptive_window_ms = _adaptive_temporal_window_ms(
                candidate,
                evidence,
                window_ms,
            )

            if pair_delta_ms > adaptive_window_ms:
                continue

            yield candidate, evidence

def has_structural_match(
    left: Evidence,
    right: Evidence,
) -> bool:
    matches = match_correlation_signals(left, right)
    structural_signal_count = sum(
        1 for match in matches if match.signal in _STRUCTURAL_SIGNALS
    )

    if structural_signal_count == 0:
        return False

    if left.artifact_id == right.artifact_id:
        return True

    return structural_signal_count >= 2

def iter_valid_temporal_candidates(
    evidence_rows: list[Evidence],
    window_ms: float = 5000.0,
):
    for source, target in iter_temporal_candidates(
        evidence_rows,
        window_ms,
    ):
        if not has_structural_match(source, target):
            continue

        yield source, target

def build_correlation_edge(
    source: Evidence,
    target: Evidence,
) -> CorrelationEdge | None:
    score, delta_ms, matches = score_candidate_pair(
        source,
        target,
    )

    if not matches or score <= 0.0:
        return None

    return CorrelationEdge(
        source_id=f"evidence-{source.id}",
        target_id=f"evidence-{target.id}",
        score=score,
        delta_ms=delta_ms,
        signals=[match.signal for match in matches],
        relationship_type="explicit_parent_child",
        direction_confidence=1.0,
    )

def _pair_key(
    left: Evidence,
    right: Evidence,
) -> frozenset[int]:
    return frozenset((left.id, right.id))

def _has_strict_time_direction(delta_ms: float | None) -> bool:
    return delta_ms is not None and delta_ms > 0.0

def _canonical_pair_order(
    left: Evidence,
    right: Evidence,
) -> tuple[Evidence, Evidence]:
    left_ts, right_ts = left.first_seen, right.first_seen

    if left_ts is not None and right_ts is not None and left_ts != right_ts:
        return (left, right) if left_ts < right_ts else (right, left)

    return (
        (left, right)
        if _stable_evidence_key(left) < _stable_evidence_key(right)
        else (right, left)
    )

def build_correlation_edges(
    evidence_rows: list[Evidence],
    indexes: CorrelationIndexes,
    temporal_window_ms: float = 5000.0,
) -> tuple[list[CorrelationEdge], list[CorrelationEdge]]:
    directed_edges: list[CorrelationEdge] = []
    associations: list[CorrelationEdge] = []
    seen_pairs: set[frozenset[int]] = set()

    for child in evidence_rows:
        parent = find_parent_span_candidate(child, indexes)

        if parent is None:
            continue

        key = _pair_key(parent, child)

        if key in seen_pairs:
            continue

        edge = build_correlation_edge(parent, child)

        if edge is None:
            continue

        seen_pairs.add(key)
        directed_edges.append(edge)

    def resolve_pair(a: Evidence, b: Evidence) -> None:
        key = _pair_key(a, b)

        if key in seen_pairs:
            return

        seen_pairs.add(key)

        source, target = _canonical_pair_order(a, b)
        score, delta_ms, matches = score_candidate_pair(source, target)

        if not matches or score <= 0.0:
            return

        is_time_ordered = _has_strict_time_direction(delta_ms)

        relationship = CorrelationEdge(
            source_id=f"evidence-{source.id}",
            target_id=f"evidence-{target.id}",
            score=score,
            delta_ms=delta_ms,
            signals=[match.signal for match in matches],
            relationship_type="inferred_propagation" if is_time_ordered else None,
            direction_confidence=temporal_score(delta_ms) if is_time_ordered else None,
        )

        if is_time_ordered:
            directed_edges.append(relationship)
        else:
            associations.append(relationship)

    for source, target in iter_identity_candidate_pairs(indexes):
        resolve_pair(source, target)

    for source, target in iter_valid_temporal_candidates(
        evidence_rows,
        temporal_window_ms,
    ):
        resolve_pair(source, target)

    return directed_edges, associations

def prepare_correlation(
    evidence_rows: list[Evidence],
    *,
    indexes: CorrelationIndexes | None = None,
) -> CorrelationPreparation:
    if indexes is None:
        indexes = build_correlation_indexes(evidence_rows)

    directed_edges, associations = build_correlation_edges(
        evidence_rows,
        indexes,
    )
    return CorrelationPreparation(
        indexes=indexes,
        directed_edges=directed_edges,
        associations=associations,
    )

def build_correlation_nodes(
    evidence_rows: list[Evidence],
) -> list[CorrelationNode]:
    nodes: list[CorrelationNode] = []

    for evidence in evidence_rows:
        nodes.append(
            CorrelationNode(
                id=f"evidence-{evidence.id}",
                service=evidence.service,
                fingerprint=evidence.fingerprint,
                first_seen=evidence.first_seen,
                last_seen=evidence.last_seen,
                occurrence_count=evidence.occurrence_count,
                evidence_ids=[evidence.id],
                artifact_id=evidence.artifact_id,
                correlation_key=evidence.correlation_key,
                trace_id=evidence.trace_id,
                request_id=evidence.request_id,
                span_id=evidence.span_id,
                parent_span_id=evidence.parent_span_id,
                resolved_identity=evidence.resolved_identity,
                event_type=evidence.event_type,
                severity=evidence.severity,
                module=evidence.module,
                host=evidence.host,
                container=evidence.container,
                pod=evidence.pod,
                endpoint=evidence.endpoint,
                http_status=evidence.http_status,
                source_file=evidence.source_file,
                source_format=evidence.source_format,
                source_matches=list(evidence.source_matches or []),
                first_line_number=evidence.first_line_number,
                last_line_number=evidence.last_line_number,
                representative_line=evidence.representative_line,
                identity_match_type=evidence.identity_match_type,
                identity_strength=evidence.identity_strength,
                diagnostic_attributes=evidence.diagnostic_attributes,
            )
        )

    return nodes

def build_correlation_components(
    nodes: list[CorrelationNode],
    edges: list[CorrelationEdge],
    associations: list[CorrelationEdge] | None = None,
) -> list[CorrelationComponent]:
    associations = associations or []
    node_by_id = {node.id: node for node in nodes}
    adjacency: dict[str, set[str]] = {
        node.id: set()
        for node in nodes
    }

    for edge in edges:
        adjacency[edge.source_id].add(edge.target_id)
        adjacency[edge.target_id].add(edge.source_id)

    for association in associations:
        adjacency[association.source_id].add(association.target_id)
        adjacency[association.target_id].add(association.source_id)

    visited: set[str] = set()
    components: list[CorrelationComponent] = []

    for node in sorted(nodes, key=_stable_node_key):
        if node.id in visited:
            continue

        stack = [node.id]
        component_ids: set[str] = set()

        while stack:
            node_id = stack.pop()

            if node_id in visited:
                continue

            visited.add(node_id)
            component_ids.add(node_id)

            for neighbor_id in adjacency[node_id]:
                if neighbor_id not in visited:
                    stack.append(neighbor_id)

        component_nodes = sorted(
            (node_by_id[node_id] for node_id in component_ids),
            key=_stable_node_key,
        )

        component_edges = [
            edge
            for edge in edges
            if edge.source_id in component_ids
            and edge.target_id in component_ids
        ]

        component_associations = [
            association
            for association in associations
            if association.source_id in component_ids
            and association.target_id in component_ids
        ]

        components.append(
            CorrelationComponent(
                nodes=component_nodes,
                edges=component_edges,
                associations=component_associations,
            )
        )

    components.sort(
        key=lambda component: _stable_node_key(component.nodes[0])
        if component.nodes
        else (float("inf"), "", "", ""),
    )

    return components

def would_create_cycle(
    adjacency: dict[str, set[str]],
    source_id: str,
    target_id: str,
) -> bool:
    if source_id == target_id:
        return True

    stack = [target_id]
    visited: set[str] = set()

    while stack:
        node_id = stack.pop()

        if node_id == source_id:
            return True

        if node_id in visited:
            continue

        visited.add(node_id)
        stack.extend(adjacency.get(node_id, ()))

    return False

def _is_time_ordered(edge: CorrelationEdge) -> bool:
    return edge.delta_ms is not None and edge.delta_ms > 0.0

def _edge_endpoint_stable_key(
    node_id: str,
    evidence_by_id: dict[int, Evidence],
) -> tuple:
    try:
        evidence_id = int(node_id.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return (float("inf"), "", "", "")

    evidence = evidence_by_id.get(evidence_id)

    if evidence is None:
        return (float("inf"), "", "", "")

    return _stable_evidence_key(evidence)

def enforce_dag(
    edges: list[CorrelationEdge],
    evidence_by_id: dict[int, Evidence],
) -> list[CorrelationEdge]:
    def _edge_priority_key(edge: CorrelationEdge) -> tuple:
        return (
            -edge.score,
            _edge_endpoint_stable_key(
                edge.source_id,
                evidence_by_id,
            ),
            _edge_endpoint_stable_key(
                edge.target_id,
                evidence_by_id,
            ),
            edge.relationship_type or "",
            tuple(
                sorted(
                    signal.value
                    for signal in edge.signals
                )
            ),
            (
                edge.delta_ms
                if edge.delta_ms is not None
                else float("inf")
            ),
        )

    time_ordered = sorted(
        (
            edge
            for edge in edges
            if _is_time_ordered(edge)
        ),
        key=_edge_priority_key,
    )

    remaining = sorted(
        (
            edge
            for edge in edges
            if not _is_time_ordered(edge)
        ),
        key=_edge_priority_key,
    )

    adjacency: dict[str, set[str]] = {}
    dag_edges: list[CorrelationEdge] = []

    for edge in time_ordered:
        adjacency.setdefault(
            edge.source_id,
            set(),
        ).add(edge.target_id)

        dag_edges.append(edge)

    for edge in remaining:
        if would_create_cycle(
            adjacency,
            edge.source_id,
            edge.target_id,
        ):
            continue

        adjacency.setdefault(
            edge.source_id,
            set(),
        ).add(edge.target_id)

        dag_edges.append(edge)

    return dag_edges

def build_graph_stats(
    nodes: list[CorrelationNode],
    edges: list[CorrelationEdge],
) -> dict[str, NodeGraphStats]:
    stats = {
        node.id: NodeGraphStats()
        for node in nodes
    }

    outgoing: dict[str, set[str]] = {
        node.id: set()
        for node in nodes
    }

    for edge in edges:
        source_stats = stats[edge.source_id]
        target_stats = stats[edge.target_id]

        source_stats.outgoing_count += 1
        source_stats.outgoing_strength += edge.score

        target_stats.incoming_count += 1
        target_stats.incoming_strength += edge.score

        outgoing[edge.source_id].add(edge.target_id)

    downstream_cache: dict[str, frozenset[str]] = {}
    for node_id in reversed(_topological_order(nodes, outgoing)):
        reachable: set[str] = set(outgoing[node_id])
        for child_id in outgoing[node_id]:
            reachable |= downstream_cache.get(child_id, frozenset())
        downstream_cache[node_id] = frozenset(reachable)
        stats[node_id].downstream_count = len(reachable)

    return stats

def _topological_order(
    nodes: list[CorrelationNode],
    outgoing: dict[str, set[str]],
) -> list[str]:
    incoming_count: dict[str, int] = {node.id: 0 for node in nodes}

    for targets in outgoing.values():
        for target_id in targets:
            incoming_count[target_id] += 1

    queue = [node_id for node_id, count in incoming_count.items() if count == 0]
    order: list[str] = []

    while queue:
        node_id = queue.pop()
        order.append(node_id)

        for target_id in outgoing[node_id]:
            incoming_count[target_id] -= 1
            if incoming_count[target_id] == 0:
                queue.append(target_id)

    return order

def root_cause_score(
    node: CorrelationNode,
    stats: NodeGraphStats,
    component: CorrelationComponent,
    evidence_by_id: dict[int, Evidence],
) -> float:
    component_size = len(component.nodes)

    if component_size <= 1:
        return 1.0

    root_position = (
        0.0
        if stats.incoming_count == 0 and stats.outgoing_count == 0
        else 1.0
        if stats.incoming_count == 0
        else 1.0 / (1.0 + stats.incoming_count)
    )

    propagation = (
        stats.downstream_count
        / max(component_size - 1, 1)
    )

    outgoing_support = min(
        stats.outgoing_strength
        / max(stats.outgoing_count, 1),
        1.0,
    )

    failure = failure_strength(
        node,
        evidence_by_id,
    )

    source_support = source_evidence_strength(
        node,
        evidence_by_id,
    )

    failure_support = max(
        failure,
        source_support,
    )

    temporal_origin = temporal_origin_score(
        node,
        component,
    )

    return (
        0.30 * root_position
        + 0.25 * propagation
        + 0.15 * outgoing_support
        + 0.20 * failure_support
        + 0.10 * temporal_origin
    )

def failure_strength(
    node: CorrelationNode,
    evidence_by_id: dict[int, Evidence],
) -> float:
    strength = 0.0

    for evidence_id in node.evidence_ids:
        evidence = evidence_by_id.get(evidence_id)

        if evidence is None:
            continue

        severity = (evidence.severity or "").upper()

        if severity in {"CRITICAL", "FATAL"}:
            strength = max(strength, 1.0)
        elif severity == "ERROR":
            strength = max(strength, 0.85)
        elif severity in {"WARNING", "WARN"}:
            strength = max(strength, 0.60)

        if evidence.event_type is not None:
            strength = max(strength, 0.85)

        if (
            evidence.http_status is not None
            and evidence.http_status >= 500
        ):
            strength = max(strength, 0.85)

    return strength

def source_evidence_strength(
    node: CorrelationNode,
    evidence_by_id: dict[int, Evidence],
) -> float:
    best = 0.0

    for evidence_id in node.evidence_ids:
        evidence = evidence_by_id.get(evidence_id)

        if evidence is None or not evidence.source_matches:
            continue

        for match in evidence.source_matches:
            confidence = match.get("confidence")

            if isinstance(confidence, (int, float)):
                value = min(max(float(confidence), 0.0), 1.0)
            elif confidence == "high":
                value = 1.0
            elif confidence == "medium":
                value = 0.60
            elif confidence == "low":
                value = 0.30
            else:
                continue

            best = max(best, value)

    return best

def temporal_origin_score(
    node: CorrelationNode,
    component: CorrelationComponent,
) -> float:
    timestamps = [
        candidate.first_seen
        for candidate in component.nodes
        if candidate.first_seen is not None
    ]

    if node.first_seen is None or not timestamps:
        return 0.0

    earliest = min(timestamps)
    latest = max(timestamps)

    total_ms = (
        latest - earliest
    ).total_seconds() * 1000.0

    if total_ms <= 0.0:
        return 1.0

    offset_ms = (
        node.first_seen - earliest
    ).total_seconds() * 1000.0

    return max(
        0.0,
        1.0 - (offset_ms / total_ms),
    )

def classify_node_role(
    stats: NodeGraphStats,
    component_size: int,
) -> str:
    if component_size <= 1:
        return "uncorrelated"
    if stats.incoming_count == 0 and stats.outgoing_count == 0:
        return "uncorrelated"
    if stats.incoming_count == 0:
        return "root"
    if stats.outgoing_count == 0:
        return "victim"
    return "propagation"

def rank_root_causes(
    component: CorrelationComponent,
    evidence_by_id: dict[int, Evidence],
) -> list[RootCauseCandidate]:
    stats = build_graph_stats(
        component.nodes,
        component.edges,
    )
    component_size = len(component.nodes)

    candidates = [
        RootCauseCandidate(
            node_id=node.id,
            score=root_cause_score(
                node,
                stats[node.id],
                component,
                evidence_by_id,
            ),
            graph_stats=stats[node.id],
            role=classify_node_role(stats[node.id], component_size),
        )
        for node in component.nodes
    ]

    node_by_id = {
        node.id: node
        for node in component.nodes
    }

    candidates.sort(
        key=lambda candidate: (
            -candidate.score,
            _stable_node_key(
                node_by_id[candidate.node_id]
            ),
        )
    )

    return candidates

FORMAT_SIGNAL_PRIORITY: dict[str, dict[CorrelationSignal, SignalStrength]] = {
    "opentelemetry": {
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "json": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "web_server": {
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.ENDPOINT: SignalStrength.MEDIUM,
        CorrelationSignal.HOST: SignalStrength.MEDIUM,
        CorrelationSignal.HTTP_STATUS: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "container": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.POD: SignalStrength.HIGH,
        CorrelationSignal.CONTAINER: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "database": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.HIGH,
        CorrelationSignal.EXCEPTION: SignalStrength.HIGH,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "cloud_gateway": {
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.ENDPOINT: SignalStrength.HIGH,
        CorrelationSignal.HTTP_STATUS: SignalStrength.MEDIUM,
        CorrelationSignal.HOST: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "serverless": {
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.HIGH,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "syslog": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.HOST: SignalStrength.HIGH,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "message_broker": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.HIGH,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "browser": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.ENDPOINT: SignalStrength.HIGH,
        CorrelationSignal.HTTP_STATUS: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "ci_cd": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.SOURCE: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "stack_trace": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.EXCEPTION: SignalStrength.HIGH,
        CorrelationSignal.SOURCE: SignalStrength.HIGH,
        CorrelationSignal.FINGERPRINT: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.HIGH,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "generic": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "image": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
}

def signal_strength(
    source_format: str | None,
    signal: CorrelationSignal,
) -> SignalStrength | None:
    if source_format is None:
        return None

    priorities = FORMAT_SIGNAL_PRIORITY.get(source_format)

    if priorities is None:
        return None

    return priorities.get(signal)

def temporal_score(
    delta_ms: float,
    decay_ms: float = 1000.0,
) -> float:
    if delta_ms < 0:
        return 0.0

    return 1.0 / (1.0 + (delta_ms / decay_ms))

def has_genuine_correlatable_structure(
    evidence_rows: list[Evidence],
    *,
    indexes: CorrelationIndexes | None = None,
) -> bool:
    if len(evidence_rows) <= 1:
        return False

    if indexes is None:
        indexes = build_correlation_indexes(evidence_rows)

    for child in evidence_rows:
        if find_parent_span_candidate(child, indexes) is not None:
            return True

    for source, target in iter_identity_candidate_pairs(indexes):
        score, _delta_ms, matches = score_candidate_pair(source, target)
        if matches and score > 0.0:
            return True

    for source, target in iter_valid_temporal_candidates(evidence_rows):
        score, _delta_ms, matches = score_candidate_pair(source, target)
        if matches and score > 0.0:
            return True

    return False

def run_correlation(
    analysis_id: int,
    evidence_rows: list[Evidence],
    *,
    indexes: CorrelationIndexes | None = None,
    preparation: CorrelationPreparation | None = None,
) -> CorrelationRun:
    total_start = perf_counter()

    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }

    index_start = perf_counter()
    if preparation is None:
        if indexes is None:
            indexes = build_correlation_indexes(evidence_rows)
        index_seconds = perf_counter() - index_start

        edge_start = perf_counter()
        edges, associations = build_correlation_edges(
            evidence_rows,
            indexes,
        )
        edge_seconds = perf_counter() - edge_start
    else:
        indexes = preparation.indexes
        index_seconds = 0.0
        edges = preparation.directed_edges
        associations = preparation.associations
        edge_seconds = 0.0

    dag_start = perf_counter()
    dag_edges = enforce_dag(edges, evidence_by_id)
    dag_seconds = perf_counter() - dag_start

    node_start = perf_counter()
    nodes = build_correlation_nodes(evidence_rows)
    node_seconds = perf_counter() - node_start

    component_start = perf_counter()
    components = build_correlation_components(
        nodes,
        dag_edges,
        associations,
    )
    component_seconds = perf_counter() - component_start

    ranking_start = perf_counter()

    root_causes = {
        index: rank_root_causes(
            component,
            evidence_by_id,
        )
        for index, component in enumerate(components)
    }

    ranking_seconds = perf_counter() - ranking_start
    total_seconds = perf_counter() - total_start

    logger.info(
        "Correlation performance | "
        "evidence=%s | edges=%s | dag_edges=%s | associations=%s | components=%s | "
        "index=%.4fs | edges=%.4fs | dag=%.4fs | nodes=%.4fs | "
        "components=%.4fs | ranking=%.4fs | total=%.4fs",
        len(evidence_rows),
        len(edges),
        len(dag_edges),
        len(associations),
        len(components),
        index_seconds,
        edge_seconds,
        dag_seconds,
        node_seconds,
        component_seconds,
        ranking_seconds,
        total_seconds,
    )

    return CorrelationRun(
        result=CorrelationResult(
            analysis_id=analysis_id,
            components=components,
        ),
        root_causes=root_causes,
    )
