from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from app.models.evidence import Evidence
from time import perf_counter

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
    # Provenance only - build_correlation_nodes() creates exactly one node
    # per evidence row (evidence_ids is always a single-element list), so
    # this is unambiguous. Never read by matching/scoring; not correlated
    # on. Exists so payload/context consumers can show and explain which
    # uploaded artifact a piece of evidence came from.
    artifact_id: int | None = None
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

@dataclass(slots=True)
class CorrelationComponent:
    nodes: list[CorrelationNode] = field(default_factory=list)
    edges: list[CorrelationEdge] = field(default_factory=list)


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
    # Structural label derived directly from graph_stats/component size
    # above - not a new heuristic, not a certainty claim about what "the"
    # root cause is. score already ranks candidates; role is a
    # human-readable summary of the same DAG position that ranking uses:
    # no upstream edge but has downstream edges ("root"), has an upstream
    # edge but no downstream edges ("victim"), both ("propagation"), or no
    # edges connecting it to anything else in its component ("uncorrelated"
    # - includes true singleton components).
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

def match_correlation_signals(
    left: Evidence,
    right: Evidence,
) -> list[CorrelationSignalMatch]:
    matches: list[CorrelationSignalMatch] = []

    shared_checks = (
        (CorrelationSignal.TRACE_ID, left.trace_id, right.trace_id),
        (CorrelationSignal.REQUEST_ID, left.request_id, right.request_id),
        (
            CorrelationSignal.RESOLVED_IDENTITY,
            left.resolved_identity,
            right.resolved_identity,
        ),
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
    span_ids: dict[str, Evidence] = field(default_factory=dict)
    resolved_identities: dict[str, list[Evidence]] = field(default_factory=dict)


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

        if evidence.span_id is not None:
            indexes.span_ids[evidence.span_id] = evidence

    return indexes

def find_parent_span_candidate(
    child: Evidence,
    indexes: CorrelationIndexes,
) -> Evidence | None:
    if child.parent_span_id is None:
        return None

    parent = indexes.span_ids.get(child.parent_span_id)

    if parent is None or parent.id == child.id:
        return None

    if match_parent_span(parent, child) is None:
        return None

    return parent

def iter_identity_candidates(
    evidence: Evidence,
    indexes: CorrelationIndexes,
):
    seen_ids: set[int] = {evidence.id}

    groups = (
        indexes.trace_ids.get(evidence.trace_id, [])
        if evidence.trace_id is not None
        else []
    ), (
        indexes.request_ids.get(evidence.request_id, [])
        if evidence.request_id is not None
        else []
    ), (
        indexes.resolved_identities.get(evidence.resolved_identity, [])
        if evidence.resolved_identity is not None
        else []
    )

    for group in groups:
        for candidate in group:
            if candidate.id in seen_ids:
                continue

            seen_ids.add(candidate.id)
            yield candidate

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

def _resolved_identity_support(source: Evidence, target: Evidence) -> float:
    if (
        source.resolved_identity is None
        or source.resolved_identity != target.resolved_identity
    ):
        return 0.0

    left = source.identity_strength
    right = target.identity_strength
    if left is None or right is None:
        return 0.0

    return min(max(float(left), 0.0), max(float(right), 0.0), 1.0)

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

    identity_support = _resolved_identity_support(source, target)
    if identity_support > 0.0:
        score = 1.0 - ((1.0 - score) * (1.0 - (0.20 * identity_support)))

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
        key=lambda evidence: evidence.first_seen,
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

        for index in range(left, right):
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

            yield candidate, evidence

def has_structural_match(
    left: Evidence,
    right: Evidence,
) -> bool:
    structural_signals = {
        CorrelationSignal.SERVICE,
        CorrelationSignal.MODULE,
        CorrelationSignal.HOST,
        CorrelationSignal.CONTAINER,
        CorrelationSignal.POD,
        CorrelationSignal.ENDPOINT,
        CorrelationSignal.EXCEPTION,
        CorrelationSignal.FINGERPRINT,
    }

    matches = match_correlation_signals(left, right)

    return any(
        match.signal in structural_signals
        for match in matches
    )

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
    )

def _edge_key(
    source: Evidence,
    target: Evidence,
) -> tuple[int, int]:
    return source.id, target.id

def build_correlation_edges(
    evidence_rows: list[Evidence],
    indexes: CorrelationIndexes,
    temporal_window_ms: float = 5000.0,
) -> list[CorrelationEdge]:
    edges: list[CorrelationEdge] = []
    seen_pairs: set[tuple[int, int]] = set()

    def add_edge(source: Evidence, target: Evidence) -> None:
        key = _edge_key(source, target)

        if key in seen_pairs:
            return

        edge = build_correlation_edge(source, target)

        if edge is None:
            return

        seen_pairs.add(key)
        edges.append(edge)

    for child in evidence_rows:
        parent = find_parent_span_candidate(child, indexes)

        if parent is not None:
            add_edge(parent, child)

    for source in evidence_rows:
        for target in iter_identity_candidates(source, indexes):
            if source.first_seen is not None and target.first_seen is not None:
                if source.first_seen > target.first_seen:
                    continue

            add_edge(source, target)

    for source, target in iter_valid_temporal_candidates(
        evidence_rows,
        temporal_window_ms,
    ):
        add_edge(source, target)

    return edges

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
            )
        )

    return nodes

def build_correlation_components(
    nodes: list[CorrelationNode],
    edges: list[CorrelationEdge],
) -> list[CorrelationComponent]:
    node_by_id = {node.id: node for node in nodes}
    adjacency: dict[str, set[str]] = {
        node.id: set()
        for node in nodes
    }

    for edge in edges:
        adjacency[edge.source_id].add(edge.target_id)
        adjacency[edge.target_id].add(edge.source_id)

    visited: set[str] = set()
    components: list[CorrelationComponent] = []

    for node in nodes:
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

        component_nodes = [
            node_by_id[node_id]
            for node_id in component_ids
        ]

        component_edges = [
            edge
            for edge in edges
            if edge.source_id in component_ids
            and edge.target_id in component_ids
        ]

        components.append(
            CorrelationComponent(
                nodes=component_nodes,
                edges=component_edges,
            )
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

def enforce_dag(
    edges: list[CorrelationEdge],
) -> list[CorrelationEdge]:
    adjacency: dict[str, set[str]] = {}
    dag_edges: list[CorrelationEdge] = []

    ordered_edges = sorted(
        edges,
        key=lambda edge: edge.score,
        reverse=True,
    )

    for edge in ordered_edges:
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

    for node in nodes:
        stack = list(outgoing[node.id])
        visited: set[str] = set()

        while stack:
            target_id = stack.pop()

            if target_id in visited:
                continue

            visited.add(target_id)
            stack.extend(outgoing[target_id])

        stats[node.id].downstream_count = len(visited)

    return stats

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
        1.0
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

    candidates.sort(
        key=lambda candidate: candidate.score,
        reverse=True,
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

def run_correlation(
    analysis_id: int,
    evidence_rows: list[Evidence],
) -> CorrelationRun:
    total_start = perf_counter()

    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }

    index_start = perf_counter()
    indexes = build_correlation_indexes(evidence_rows)
    index_seconds = perf_counter() - index_start

    edge_start = perf_counter()
    edges = build_correlation_edges(
        evidence_rows,
        indexes,
    )
    edge_seconds = perf_counter() - edge_start

    dag_start = perf_counter()
    dag_edges = enforce_dag(edges)
    dag_seconds = perf_counter() - dag_start

    node_start = perf_counter()
    nodes = build_correlation_nodes(evidence_rows)
    node_seconds = perf_counter() - node_start

    component_start = perf_counter()
    components = build_correlation_components(
        nodes,
        dag_edges,
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

    print(
        "Correlation performance | "
        f"evidence={len(evidence_rows)} | "
        f"edges={len(edges)} | "
        f"dag_edges={len(dag_edges)} | "
        f"components={len(components)} | "
        f"index={index_seconds:.4f}s | "
        f"edges={edge_seconds:.4f}s | "
        f"dag={dag_seconds:.4f}s | "
        f"nodes={node_seconds:.4f}s | "
        f"components={component_seconds:.4f}s | "
        f"ranking={ranking_seconds:.4f}s | "
        f"total={total_seconds:.4f}s"
    )

    return CorrelationRun(
        result=CorrelationResult(
            analysis_id=analysis_id,
            components=components,
        ),
        root_causes=root_causes,
    )