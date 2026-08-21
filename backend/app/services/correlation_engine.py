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
    # Bounded, non-canonical structured fields carried straight through
    # from Evidence.diagnostic_attributes (see diagnostic_adapters.py) -
    # provenance only, never itself a matching/correlation signal.
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
    # "explicit_parent_child" (exact parent.span_id == child.parent_span_id
    # match - the strongest, definitionally-certain DIRECTED relationship:
    # trace topology and direction are proven, physical failure causation
    # is NOT) vs "inferred_propagation" (a strictly positive time delta
    # between two otherwise-correlated records - directional, but a
    # deterministic hypothesis, never proven physical causation either).
    # None for associations (this same dataclass shape is reused there;
    # associations are never directional so relationship_type/
    # direction_confidence do not apply).
    relationship_type: str | None = None
    # How confident Devflo is in the DIRECTION claim itself (parent/child,
    # earlier/later), as opposed to `score` (how confident the two records
    # are related AT ALL) - never a claim about physical causation either
    # way. 1.0 for explicit_parent_child (topology/direction is
    # definitionally certain); temporal_score(delta_ms) for
    # inferred_propagation - the same decay function scoring already uses,
    # reused rather than inventing a new confidence number: two records 5ms
    # apart support a directional propagation hypothesis far more than the
    # same pair 5 seconds apart.
    direction_confidence: float | None = None

@dataclass(slots=True)
class CorrelationComponent:
    nodes: list[CorrelationNode] = field(default_factory=list)
    # Directed relationships only (an explicit parent-span match, or a real
    # strictly-positive time delta between two otherwise-correlated
    # records) - the ONLY edges build_graph_stats/root_cause_score/
    # classify_node_role ever read, so incoming/outgoing/downstream counts
    # and root/propagation/victim roles are always derived from real
    # directed structure, never from association order. Neither edge kind
    # is proof of physical causation on its own - see relationship_type
    # above.
    edges: list[CorrelationEdge] = field(default_factory=list)
    # Non-directional "same incident" relationships: two records correlate
    # (shared trace/request/resolved identity, or a structural+temporal
    # match) but no real signal establishes which one came first - equal
    # timestamps, or a timestamp missing on either side. Used only for
    # component membership (see build_correlation_components) and for a
    # bounded LLM context - never for graph stats/roles/root-cause ranking,
    # so association-only evidence can never masquerade as directed
    # root/propagation/victim structure.
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


def _shared_source_location(left: Evidence, right: Evidence) -> bool:
    """True only when both records have a source_matches entry pointing at
    the identical file+line - never merely "both have some source match".
    Every source_matches entry already comes from source_index.py's
    _match_frame(), which returns None rather than guess on an ambiguous
    candidate, so no additional confidence filtering is needed here."""
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
        # A real equal span_id is a genuine identity/correlation signal
        # (Section 7) - configured with format weights in
        # FORMAT_SIGNAL_PRIORITY for years but never actually emitted here.
        # This alone can never become an explicit_parent_child direction:
        # only an exact parent.span_id == child.parent_span_id match
        # (match_parent_span/build_correlation_edge) is treated that way -
        # resolve_pair() (build_correlation_edges) decides
        # inferred_propagation direction purely from a real positive time
        # delta, never from which signal matched.
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

    # RESOLVED_IDENTITY is a FALLBACK correlation signal only. resolved_
    # identity is itself DERIVED from trace_id first, then request_id (see
    # persist_resolved_identities/persistent_identity_resolver.py) - so a
    # pair that already shares a real trace_id or request_id would
    # otherwise receive credit for the SAME underlying identity fact twice
    # under noisy-OR (once as TRACE_ID/REQUEST_ID above, again here). Only
    # emit it when neither raw identifier already represents the match.
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

    # A real, unambiguous match to the SAME source-code location on both
    # sides (Section 7) - source_index.py's _match_frame() already never
    # fabricates/guesses an ambiguous match, so every entry actually in
    # source_matches is already trustworthy; this only checks whether two
    # records' matches converge on the identical file+line, never merely
    # "both came from source code in general".
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
    # A list, not a single Evidence, so two different rows that happen to
    # share the same span_id (retried/duplicate emissions, or a genuine
    # collision) are both considered - previously the last one indexed
    # silently shadowed any earlier one for parent-span lookups.
    span_ids: dict[str, list[Evidence]] = field(default_factory=dict)
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
        _append_index(indexes.span_ids, evidence.span_id, evidence)

    return indexes

def find_parent_span_candidate(
    child: Evidence,
    indexes: CorrelationIndexes,
) -> Evidence | None:
    if child.parent_span_id is None:
        return None

    candidates = indexes.span_ids.get(child.parent_span_id, [])

    for parent in candidates:
        if parent.id == child.id:
            continue
        if match_parent_span(parent, child) is not None:
            return parent

    return None

_IDENTITY_GROUP_MIN_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)


def _identity_group_sort_key(evidence: Evidence):
    """Stable (first_seen, id) ordering for a shared-identity group -
    deterministic regardless of naive/aware timestamps or input list order,
    and regardless of whether first_seen is even known (falls back to the
    earliest possible sentinel, then evidence.id)."""
    first_seen = evidence.first_seen
    if first_seen is not None and first_seen.tzinfo is None:
        first_seen = first_seen.replace(tzinfo=timezone.utc)
    return (first_seen or _IDENTITY_GROUP_MIN_TIMESTAMP, evidence.id)


def iter_identity_candidate_pairs(indexes: CorrelationIndexes):
    """Bounded replacement for the previous per-source full-group scan: a
    shared identity (trace_id/request_id/resolved_identity/span_id) group
    of size N used to cost O(N^2) candidate pairs to discover (a single
    5000-row shared trace could imply ~12.5M pairs) - unnecessary for an
    investigation graph, whose real goals are: the group stays one
    connected component, near-in-time relationships stay linked, and work
    is deterministically bounded.

    Each group is scanned once, in stable (first_seen, id) order:
    - at or under IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE, every pair in the
      group is still yielded (today's behavior, unchanged for small/normal
      incidents);
    - above it, each row is paired only with its next
      IDENTITY_CANDIDATE_MAX_NEIGHBORS rows in that stable order, bounding
      candidate-pair generation to O(n*K). Every row still shares a
      neighbor with the row before and after it in the ordering, so the
      whole group remains transitively connected as one component even
      though not every pair within it is individually scored.

    Deterministic and input-order-invariant: only intra-group temporal/id
    order decides pairing, never the order evidence_rows/indexes were
    built from. Explicit parent-span relationships never go through this
    path at all (see find_parent_span_candidate, called separately and
    unconditionally before this iterator in build_correlation_edges/
    has_genuine_correlatable_structure), so they can never be affected by
    the neighbor bound below.
    """
    seen_pairs: set[frozenset[int]] = set()

    for index in (
        indexes.trace_ids,
        indexes.request_ids,
        indexes.resolved_identities,
        # A real equal span_id (Section 7) - never a directed relationship
        # on its own (see match_correlation_signals), but a genuine
        # identity-tier candidate like trace_id/request_id, so it must
        # actually be discoverable as a candidate pair, not just scoreable
        # if it happened to already be one.
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

# Same structural-relatedness definition has_structural_match() already
# uses downstream (deliberately excludes HTTP_STATUS - see that function -
# and TRACE_ID/REQUEST_ID/RESOLVED_IDENTITY, which never reach this path at
# all: iter_temporal_candidates() below excludes any pair already sharing
# one of those before the adaptive window is even considered). Named once
# here so the temporal-fallback window and has_structural_match's own
# filter can never silently drift apart from each other.
_STRUCTURAL_SIGNALS = frozenset({
    CorrelationSignal.SERVICE,
    CorrelationSignal.MODULE,
    CorrelationSignal.HOST,
    CorrelationSignal.CONTAINER,
    CorrelationSignal.POD,
    CorrelationSignal.ENDPOINT,
    CorrelationSignal.EXCEPTION,
    CorrelationSignal.FINGERPRINT,
    # A genuine same-file+line source match (Section 7) is real structural
    # relatedness, never fabricated (see _shared_source_location) - SPAN_ID
    # is deliberately NOT included here, it is treated as an identity-tier
    # signal (see iter_identity_candidate_pairs) like trace_id/request_id.
    CorrelationSignal.SOURCE,
})

# Small, explicit, deterministic tiers for the temporal FALLBACK window
# only - identity-based matching (trace_id/request_id/resolved_identity/
# parent_span) never goes through this path (see the exclusion checks in
# iter_temporal_candidates below) and is completely unaffected. These reuse
# the existing SignalStrength tier boundaries (FORMAT_SIGNAL_PRIORITY's own
# VERY_HIGH/HIGH/MEDIUM/LOW, already format-calibrated by _pair_strength)
# instead of inventing a second scoring scale - they just map the
# strongest real structural signal a candidate pair shares onto a time
# budget instead of a match-score multiplier. Not learned/tunable.
_TEMPORAL_WINDOW_STRONG_MS = 5000.0  # VERY_HIGH/HIGH strongest structural signal
_TEMPORAL_WINDOW_MEDIUM_MS = 2500.0  # MEDIUM strongest structural signal
_TEMPORAL_WINDOW_WEAK_MS = 1000.0    # LOW strongest structural signal
_TEMPORAL_WINDOW_NONE_MS = 0.0       # no structural signal at all - reject outright


def _adaptive_temporal_window_ms(
    candidate: Evidence,
    evidence: Evidence,
    max_window_ms: float,
) -> float:
    """How much timestamp separation this specific pair may tolerate as a
    temporal-fallback candidate. Reuses match_correlation_signals() (the
    same function has_structural_match()/score_candidate_pair() already
    call) purely to read the strongest REAL structural signal strength the
    pair already shares - it does not itself decide correlation, and its
    result never exceeds max_window_ms (the existing absolute cap)."""
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

        # left/right bound the search space by the fixed max window, but
        # that alone is only O(n) amortized when events are temporally
        # sparse - a dense burst (many events within one window) would
        # otherwise degrade toward an all-pairs O(n^2) scan (see
        # TEMPORAL_CANDIDATE_MAX_NEIGHBORS). Capping to the nearest K
        # candidates keeps total work O(n * K) regardless of density.
        # Within that bounded range, each individual pair additionally has
        # to clear its OWN adaptive window below - a strictly narrower,
        # per-pair filter layered on top, never a wider one.
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
    """Gate for the temporal-fallback path only (real trace/request/parent-
    span identity never reaches this - iter_temporal_candidates excludes
    any pair already sharing one). Same-artifact pairs (one uploaded file,
    genuinely one log stream) may correlate on a single structural signal -
    that shared provenance is already real evidence. Cross-artifact pairs
    need at least two independent structural signals: two different
    uploaded files merely sharing one service name, or one HTTP status
    (never counted as structural to begin with), is not enough evidence
    that they describe the same incident - see e.g. same fingerprint +
    compatible service, or same exception + compatible service/module.
    """
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
        # This function is only ever called for a verified parent.span_id
        # == child.parent_span_id match (see the caller below) - an
        # explicit, proven trace-topology parent/child relationship. That
        # proves DIRECTION, not physical failure causation (see
        # CorrelationEdge.relationship_type above).
        relationship_type="explicit_parent_child",
        direction_confidence=1.0,
    )

def _pair_key(
    left: Evidence,
    right: Evidence,
) -> frozenset[int]:
    # Unordered: the same relationship encountered from either iteration
    # direction (source=A,target=B or source=B,target=A - both happen,
    # since identity/temporal candidate search is symmetric) is one
    # relationship, resolved exactly once.
    return frozenset((left.id, right.id))

def _has_strict_time_direction(delta_ms: float | None) -> bool:
    """True only when there is a real, strictly positive time separation.
    Equal timestamps (delta_ms == 0.0) or unknown timestamps (delta_ms is
    None, i.e. first_seen missing on either side) establish no direction on
    their own and must never be assigned one by iteration/evidence-ID/
    artifact/database order - see build_correlation_edges."""
    return delta_ms is not None and delta_ms > 0.0

def _canonical_pair_order(
    left: Evidence,
    right: Evidence,
) -> tuple[Evidence, Evidence]:
    """Deterministic (earlier, later) ordering for a correlated pair, used
    only to decide which side is scored as "source" for
    evidence_delta_ms()/temporal support in score_candidate_pair() - this
    is bookkeeping for a stable, reproducible score/delta_ms, never itself
    a claim of direction (see _has_strict_time_direction, which is what
    actually decides inferred_propagation-vs-association). Falls back to
    evidence.id when timestamps tie or are missing so the SAME unordered pair always
    canonicalizes to the SAME order regardless of which iteration
    direction - or which shuffled input order - encounters it first;
    required for correlation to stay invariant to evidence input order.
    """
    left_ts, right_ts = left.first_seen, right.first_seen

    if left_ts is not None and right_ts is not None and left_ts != right_ts:
        return (left, right) if left_ts < right_ts else (right, left)

    return (left, right) if left.id < right.id else (right, left)

def build_correlation_edges(
    evidence_rows: list[Evidence],
    indexes: CorrelationIndexes,
    temporal_window_ms: float = 5000.0,
) -> tuple[list[CorrelationEdge], list[CorrelationEdge]]:
    """Returns (directed_edges, associations).

    Directed edges carry real directional evidence - either an explicit
    parent-span relationship (always directional, even at equal
    timestamps: an exact parent.span_id == child.parent_span_id match with
    compatible trace identity - relationship_type="explicit_parent_child"),
    or a strictly positive time delta between two otherwise-correlated
    records (relationship_type="inferred_propagation"). These alone feed
    build_graph_stats/root_cause_score/classify_node_role (all read
    component.edges only). Neither kind is proof of physical failure
    causation on its own - see CorrelationEdge.relationship_type.

    Associations record the same underlying correlating signal (shared
    trace_id/request_id/resolved_identity/fingerprint/service/exception/
    endpoint, or a structural+temporal match) for pairs where no real
    direction is established: equal timestamps, or a timestamp missing on
    either side. This is "same incident" evidence, not "A caused B" -
    Devflo underclaims rather than fabricates causality. Both kinds still
    connect nodes into the same component (build_correlation_components).
    """
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
            # A strictly positive delta_ms between two otherwise-correlated
            # records is a directional HYPOTHESIS (inferred_propagation) -
            # never proven physical causation, exactly like an explicit
            # parent-span match proves direction but not causation -
            # direction_confidence reuses the same temporal decay scoring
            # already applies, not a new number.
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
                diagnostic_attributes=evidence.diagnostic_attributes,
            )
        )

    return nodes

def build_correlation_components(
    nodes: list[CorrelationNode],
    edges: list[CorrelationEdge],
    associations: list[CorrelationEdge] | None = None,
) -> list[CorrelationComponent]:
    """Connectivity (which nodes end up grouped into the same component) is
    determined by BOTH directed edges and associations - two records that
    correlate but establish no direction (e.g. equal timestamps) must still
    be recognized as part of the same incident. Only `edges` (directed) is
    ever used for graph stats/roles/root-cause ranking; `associations` is
    carried on each resulting component purely for display/LLM context.
    """
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


def enforce_dag(
    edges: list[CorrelationEdge],
) -> list[CorrelationEdge]:
    """A directed cycle needs at least one edge that does NOT strictly
    advance in time: a chain of strictly-increasing timestamps can never
    loop back to an earlier one. So every edge with delta_ms > 0 (the vast
    majority in practice - every inferred_propagation edge) is safe to
    include unconditionally, skipping the expensive incremental
    reachability check (would_create_cycle) for it entirely. Only edges
    without that guarantee (explicit_parent_child edges at an equal/
    unknown/non-positive delta - the only way a directed edge can lack
    strictly-positive delta_ms) need real cycle detection, evaluated
    against the adjacency already built from every time-ordered edge.

    This was a genuine, measured bottleneck: enforce_dag's original
    "check every edge against the whole graph so far" cost degrades toward
    O(edges * (nodes + edges)) as edges accumulate - confirmed directly, a
    few thousand densely-timestamped evidence rows did not finish
    correlating within 60s before this fix (see
    TEMPORAL_CANDIDATE_MAX_NEIGHBORS for the matching candidate-count
    bound). Splitting by provable safety instead of checking every edge
    is a correctness-preserving optimization, not a new heuristic: the
    final included/excluded edge set is unchanged except in the one
    corner case where a lower-scored time-ordered edge would previously
    have been dropped in favor of a higher-scored but NOT time-ordered one
    that turned out to conflict with it - time-ordered edges are now
    always preferred there, which is at least as defensible as pure score.
    """
    time_ordered = sorted(
        (edge for edge in edges if _is_time_ordered(edge)),
        key=lambda edge: edge.score,
        reverse=True,
    )
    remaining = sorted(
        (edge for edge in edges if not _is_time_ordered(edge)),
        key=lambda edge: edge.score,
        reverse=True,
    )

    adjacency: dict[str, set[str]] = {}
    dag_edges: list[CorrelationEdge] = []

    for edge in time_ordered:
        adjacency.setdefault(edge.source_id, set()).add(edge.target_id)
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
    # downstream_count = distinct nodes reachable via outgoing edges.
    # `edges` is always component.edges - already a DAG (enforce_dag) - so
    # this is computed once for the whole graph via topological-order DP
    # (downstream[v] = union of v's children and each child's own already-
    # resolved downstream set) rather than one full DFS PER node, which
    # degrades to O(V * (V+E)) on a densely-connected component (measured
    # directly: the per-node-DFS version took 33s on a 2000-node dense
    # component that this version completes in a small fraction of that).
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
    """Kahn's algorithm. Safe to assume the graph is acyclic (it is always
    built from enforce_dag's output); any node that unexpectedly never
    reaches in-degree 0 (would only happen if that invariant were somehow
    violated) is simply omitted rather than raising - build_graph_stats
    treats a missing entry as "no further downstream" instead of crashing.
    """
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
        # No directed edge touches this node at all (it may still sit in a
        # larger component purely via association - e.g. equal-timestamp
        # "same incident" evidence) - that is not a root signal, it is the
        # same "uncorrelated" position classify_node_role already assigns
        # for this exact condition. Without this, an association-only node
        # would read as maximally root-like merely for having no incoming
        # DIRECTED edge, even though it has no outgoing one either.
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
    # OCR-derived evidence (diagnostic_adapters._stream_image_events) runs
    # the extracted/normalized text through the exact same
    # normalize_text_event() classification as "generic" text, so it is
    # given the identical signal profile - not a new scoring rule, just
    # registering the format so _pair_strength()'s existing min()-of-both-
    # sides calibration (unchanged) can engage instead of unconditionally
    # returning None for every signal. Without this entry, "image"-sourced
    # evidence could never match any other evidence on ANY signal
    # (including trace_id), regardless of genuine shared identity.
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

def has_genuine_correlatable_structure(evidence_rows: list[Evidence]) -> bool:
    """Early-exit yes/no check reusing the EXACT relationship semantics
    build_correlation_edges uses (parent-span, identity-candidate, and
    bounded temporal+structural matching) - so investigation routing can
    never drift from what correlation itself would actually find. Stops at
    the first qualifying relationship; callers here only need a routing
    decision, never the full graph/scores.
    """
    if len(evidence_rows) <= 1:
        return False

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
    edges, associations = build_correlation_edges(
        evidence_rows,
        indexes,
    )
    edge_seconds = perf_counter() - edge_start

    dag_start = perf_counter()
    # enforce_dag only makes sense for directed edges - associations are
    # non-directional by definition, so there is no "cycle" for them.
    dag_edges = enforce_dag(edges)
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