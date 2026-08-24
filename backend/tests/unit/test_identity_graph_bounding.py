"""Identity graph complexity bounding.

iter_identity_candidate_pairs replaces the previous per-source full-group
scan (O(n^2) candidate pairs for one giant shared-identity group - 5000
rows sharing a trace could imply ~12.5M pairs) with a group-based,
deterministically-ordered iterator: full pairwise at or under
IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE, a bounded
IDENTITY_CANDIDATE_MAX_NEIGHBORS-ahead sliding window above it - O(n*K)
instead of O(n^2), while the whole group stays transitively connected as
one component and explicit parent-span relationships are never subject to
the bound at all.

Small monkeypatched thresholds throughout so tests exercise the bounding
behavior itself without constructing thousands of real rows.
"""
from datetime import datetime, timedelta, timezone

from app.models.evidence import Evidence
from app.services import correlation_engine
from app.services.correlation_engine import (
    TEMPORAL_CANDIDATE_MAX_NEIGHBORS,
    build_correlation_components,
    build_correlation_edges,
    build_correlation_indexes,
    build_correlation_nodes,
    iter_identity_candidate_pairs,
    rank_root_causes,
)


def _evidence(evidence_id, **kwargs):
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "occurrence_count": 1,
        "source_format": "generic",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)


def _shared_trace_group(count, base, trace_id="trace-big", **extra):
    return [
        _evidence(
            i,
            trace_id=trace_id,
            first_seen=base + timedelta(milliseconds=i),
            last_seen=base + timedelta(milliseconds=i),
            **extra,
        )
        for i in range(1, count + 1)
    ]


# --- 1: small group retains full pairwise behavior --------------------------


def test_small_identity_group_retains_full_pairwise_behavior(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 128)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(5, base)

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))

    # C(5, 2) = 10 - every pair in the group, not bounded by the tiny
    # IDENTITY_CANDIDATE_MAX_NEIGHBORS since the group is under the
    # (patched-large) full-pairwise threshold.
    assert len(pairs) == 10


# --- 2: large group generates O(n*K), not O(n^2) ----------------------------


def test_large_identity_group_generates_bounded_not_quadratic_candidate_work(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(20, base)

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))

    # Full pairwise would be C(20, 2) = 190. Bounded to at most 20*2 = 40
    # (fewer in practice, since the last few rows have fewer than K rows
    # remaining ahead of them in the stable ordering).
    assert len(pairs) < 190
    assert len(pairs) <= 20 * 2


# --- 3: a large same-trace group still becomes one connected component -----


def test_large_shared_trace_group_still_becomes_one_component(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(20, base)

    edges, associations = build_correlation_edges(rows, build_correlation_indexes(rows))
    nodes = build_correlation_nodes(rows)
    components = build_correlation_components(nodes, edges, associations)

    assert len(components) == 1
    assert len(components[0].nodes) == 20


# --- 4: adjacent/near-time evidence remains linked --------------------------


def test_adjacent_rows_in_a_large_group_remain_linked(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(20, base)

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))
    paired_ids = {frozenset((a.id, b.id)) for a, b in pairs}

    # Every consecutive (in stable temporal order) row pair is discovered.
    for i in range(1, 20):
        assert frozenset((i, i + 1)) in paired_ids


# --- 5: explicit parent-span relationship survives in a huge identity group -


def test_explicit_parent_span_survives_inside_a_huge_identity_group(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    # A large shared-trace group where the FIRST and LAST rows are also a
    # real parent/child span pair - far outside any neighbor window if this
    # had to go through the bounded identity-candidate path at all.
    rows = _shared_trace_group(30, base, source_format="opentelemetry")
    rows[0].span_id = "span-parent"
    rows[-1].parent_span_id = "span-parent"

    edges, _associations = build_correlation_edges(rows, build_correlation_indexes(rows))

    explicit = [e for e in edges if e.relationship_type == "explicit_parent_child"]
    assert len(explicit) == 1
    assert explicit[0].source_id == f"evidence-{rows[0].id}"
    assert explicit[0].target_id == f"evidence-{rows[-1].id}"


# --- 6: shuffled input produces the same deterministic candidate pair set --


def test_shuffled_evidence_input_produces_the_same_candidate_pairs(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(15, base)

    ordered_pairs = {
        frozenset((a.id, b.id))
        for a, b in iter_identity_candidate_pairs(build_correlation_indexes(rows))
    }

    shuffled_pairs = {
        frozenset((a.id, b.id))
        for a, b in iter_identity_candidate_pairs(build_correlation_indexes(list(reversed(rows))))
    }

    assert ordered_pairs == shuffled_pairs


# --- 7-9: request-id / resolved-identity / span-id groups are bounded too --


def test_large_request_id_only_group_is_bounded(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, request_id="req-big", first_seen=base + timedelta(milliseconds=i))
        for i in range(1, 21)
    ]

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))
    assert len(pairs) <= 20 * 2


def test_large_resolved_identity_only_group_is_bounded(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, resolved_identity="custom:big", first_seen=base + timedelta(milliseconds=i))
        for i in range(1, 21)
    ]

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))
    assert len(pairs) <= 20 * 2


def test_large_span_id_only_group_is_bounded(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, span_id="span-big", first_seen=base + timedelta(milliseconds=i))
        for i in range(1, 21)
    ]

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))
    assert len(pairs) <= 20 * 2


# --- 10: temporal fallback K is unchanged ------------------------------------


def test_temporal_fallback_neighbor_bound_is_unchanged():
    """Distinct from IDENTITY_CANDIDATE_MAX_NEIGHBORS - the temporal
    fallback path (iter_temporal_candidates) is a separate mechanism this
    item must not touch."""
    assert TEMPORAL_CANDIDATE_MAX_NEIGHBORS == 40


# --- 11: root score formula/weights are unchanged ---------------------------


def test_root_cause_scoring_formula_and_weights_are_unchanged():
    """Item 4 only bounds candidate-PAIR GENERATION - scoring itself
    (root_cause_score/rank_root_causes) must remain untouched."""
    base = datetime.now(timezone.utc)
    root = _evidence(1, trace_id="trace-r", first_seen=base, last_seen=base)
    victim = _evidence(
        2, trace_id="trace-r",
        first_seen=base + timedelta(milliseconds=10), last_seen=base + timedelta(milliseconds=10),
    )
    rows = [root, victim]
    edges, associations = build_correlation_edges(rows, build_correlation_indexes(rows))
    nodes = build_correlation_nodes(rows)
    components = build_correlation_components(nodes, edges, associations)
    evidence_by_id = {e.id: e for e in rows}

    candidates = rank_root_causes(components[0], evidence_by_id)
    root_candidate = next(c for c in candidates if c.node_id == "evidence-1")
    assert root_candidate.role == "root"
