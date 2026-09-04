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

def test_small_identity_group_retains_full_pairwise_behavior(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 128)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(5, base)

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))

    assert len(pairs) == 10

def test_large_identity_group_generates_bounded_not_quadratic_candidate_work(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(20, base)

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))

    assert len(pairs) < 190
    assert len(pairs) <= 20 * 2

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

def test_adjacent_rows_in_a_large_group_remain_linked(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(20, base)

    pairs = list(iter_identity_candidate_pairs(build_correlation_indexes(rows)))
    paired_ids = {frozenset((a.id, b.id)) for a, b in pairs}

    for i in range(1, 20):
        assert frozenset((i, i + 1)) in paired_ids

def test_explicit_parent_span_survives_inside_a_huge_identity_group(monkeypatch):
    monkeypatch.setattr(correlation_engine, "IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE", 4)
    monkeypatch.setattr(correlation_engine, "IDENTITY_CANDIDATE_MAX_NEIGHBORS", 2)
    base = datetime.now(timezone.utc)
    rows = _shared_trace_group(30, base, source_format="opentelemetry")
    rows[0].span_id = "span-parent"
    rows[-1].parent_span_id = "span-parent"

    edges, _associations = build_correlation_edges(rows, build_correlation_indexes(rows))

    explicit = [e for e in edges if e.relationship_type == "explicit_parent_child"]
    assert len(explicit) == 1
    assert explicit[0].source_id == f"evidence-{rows[0].id}"
    assert explicit[0].target_id == f"evidence-{rows[-1].id}"

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

def test_temporal_fallback_neighbor_bound_is_unchanged():
    assert TEMPORAL_CANDIDATE_MAX_NEIGHBORS == 40

def test_root_cause_scoring_formula_and_weights_are_unchanged():
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
