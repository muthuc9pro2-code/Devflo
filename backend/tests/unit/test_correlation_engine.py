from datetime import datetime, timedelta, timezone

import pytest

from app.models.evidence import Evidence
from app.services.correlation_engine import (
    CorrelationComponent,
    CorrelationNode,
    NodeGraphStats,
    build_correlation_components,
    build_correlation_edges,
    build_correlation_indexes,
    classify_node_role,
    match_correlation_signals,
    match_parent_span,
    rank_root_causes,
    run_correlation,
    temporal_score,
)
from app.services import correlation_engine as correlation_engine_module


def _evidence(
    evidence_id: int,
    **kwargs,
) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "event_type": "error",
        "severity": "ERROR",
        "occurrence_count": 1,
        "first_seen": datetime.now(timezone.utc),
        "last_seen": datetime.now(timezone.utc),
        "source_format": "generic",
        # Real Evidence.first_line_number is NOT NULL (see the model) -
        # every deterministic tie-break in correlation_engine.py now uses
        # it instead of evidence.id, so test fixtures need a real value
        # too. Distinct per evidence_id, matching this helper's existing
        # convention for id/fingerprint.
        "first_line_number": evidence_id,
    }

    defaults.update(kwargs)

    return Evidence(**defaults)


def test_indexes_strong_identity_fields() -> None:
    evidence = _evidence(
        1,
        trace_id="trace-1",
        request_id="request-1",
        span_id="span-1",
        resolved_identity="identity-1",
    )

    indexes = build_correlation_indexes([evidence])

    assert indexes.trace_ids["trace-1"] == [evidence]
    assert indexes.request_ids["request-1"] == [evidence]
    # A list (not a single Evidence) - two different rows can
    # share a span_id, and both must remain discoverable as parent-span/
    # identity candidates, not just the last one indexed.
    assert indexes.span_ids["span-1"] == [evidence]
    assert indexes.resolved_identities["identity-1"] == [evidence]


def test_parent_span_creates_explicit_match() -> None:
    parent = _evidence(
        1,
        source_format="opentelemetry",
        trace_id="trace-1",
        span_id="parent-span",
    )

    child = _evidence(
        2,
        source_format="opentelemetry",
        trace_id="trace-1",
        parent_span_id="parent-span",
    )

    match = match_parent_span(parent, child)

    assert match is not None
    assert match.signal.value == "parent_span"
    assert match.strength.value == 1.0


def test_conflicting_trace_rejects_parent_span() -> None:
    parent = _evidence(
        1,
        source_format="opentelemetry",
        trace_id="trace-A",
        span_id="parent-span",
    )

    child = _evidence(
        2,
        source_format="opentelemetry",
        trace_id="trace-B",
        parent_span_id="parent-span",
    )

    assert match_parent_span(parent, child) is None


def test_cross_format_trace_match() -> None:
    left = _evidence(
        1,
        source_format="web_server",
        trace_id="trace-1",
    )

    right = _evidence(
        2,
        source_format="database",
        trace_id="trace-1",
    )

    matches = match_correlation_signals(left, right)

    assert any(
        match.signal.value == "trace_id"
        for match in matches
    )


def test_temporal_score_decays() -> None:
    assert temporal_score(0.0) == 1.0
    assert temporal_score(100.0) > temporal_score(1000.0)
    assert temporal_score(-1.0) == 0.0

def test_run_correlation_builds_propagation_dag() -> None:
    base = datetime.now(timezone.utc)

    database = _evidence(
        1,
        source_format="database",
        trace_id="trace-1",
        service="database",
        first_seen=base,
        last_seen=base,
    )

    payment = _evidence(
        2,
        source_format="opentelemetry",
        trace_id="trace-1",
        span_id="payment-span",
        service="payment",
        first_seen=base + timedelta(milliseconds=100),
        last_seen=base + timedelta(milliseconds=100),
    )

    api = _evidence(
        3,
        source_format="web_server",
        trace_id="trace-1",
        service="api",
        http_status=500,
        first_seen=base + timedelta(milliseconds=250),
        last_seen=base + timedelta(milliseconds=250),
    )

    run = run_correlation(
        analysis_id=1,
        evidence_rows=[database, payment, api],
    )

    assert run.result.analysis_id == 1
    assert len(run.result.components) == 1

    component = run.result.components[0]

    assert len(component.nodes) == 3
    assert component.edges
    assert run.root_causes[0]

    # Structural role: derived purely from graph position (incoming/
    # outgoing edge counts already computed for scoring), not a new
    # heuristic. database has no upstream edge but leads to payment/api ->
    # root. api is a dead end everything else leads to -> victim. payment
    # is both caused-by and a cause-of something else -> propagation.
    roles = {candidate.node_id: candidate.role for candidate in run.root_causes[0]}
    assert roles["evidence-1"] == "root"  # database
    assert roles["evidence-2"] == "propagation"  # payment
    assert roles["evidence-3"] == "victim"  # api


# --- Regression tests: the "__none__" sentinel bug ---------------------
#
# evidence_store.py used to persist the literal string "__none__" into the
# real trace_id/request_id columns for events with no such id (it already
# converted the same sentinel back to a real NULL for span_id, just missed
# trace_id/request_id). Since _shared_value() only treats `None` specially
# ("left is not None and left == right"), two completely unrelated events
# that both lacked an id would both store trace_id="__none__" and register
# a perfect TRACE_ID/REQUEST_ID match - reproduced directly against
# match_correlation_signals() before the fix (score 1.0 for two events
# months apart, different services, different formats). These tests prove
# the fix at the correlation-engine boundary: real NULL correctly yields no
# match, matching real IDs still correlates exactly as before.


def test_events_without_trace_id_do_not_trace_match() -> None:
    left = _evidence(
        1,
        source_format="web_server",
        service="checkout-service",
        trace_id=None,
    )
    right = _evidence(
        2,
        source_format="database",
        service="unrelated-batch-job",
        trace_id=None,
    )

    matches = match_correlation_signals(left, right)

    assert not any(match.signal.value == "trace_id" for match in matches)


def test_events_without_request_id_do_not_request_match() -> None:
    left = _evidence(
        1,
        source_format="cloud_gateway",
        service="checkout-service",
        request_id=None,
    )
    right = _evidence(
        2,
        source_format="serverless",
        service="unrelated-batch-job",
        request_id=None,
    )

    matches = match_correlation_signals(left, right)

    assert not any(match.signal.value == "request_id" for match in matches)


def test_sparse_unrelated_evidence_cannot_receive_a_perfect_correlation() -> None:
    """Five events, no trace/request/span/resolved_identity anywhere,
    different services, different formats, days apart (well outside the
    5s temporal window) and no shared structural field either - the
    engine must never merge these into one component just because none of
    them happen to carry an identifier.
    """
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        _evidence(1, source_format="web_server", service="checkout-service", first_seen=base),
        _evidence(
            2,
            source_format="database",
            service="unrelated-batch-job",
            first_seen=base + timedelta(hours=4, minutes=30),
        ),
        _evidence(
            3,
            source_format="syslog",
            service="auth-service",
            first_seen=base + timedelta(days=1, hours=3),
        ),
        _evidence(
            4,
            source_format="ci_cd",
            service="ci-pipeline",
            first_seen=base + timedelta(days=2, hours=9),
        ),
        _evidence(
            5,
            source_format="serverless",
            service="image-resizer",
            first_seen=base + timedelta(days=3, hours=22),
        ),
    ]

    causal_edges, associations = build_correlation_edges(rows, build_correlation_indexes(rows))
    assert causal_edges == []
    assert associations == []

    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 5
    assert all(len(component.edges) == 0 for component in run.result.components)
    assert all(len(component.associations) == 0 for component in run.result.components)
    assert all(len(component.nodes) == 1 for component in run.result.components)


def test_real_matching_trace_id_still_correlates_normally() -> None:
    """Both records share the SAME explicit timestamp:
    with no real time separation, a real trace_id match is still a
    genuine relationship - just an association (same incident), not a
    fabricated causal direction. Equal timestamps must not gain
    direction merely from which side happens to be evidence id 1 vs 2."""
    same_time = datetime.now(timezone.utc)
    left = _evidence(1, source_format="web_server", trace_id="trace-real-1", first_seen=same_time, last_seen=same_time)
    right = _evidence(2, source_format="database", trace_id="trace-real-1", first_seen=same_time, last_seen=same_time)

    matches = match_correlation_signals(left, right)

    assert any(
        match.signal.value == "trace_id" and match.strength.value == 1.0
        for match in matches
    )

    causal_edges, associations = build_correlation_edges(
        [left, right], build_correlation_indexes([left, right])
    )
    assert causal_edges == []
    assert len(associations) == 1
    assert associations[0].score > 0.0


def test_classify_node_role_singleton_component_is_uncorrelated() -> None:
    assert classify_node_role(NodeGraphStats(), component_size=1) == "uncorrelated"


def test_classify_node_role_no_edges_in_a_larger_component_is_uncorrelated() -> None:
    assert (
        classify_node_role(
            NodeGraphStats(incoming_count=0, outgoing_count=0),
            component_size=3,
        )
        == "uncorrelated"
    )


def test_classify_node_role_root_has_no_incoming_but_has_outgoing() -> None:
    assert (
        classify_node_role(
            NodeGraphStats(incoming_count=0, outgoing_count=2),
            component_size=3,
        )
        == "root"
    )


def test_classify_node_role_victim_has_incoming_but_no_outgoing() -> None:
    assert (
        classify_node_role(
            NodeGraphStats(incoming_count=2, outgoing_count=0),
            component_size=3,
        )
        == "victim"
    )


def test_classify_node_role_propagation_has_both() -> None:
    assert (
        classify_node_role(
            NodeGraphStats(incoming_count=1, outgoing_count=1),
            component_size=3,
        )
        == "propagation"
    )


# --- Association vs causation -----------------------------------------------


def test_equal_timestamp_relationship_never_gains_direction_from_evidence_id():
    """The exact bug this fix targets: two records sharing a trace_id at
    the EXACT same timestamp used to become a directed edge purely because
    one had a lower evidence id / appeared first in the input list. The
    higher-id record is deliberately listed FIRST here - if direction were
    still leaking from id/iteration order, evidence-9 would wrongly become
    the causal "source"."""
    same_time = datetime.now(timezone.utc)
    higher_id = _evidence(9, trace_id="trace-x", first_seen=same_time, last_seen=same_time)
    lower_id = _evidence(3, trace_id="trace-x", first_seen=same_time, last_seen=same_time)

    causal_edges, associations = build_correlation_edges(
        [higher_id, lower_id], build_correlation_indexes([higher_id, lower_id])
    )

    assert causal_edges == []
    assert len(associations) == 1
    # A pure association carries no directional claim - only that the two
    # ARE the same relationship, regardless of which id/order produced it.
    assert {associations[0].source_id, associations[0].target_id} == {
        "evidence-9",
        "evidence-3",
    }


def test_strictly_earlier_record_becomes_causal_source_regardless_of_id_order():
    """The flip side: a REAL time gap does establish direction, and that
    direction must follow chronology, never evidence id. The
    chronologically-earlier record here deliberately has the HIGHER
    evidence id."""
    base = datetime.now(timezone.utc)
    earlier_but_higher_id = _evidence(
        9, trace_id="trace-y", first_seen=base, last_seen=base
    )
    later_but_lower_id = _evidence(
        3, trace_id="trace-y", first_seen=base + timedelta(milliseconds=500),
        last_seen=base + timedelta(milliseconds=500),
    )

    causal_edges, associations = build_correlation_edges(
        [earlier_but_higher_id, later_but_lower_id],
        build_correlation_indexes([earlier_but_higher_id, later_but_lower_id]),
    )

    assert associations == []
    assert len(causal_edges) == 1
    assert causal_edges[0].source_id == "evidence-9"  # chronologically earlier
    assert causal_edges[0].target_id == "evidence-3"  # chronologically later


def test_exact_parent_span_match_is_causal_even_at_equal_timestamps():
    """A genuine parent.span_id == child.parent_span_id relationship is
    real directional evidence and may produce a directed causal edge even
    when both records share the exact same timestamp - unlike a bare
    trace_id/request_id match at equal timestamps, which must NOT."""
    same_time = datetime.now(timezone.utc)
    parent = _evidence(
        1, source_format="opentelemetry", trace_id="trace-z", span_id="span-parent",
        first_seen=same_time, last_seen=same_time,
    )
    child = _evidence(
        2, source_format="opentelemetry", trace_id="trace-z", parent_span_id="span-parent",
        first_seen=same_time, last_seen=same_time,
    )

    causal_edges, associations = build_correlation_edges(
        [parent, child], build_correlation_indexes([parent, child])
    )

    assert len(causal_edges) == 1
    assert causal_edges[0].source_id == "evidence-1"
    assert causal_edges[0].target_id == "evidence-2"
    assert associations == []


def test_root_cause_score_never_treats_an_association_only_node_as_root():
    """Two nodes connected ONLY by an association (equal timestamps, no
    real causal signal) must not have either one score/rank as a "root" -
    role must stay "uncorrelated" and the numeric score must not be
    inflated by "zero incoming edges" the way a genuine root's would be."""
    same_time = datetime.now(timezone.utc)
    a = _evidence(1, trace_id="trace-assoc", first_seen=same_time, last_seen=same_time)
    b = _evidence(2, trace_id="trace-assoc", first_seen=same_time, last_seen=same_time)

    run = run_correlation(analysis_id=1, evidence_rows=[a, b])

    assert len(run.result.components) == 1
    component = run.result.components[0]
    assert component.edges == []
    assert len(component.associations) == 1
    assert len(component.nodes) == 2

    roles = {c.node_id: c for c in run.root_causes[0]}
    assert roles["evidence-1"].role == "uncorrelated"
    assert roles["evidence-2"].role == "uncorrelated"


def test_correlation_direction_is_deterministic_regardless_of_evidence_row_order():
    """Extends the order-invariance property to the equal-timestamp case
    specifically: shuffling which record appears first in evidence_rows
    must not change whether a pair is causal vs association, nor which
    side a causal edge points from/to."""
    import random

    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="trace-shuffle", first_seen=base, last_seen=base),
        _evidence(
            2, trace_id="trace-shuffle",
            first_seen=base + timedelta(milliseconds=200),
            last_seen=base + timedelta(milliseconds=200),
        ),
        _evidence(3, trace_id="trace-shuffle", first_seen=base, last_seen=base),
    ]

    def signature(rows_order):
        causal_edges, associations = build_correlation_edges(
            rows_order, build_correlation_indexes(rows_order)
        )
        return (
            sorted((e.source_id, e.target_id) for e in causal_edges),
            sorted(tuple(sorted((a.source_id, a.target_id))) for a in associations),
        )

    baseline = signature(rows)
    shuffled = list(rows)
    random.Random(7).shuffle(shuffled)

    assert signature(shuffled) == baseline
    assert signature(list(reversed(rows))) == baseline


def test_rank_root_causes_breaks_an_exact_score_tie_by_first_line_number(monkeypatch):
    """Two nodes tying exactly on root_cause_score (a real possibility -
    several float terms of that score can coincide, e.g. two structurally
    symmetric isolated nodes) must resolve to the SAME winner regardless of
    which order they happen to appear in component.nodes - proving the
    tie-break is an explicit, first_line_number-based rule, not an
    accident of whatever order build_correlation_components (or a shuffled
    input) produced. root_cause_score itself is stubbed to force the tie
    deterministically rather than hoping real scoring inputs happen to
    coincide."""
    monkeypatch.setattr(correlation_engine_module, "root_cause_score", lambda *a, **k: 0.5)

    node_early = CorrelationNode(
        id="evidence-a", service="svc", fingerprint=None,
        first_seen=None, last_seen=None, first_line_number=50,
    )
    node_late = CorrelationNode(
        id="evidence-b", service="svc", fingerprint=None,
        first_seen=None, last_seen=None, first_line_number=100,
    )

    forward = CorrelationComponent(nodes=[node_early, node_late], edges=[], associations=[])
    reversed_order = CorrelationComponent(nodes=[node_late, node_early], edges=[], associations=[])

    ranked_forward = rank_root_causes(forward, {})
    ranked_reversed = rank_root_causes(reversed_order, {})

    # The earlier (smaller first_line_number) node wins the tie, and wins
    # it identically regardless of input list order.
    assert ranked_forward[0].node_id == "evidence-a"
    assert ranked_reversed[0].node_id == "evidence-a"


def test_build_correlation_components_node_order_is_deterministic_regardless_of_set_iteration():
    """component.nodes must reflect first_line_number order, never raw
    set() iteration order over string node-ids - Python's per-process
    string-hash randomization (this repo pins no PYTHONHASHSEED) would
    otherwise make component.nodes (and everything downstream that relies
    on its order as an implicit tie-break, e.g. rank_root_causes) vary
    between worker processes for identical input."""
    nodes = [
        CorrelationNode(
            id=f"evidence-{i}", service="svc", fingerprint=None,
            first_seen=None, last_seen=None, first_line_number=100 - i,
        )
        for i in range(1, 6)
    ]

    components = build_correlation_components(nodes, edges=[], associations=[])

    assert len(components) == 5  # no edges at all - every node is its own component
    for component in components:
        assert len(component.nodes) == 1

    # Now link them all into one component and confirm ordering.
    from app.services.correlation_engine import CorrelationEdge

    edges = [
        CorrelationEdge(
            source_id=nodes[i].id, target_id=nodes[i + 1].id,
            score=0.9, delta_ms=10.0, relationship_type="explicit_parent_child",
        )
        for i in range(len(nodes) - 1)
    ]

    [linked_component] = build_correlation_components(nodes, edges, associations=[])

    assert [node.first_line_number for node in linked_component.nodes] == sorted(
        node.first_line_number for node in nodes
    )


def test_equal_timestamp_same_request_id_is_associated_not_causal():
    """Same requirement as the trace_id case above, for request_id -
    listed as its own required regression case since match_correlation_
    signals/iter_identity_candidates treat the two id kinds as independent
    candidate sources, not because the scoring differs."""
    same_time = datetime.now(timezone.utc)
    a = _evidence(1, request_id="req-x", first_seen=same_time, last_seen=same_time)
    b = _evidence(2, request_id="req-x", first_seen=same_time, last_seen=same_time)

    causal_edges, associations = build_correlation_edges(
        [a, b], build_correlation_indexes([a, b])
    )

    assert causal_edges == []
    assert len(associations) == 1
    assert any(s.value == "request_id" for s in associations[0].signals)


def test_association_only_relationship_does_not_inflate_downstream_causal_counts():
    """A node connected to the rest of its component ONLY via association
    (equal timestamps) must contribute nothing to any other node's
    incoming/outgoing/downstream causal graph stats - those are computed
    from component.edges (causal) only, never component.associations."""
    same_time = datetime.now(timezone.utc)
    later = same_time + timedelta(milliseconds=500)

    # A causally precedes B (real positive delta, shared trace_id); C only
    # associates with A via a separate equal-timestamp request_id match -
    # event_type=None and a unique service on C/victim so NEITHER the
    # EXCEPTION nor SERVICE structural signal accidentally links C to
    # victim through the temporal-fallback path (that would defeat the
    # point of this test: C must have NO real path to victim at all).
    root = _evidence(
        1, trace_id="trace-chain", request_id="req-shared", event_type=None,
        service="svc-root", first_seen=same_time, last_seen=same_time,
    )
    victim = _evidence(
        2, trace_id="trace-chain", event_type=None, service="svc-root",
        first_seen=later, last_seen=later,
    )
    associated_only = _evidence(
        3, trace_id="trace-other", request_id="req-shared", event_type=None,
        service="svc-unrelated", first_seen=same_time, last_seen=same_time,
    )

    run = run_correlation(analysis_id=1, evidence_rows=[root, victim, associated_only])

    assert len(run.result.components) == 1
    component = run.result.components[0]
    assert len(component.nodes) == 3
    assert len(component.associations) >= 1

    stats_by_id = {c.node_id: c.graph_stats for c in run.root_causes[0]}
    # root's real causal edge to victim is unaffected by the association.
    assert stats_by_id["evidence-1"].outgoing_count == 1
    assert stats_by_id["evidence-1"].downstream_count == 1
    # The association-only node contributes zero incoming/outgoing/
    # downstream to ANY node's causal graph stats.
    assert stats_by_id["evidence-3"].incoming_count == 0
    assert stats_by_id["evidence-3"].outgoing_count == 0
    assert stats_by_id["evidence-3"].downstream_count == 0
    roles = {c.node_id: c.role for c in run.root_causes[0]}
    assert roles["evidence-3"] == "uncorrelated"


# --- explicit_parent_child vs inferred_propagation vs association -----------


def test_scenario_n_exact_parent_span_is_relationship_type_explicit_parent_child():
    same_time = datetime.now(timezone.utc)
    parent = _evidence(
        1, source_format="opentelemetry", trace_id="trace-n", span_id="span-parent",
        first_seen=same_time, last_seen=same_time,
    )
    child = _evidence(
        2, source_format="opentelemetry", trace_id="trace-n", parent_span_id="span-parent",
        first_seen=same_time, last_seen=same_time,
    )

    directed_edges, associations = build_correlation_edges(
        [parent, child], build_correlation_indexes([parent, child])
    )

    assert len(directed_edges) == 1
    assert associations == []
    edge = directed_edges[0]
    # Exact parent.span_id == child.parent_span_id proves DIRECTION, not
    # physical failure causation - see correlation_engine.CorrelationEdge.
    assert edge.relationship_type == "explicit_parent_child"
    assert edge.direction_confidence == 1.0


def test_scenario_q_positive_time_shared_identity_is_inferred_propagation_with_exact_delta():
    base = datetime.now(timezone.utc)
    earlier = _evidence(1, trace_id="trace-q", first_seen=base, last_seen=base)
    later = _evidence(
        2, trace_id="trace-q",
        first_seen=base + timedelta(milliseconds=27.4),
        last_seen=base + timedelta(milliseconds=27.4),
    )

    directed_edges, associations = build_correlation_edges(
        [earlier, later], build_correlation_indexes([earlier, later])
    )

    assert len(directed_edges) == 1
    assert associations == []
    edge = directed_edges[0]
    assert edge.relationship_type == "inferred_propagation"
    assert edge.delta_ms == pytest.approx(27.4)
    assert edge.direction_confidence is not None
    assert 0.0 < edge.direction_confidence <= 1.0
    # Not mislabeled as an explicit, proven relationship - that value is
    # reserved for verified parent-span relationships only.
    assert edge.relationship_type != "explicit_parent_child"


def test_associations_never_carry_a_relationship_type_or_direction_confidence():
    same_time = datetime.now(timezone.utc)
    a = _evidence(1, trace_id="trace-assoc2", first_seen=same_time, last_seen=same_time)
    b = _evidence(2, trace_id="trace-assoc2", first_seen=same_time, last_seen=same_time)

    causal_edges, associations = build_correlation_edges(
        [a, b], build_correlation_indexes([a, b])
    )

    assert causal_edges == []
    assert len(associations) == 1
    assert associations[0].relationship_type is None
    assert associations[0].direction_confidence is None


def test_closer_in_time_inferred_propagation_has_higher_direction_confidence():
    """direction_confidence reuses the existing temporal_score decay -
    closer in time supports the propagation hypothesis more strongly."""
    base = datetime.now(timezone.utc)
    close_rows = [
        _evidence(1, trace_id="trace-close", first_seen=base, last_seen=base),
        _evidence(
            2, trace_id="trace-close",
            first_seen=base + timedelta(milliseconds=5),
            last_seen=base + timedelta(milliseconds=5),
        ),
    ]
    far_rows = [
        _evidence(3, trace_id="trace-far", first_seen=base, last_seen=base),
        _evidence(
            4, trace_id="trace-far",
            first_seen=base + timedelta(seconds=4),
            last_seen=base + timedelta(seconds=4),
        ),
    ]

    close_edge = build_correlation_edges(close_rows, build_correlation_indexes(close_rows))[0][0]
    far_edge = build_correlation_edges(far_rows, build_correlation_indexes(far_rows))[0][0]

    assert close_edge.direction_confidence > far_edge.direction_confidence