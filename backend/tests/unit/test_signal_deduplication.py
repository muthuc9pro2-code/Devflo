from datetime import datetime, timezone
from app.models.evidence import Evidence
from app.services.correlation_engine import (
    CorrelationSignal,
    SignalStrength,
    build_correlation_edges,
    build_correlation_indexes,
    has_genuine_correlatable_structure,
    match_correlation_signals,
    match_parent_span,
    score_candidate_pair,
    score_signal_matches,
)

def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "occurrence_count": 1,
        "first_seen": datetime.now(timezone.utc),
        "last_seen": datetime.now(timezone.utc),
        "source_format": "generic",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)

def _signals(matches):
    return {match.signal for match in matches}

def test_shared_trace_id_and_derived_resolved_identity_do_not_double_count():
    left = _evidence(1, trace_id="trace-1", resolved_identity="trace:trace-1")
    right = _evidence(2, trace_id="trace-1", resolved_identity="trace:trace-1")

    matches = match_correlation_signals(left, right)

    signals = _signals(matches)
    assert CorrelationSignal.TRACE_ID in signals
    assert CorrelationSignal.RESOLVED_IDENTITY not in signals

    trace_matches = [m for m in matches if m.signal == CorrelationSignal.TRACE_ID]
    assert len(trace_matches) == 1

def test_shared_request_id_and_derived_resolved_identity_do_not_double_count():
    left = _evidence(1, request_id="req-1", resolved_identity="request:req-1")
    right = _evidence(2, request_id="req-1", resolved_identity="request:req-1")

    matches = match_correlation_signals(left, right)

    signals = _signals(matches)
    assert CorrelationSignal.REQUEST_ID in signals
    assert CorrelationSignal.RESOLVED_IDENTITY not in signals

def test_resolved_identity_still_works_as_a_fallback_without_a_raw_match():
    left = _evidence(1, resolved_identity="custom:shared-identity")
    right = _evidence(2, resolved_identity="custom:shared-identity")

    matches = match_correlation_signals(left, right)

    assert CorrelationSignal.RESOLVED_IDENTITY in _signals(matches)

def test_matching_trace_id_and_request_id_both_remain_as_distinct_raw_signals():
    left = _evidence(1, trace_id="trace-1", request_id="req-1")
    right = _evidence(2, trace_id="trace-1", request_id="req-1")

    signals = _signals(match_correlation_signals(left, right))

    assert CorrelationSignal.TRACE_ID in signals
    assert CorrelationSignal.REQUEST_ID in signals

def test_parent_span_signal_is_unaffected_and_stays_independent():
    same_time = datetime.now(timezone.utc)
    parent = _evidence(
        1, source_format="opentelemetry", trace_id="trace-p", span_id="span-parent",
        first_seen=same_time, last_seen=same_time,
    )
    child = _evidence(
        2, source_format="opentelemetry", trace_id="trace-p", parent_span_id="span-parent",
        resolved_identity="trace:trace-p", first_seen=same_time, last_seen=same_time,
    )

    assert match_parent_span(parent, child) is not None

    score, _delta_ms, matches = score_candidate_pair(parent, child)
    signals = _signals(matches)
    assert CorrelationSignal.PARENT_SPAN in signals
    assert CorrelationSignal.TRACE_ID in signals
    assert CorrelationSignal.RESOLVED_IDENTITY not in signals
    assert score > 0.0

def test_structural_signals_still_combine_via_noisy_or():
    left = _evidence(1, service="checkout", module="payments")
    right = _evidence(2, service="checkout", module="payments")

    matches = match_correlation_signals(left, right)
    signals = _signals(matches)
    assert CorrelationSignal.SERVICE in signals
    assert CorrelationSignal.MODULE in signals

    expected = 1.0 - (1.0 - SignalStrength.MEDIUM.value) * (1.0 - SignalStrength.MEDIUM.value)
    assert score_signal_matches(matches) == expected

def test_routing_and_edge_construction_agree_on_a_resolved_identity_fallback_pair():
    rows = [
        _evidence(1, resolved_identity="custom:shared", service="svc"),
        _evidence(2, resolved_identity="custom:shared", service="svc"),
    ]

    assert has_genuine_correlatable_structure(rows) is True

    edges, associations = build_correlation_edges(rows, build_correlation_indexes(rows))
    assert len(edges) + len(associations) == 1

def test_shared_trace_id_alone_still_correlates_end_to_end():
    left = _evidence(1, trace_id="trace-x")
    right = _evidence(2, trace_id="trace-x")

    edges, associations = build_correlation_edges(
        [left, right], build_correlation_indexes([left, right])
    )

    assert len(edges) + len(associations) == 1

def test_shared_request_id_and_service_still_correlates_end_to_end():
    left = _evidence(1, request_id="req-x", service="api")
    right = _evidence(2, request_id="req-x", service="api")

    edges, associations = build_correlation_edges(
        [left, right], build_correlation_indexes([left, right])
    )

    assert len(edges) + len(associations) == 1
