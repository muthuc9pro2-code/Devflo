from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from app.models.evidence import Evidence
from app.services import correlation_engine
from app.services.correlation_engine import (
    CorrelationSignal,
    CorrelationSignalMatch,
    SignalStrength,
    _adaptive_temporal_window_ms,
    build_correlation_edges,
    build_correlation_indexes,
    iter_temporal_candidates,
    iter_valid_temporal_candidates,
    run_correlation,
)

def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "event_type": None,
        "severity": "ERROR",
        "occurrence_count": 1,
        "first_seen": datetime.now(timezone.utc),
        "last_seen": datetime.now(timezone.utc),
        "source_format": "generic",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)

def test_adaptive_window_strong_structural_signal_gets_the_full_max():
    a = _evidence(1, source_format="database", service="orders-db")
    b = _evidence(2, source_format="database", service="orders-db")

    assert _adaptive_temporal_window_ms(a, b, 5000.0) == 5000.0

def test_adaptive_window_medium_structural_signal_gets_2500ms():
    a = _evidence(1, source_format="generic", service="worker")
    b = _evidence(2, source_format="generic", service="worker")

    assert _adaptive_temporal_window_ms(a, b, 5000.0) == 2500.0

def test_adaptive_window_weak_structural_signal_gets_1000ms():
    a = _evidence(1)
    b = _evidence(2)
    weak_match = [
        CorrelationSignalMatch(signal=CorrelationSignal.SERVICE, strength=SignalStrength.LOW)
    ]

    with patch.object(correlation_engine, "match_correlation_signals", return_value=weak_match):
        assert _adaptive_temporal_window_ms(a, b, 5000.0) == 1000.0

def test_adaptive_window_no_structural_signal_is_rejected_outright():
    a = _evidence(1, source_format="generic", service="svc-x")
    b = _evidence(2, source_format="generic", service="svc-y")

    assert _adaptive_temporal_window_ms(a, b, 5000.0) == 0.0

def test_adaptive_window_never_exceeds_the_provided_max():
    a = _evidence(1, source_format="database", service="orders-db")
    b = _evidence(2, source_format="database", service="orders-db")

    assert _adaptive_temporal_window_ms(a, b, 2000.0) == 2000.0

def test_stronger_supporting_evidence_survives_a_wider_separation():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="database", service="orders-db", first_seen=base)
    b = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=4000),
    )

    candidates = list(iter_temporal_candidates([a, b]))
    assert candidates == [(a, b)]

def test_weak_medium_evidence_requires_a_tighter_separation():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="generic", service="worker", first_seen=base)
    close = _evidence(
        2, source_format="generic", service="worker",
        first_seen=base + timedelta(milliseconds=2000),
    )
    far = _evidence(
        3, source_format="generic", service="worker",
        first_seen=base + timedelta(milliseconds=3000),
    )

    close_candidates = list(iter_temporal_candidates([a, close]))
    assert close_candidates == [(a, close)]

    far_candidates = list(iter_temporal_candidates([a, far]))
    assert far_candidates == []

def test_timestamp_only_evidence_cannot_exploit_the_full_5s_window():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="generic", service="svc-x", first_seen=base)
    b = _evidence(
        2, source_format="generic", service="svc-y",
        first_seen=base + timedelta(milliseconds=1500),
    )

    assert list(iter_temporal_candidates([a, b])) == []
    assert list(iter_valid_temporal_candidates([a, b])) == []

def test_5000ms_outer_envelope_still_caps_even_a_strong_signal_pair():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="database", service="orders-db", first_seen=base)
    beyond_cap = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=5001),
    )

    assert list(iter_temporal_candidates([a, beyond_cap])) == []

def test_delta_ms_on_the_edge_is_the_real_measured_difference_not_the_window():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="database", service="orders-db", first_seen=base)
    b = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=3000),
    )

    causal_edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert len(causal_edges) == 1
    assert associations == []
    assert causal_edges[0].delta_ms == 3000.0

def test_shared_trace_id_never_enters_the_temporal_fallback_path_at_all():
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, source_format="generic", service="svc-x", trace_id="trace-1", first_seen=base
    )
    b = _evidence(
        2, source_format="generic", service="svc-y", trace_id="trace-1",
        first_seen=base + timedelta(milliseconds=4900),
    )

    assert list(iter_temporal_candidates([a, b])) == []

def test_strong_identity_correlation_result_is_unchanged():
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, source_format="generic", service="svc-x", trace_id="trace-1", first_seen=base
    )
    b = _evidence(
        2, source_format="generic", service="svc-y", trace_id="trace-1",
        first_seen=base + timedelta(milliseconds=4900),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[a, b])

    assert len(run.result.components) == 1
    assert len(run.result.components[0].edges) == 1
    assert "trace_id" in [s.value for s in run.result.components[0].edges[0].signals]

def test_cross_artifact_strong_signal_pair_still_correlates():
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, artifact_id=101, source_format="database", service="orders-db",
        fingerprint="fp-shared", first_seen=base,
    )
    b = _evidence(
        2, artifact_id=202, source_format="database", service="orders-db",
        fingerprint="fp-shared", first_seen=base + timedelta(milliseconds=4000),
    )

    causal_edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert len(causal_edges) == 1
    assert associations == []
    assert causal_edges[0].source_id == "evidence-1"
    assert causal_edges[0].target_id == "evidence-2"

def test_cross_artifact_single_structural_signal_alone_is_not_sufficient():
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, artifact_id=101, source_format="database", service="orders-db", first_seen=base
    )
    b = _evidence(
        2, artifact_id=202, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=4000),
    )

    causal_edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert causal_edges == []
    assert associations == []

def test_two_nearby_unsupported_incidents_stay_separate_components():
    base = datetime.now(timezone.utc)
    a = _evidence(1, service="svc-a", module="mod-a", first_seen=base)
    b = _evidence(
        2, service="svc-b", module="mod-b",
        first_seen=base + timedelta(milliseconds=1500),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[a, b])

    assert len(run.result.components) == 2
    assert all(len(c.edges) == 0 for c in run.result.components)

def test_image_evidence_participates_through_the_same_generic_mechanism():
    base = datetime.now(timezone.utc)
    screenshot = _evidence(
        1, source_format="image", service="checkout-ui", source_file="terminal.png",
        first_seen=base,
    )
    close = _evidence(
        2, source_format="generic", service="checkout-ui",
        first_seen=base + timedelta(milliseconds=2000),
    )
    far = _evidence(
        3, source_format="generic", service="checkout-ui",
        first_seen=base + timedelta(milliseconds=3000),
    )

    assert list(iter_temporal_candidates([screenshot, close])) == [(screenshot, close)]
    assert list(iter_temporal_candidates([screenshot, far])) == []

def test_source_matches_survive_untouched_through_temporal_correlation():
    matches = [{"relative_path": "src/checkout.tsx", "line_number": 10}]
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, source_format="database", service="orders-db", source_matches=matches, first_seen=base
    )
    b = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=3000),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[a, b])

    node = next(n for n in run.result.components[0].nodes if n.id == "evidence-1")
    assert node.source_matches == matches
    assert not any(
        signal.value == "source" for edge in run.result.components[0].edges for signal in edge.signals
    )

def test_dag_and_root_cause_output_unchanged_for_an_unaffected_fixture():
    base = datetime.now(timezone.utc)
    database = _evidence(
        1, source_format="database", trace_id="trace-1", service="database",
        first_seen=base, last_seen=base,
    )
    payment = _evidence(
        2, source_format="opentelemetry", trace_id="trace-1", span_id="payment-span",
        service="payment", first_seen=base + timedelta(milliseconds=100),
        last_seen=base + timedelta(milliseconds=100),
    )
    api = _evidence(
        3, source_format="web_server", trace_id="trace-1", service="api", http_status=500,
        first_seen=base + timedelta(milliseconds=250), last_seen=base + timedelta(milliseconds=250),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[database, payment, api])

    assert len(run.result.components) == 1
    component = run.result.components[0]
    assert len(component.nodes) == 3
    assert component.edges

    roles = {candidate.node_id: candidate.role for candidate in run.root_causes[0]}
    assert roles["evidence-1"] == "root"
    assert roles["evidence-2"] == "propagation"
    assert roles["evidence-3"] == "victim"
