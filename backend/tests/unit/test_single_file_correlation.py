from datetime import datetime, timedelta, timezone
from app.models.evidence import Evidence
from app.services.correlation_engine import (
    build_correlation_edges,
    build_correlation_indexes,
    run_correlation,
)

ARTIFACT = 1

def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": ARTIFACT,
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

def test_strong_single_file_causal_chain_no_trace_id_needed():
    base = datetime.now(timezone.utc)
    origin = _evidence(
        1, service="checkout-api", module="db-pool", event_type="PoolExhaustedError",
        endpoint="/checkout", first_seen=base, last_seen=base,
    )
    symptom = _evidence(
        2, service="checkout-api", endpoint="/checkout", http_status=504,
        first_seen=base + timedelta(milliseconds=500), last_seen=base + timedelta(milliseconds=500),
    )
    victim = _evidence(
        3, service="checkout-api", event_type="RetryExhausted", endpoint="/checkout",
        first_seen=base + timedelta(seconds=1), last_seen=base + timedelta(seconds=1),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[origin, symptom, victim])

    assert len(run.result.components) == 1
    component = run.result.components[0]
    assert len(component.nodes) == 3
    assert component.edges

    roles = {c.node_id: c.role for c in run.root_causes[0]}
    assert roles["evidence-1"] == "root"
    assert roles["evidence-3"] == "victim"

def test_sparse_single_file_evidence_stays_uncorrelated():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, service="svc-a", endpoint="/a", first_seen=base),
        _evidence(2, service="svc-b", endpoint="/b", first_seen=base + timedelta(hours=2)),
        _evidence(3, service="svc-c", endpoint="/c", first_seen=base + timedelta(hours=5)),
    ]

    run = run_correlation(analysis_id=1, evidence_rows=rows)

    assert len(run.result.components) == 3
    assert all(len(c.edges) == 0 for c in run.result.components)
    assert all(c.role == "uncorrelated" for c in run.root_causes[0] + run.root_causes[1] + run.root_causes[2])

def test_repeated_same_failure_in_a_burst_correlates_via_fingerprint():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(
            i, service="worker", fingerprint="nullpointerexception:item-processor",
            first_seen=base + timedelta(seconds=i),
        )
        for i in range(1, 4)
    ]

    causal_edges, associations = build_correlation_edges(rows, build_correlation_indexes(rows))

    assert len(causal_edges) > 0
    assert all("fingerprint" in [s.value for s in edge.signals] for edge in causal_edges)
    assert associations == []

def test_KNOWN_GAP_same_fingerprint_beyond_the_temporal_window_gets_no_edge():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(
            i, service="worker", fingerprint="nullpointerexception:item-processor",
            first_seen=base + timedelta(minutes=i * 20),
        )
        for i in range(1, 4)
    ]

    causal_edges, associations = build_correlation_edges(rows, build_correlation_indexes(rows))

    assert causal_edges == []
    assert associations == []

def test_unrelated_errors_close_in_time_do_not_correlate_without_structural_evidence():
    base = datetime.now(timezone.utc)
    a = _evidence(1, service="svc-a", module="mod-a", endpoint="/a", host="host-a", first_seen=base)
    b = _evidence(
        2, service="svc-b", module="mod-b", endpoint="/b", host="host-b",
        first_seen=base + timedelta(seconds=2),
    )

    causal_edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert causal_edges == []
    assert associations == []

def test_same_exception_type_but_unrelated_identities_and_time_do_not_correlate():
    base = datetime.now(timezone.utc)
    a = _evidence(1, event_type="TimeoutError", service="svc-a", first_seen=base)
    b = _evidence(
        2, event_type="TimeoutError", service="svc-b",
        first_seen=base + timedelta(hours=6),
    )

    causal_edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert causal_edges == []
    assert associations == []

def test_same_exception_type_close_in_time_is_a_defensible_heuristic_link():
    base = datetime.now(timezone.utc)
    a = _evidence(1, event_type="TimeoutError", service="svc-a", first_seen=base)
    b = _evidence(
        2, event_type="TimeoutError", service="svc-a",
        first_seen=base + timedelta(seconds=1),
    )

    causal_edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert len(causal_edges) == 1
    assert associations == []
    assert "exception" in [s.value for s in causal_edges[0].signals] or "service" in [
        s.value for s in causal_edges[0].signals
    ]

def test_upstream_root_followed_by_downstream_victim_preserves_direction():
    base = datetime.now(timezone.utc)
    root = _evidence(
        1, service="api", event_type="ConnectionRefused", endpoint="/orders", first_seen=base
    )
    victim = _evidence(
        2, service="api", event_type="RequestFailed", endpoint="/orders",
        first_seen=base + timedelta(milliseconds=800),
    )

    causal_edges, associations = build_correlation_edges(
        [root, victim], build_correlation_indexes([root, victim])
    )

    assert len(causal_edges) == 1
    assert associations == []
    assert causal_edges[0].source_id == "evidence-1"
    assert causal_edges[0].target_id == "evidence-2"
    assert causal_edges[0].delta_ms == 800.0
