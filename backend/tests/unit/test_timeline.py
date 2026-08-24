"""A real per-component timeline built from correlation's
already-loaded nodes/roles - never a second DB scan, never fabricated
ordering for missing/equal timestamps.
"""
from datetime import datetime, timedelta, timezone

from app.models.evidence import Evidence
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import build_correlation_payload
from app.services.timeline_processor import build_component_timeline


def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": evidence_id,
        "fingerprint": f"fp-{evidence_id}",
        "event_type": None,
        "severity": "ERROR",
        "occurrence_count": 1,
        "source_format": "generic",
        "first_line_number": 1,
        "last_line_number": 1,
    }
    defaults.update(kwargs)
    return Evidence(**defaults)


def test_scenario_s_millisecond_propagation_timeline_has_exact_relative_ms():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    root = _evidence(1, trace_id="trace-s", service="payment-api", first_seen=base)
    impact = _evidence(
        2, trace_id="trace-s", service="orders-db",
        first_seen=base + timedelta(milliseconds=18),
    )
    gateway_failure = _evidence(
        3, trace_id="trace-s", service="gateway",
        first_seen=base + timedelta(milliseconds=49),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[root, impact, gateway_failure])
    payload = build_correlation_payload(run, [root, impact, gateway_failure])

    timeline = payload["components"][0]["timeline"]
    assert [entry["relative_ms"] for entry in timeline] == [0.0, 18.0, 49.0]
    assert [entry["service"] for entry in timeline] == [
        "payment-api", "orders-db", "gateway",
    ]
    assert timeline[0]["role"] == "root"
    assert timeline[-1]["role"] == "victim"
    for entry in timeline:
        assert entry["timestamp"] is not None


def test_equal_timestamp_events_share_the_same_relative_ms():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    a = _evidence(1, trace_id="trace-eq", service="svc-a", first_seen=base)
    b = _evidence(2, trace_id="trace-eq", service="svc-b", first_seen=base)

    run = run_correlation(analysis_id=1, evidence_rows=[a, b])
    payload = build_correlation_payload(run, [a, b])

    timeline = payload["components"][0]["timeline"]
    assert len(timeline) == 2
    assert timeline[0]["relative_ms"] == timeline[1]["relative_ms"] == 0.0


def test_events_with_no_timestamp_are_never_assigned_fake_ordering():
    component_nodes_source = [
        _evidence(1, trace_id="trace-none", service="svc-a", first_seen=None),
        _evidence(
            2, trace_id="trace-none", service="svc-b",
            first_seen=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=component_nodes_source)
    payload = build_correlation_payload(run, component_nodes_source)

    timeline = payload["components"][0]["timeline"]
    timed = [e for e in timeline if e["timestamp"] is not None]
    untimed = [e for e in timeline if e["timestamp"] is None]

    assert len(timed) == 1
    assert len(untimed) == 1
    assert untimed[0]["relative_ms"] is None
    assert untimed[0]["node_id"] == "evidence-1"


def test_build_component_timeline_is_pure_in_memory_no_db_access():
    """Direct unit-level proof this never touches the database - it only
    reads attributes already present on the in-memory objects."""
    from types import SimpleNamespace

    node_a = SimpleNamespace(
        id="evidence-1", first_seen=datetime(2026, 1, 1, tzinfo=timezone.utc), service="a",
    )
    node_b = SimpleNamespace(
        id="evidence-2",
        first_seen=datetime(2026, 1, 1, 0, 0, 0, 5000, tzinfo=timezone.utc),
        service="b",
    )
    component = SimpleNamespace(nodes=[node_b, node_a])  # deliberately out of order
    root_candidates = [
        SimpleNamespace(node_id="evidence-1", role="root"),
        SimpleNamespace(node_id="evidence-2", role="victim"),
    ]

    timeline = build_component_timeline(component, root_candidates)

    assert [e["node_id"] for e in timeline] == ["evidence-1", "evidence-2"]
    assert timeline[0]["relative_ms"] == 0.0
    assert timeline[1]["relative_ms"] == 5.0
