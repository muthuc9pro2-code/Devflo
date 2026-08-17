from datetime import datetime, timedelta, timezone

from app.models.evidence import Evidence
from app.services.correlation_engine import (
    build_correlation_indexes,
    match_correlation_signals,
    match_parent_span,
    run_correlation,
    temporal_score,
)


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
    assert indexes.span_ids["span-1"] is evidence
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