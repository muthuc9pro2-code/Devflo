"""Same-format multi-artifact and cross-format correlation quality (Task:
correlation depth pass, §4 and §5).

Confirms: artifact boundaries never prevent legitimate correlation and
never create artificial correlation (artifact_id is provenance-only, never
a matching signal - verified directly here, not just by prior code
reading); format differences are bridged conservatively via
FORMAT_SIGNAL_PRIORITY's min()-of-both-sides calibration; weak/coincidental
signals (shared host, shared HTTP status, close timestamps) alone do not
manufacture a relationship across formats.
"""
from datetime import datetime, timedelta, timezone

from app.models.evidence import Evidence
from app.services.correlation_engine import (
    build_correlation_edges,
    build_correlation_indexes,
    has_structural_match,
    match_correlation_signals,
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


# --- §4 Same-format, multiple independent artifacts ---------------------


def test_same_format_different_artifacts_correlate_on_real_shared_evidence():
    """Two independent Nginx access-log artifacts (e.g. two load-balanced
    instances) that genuinely handled the same request must still
    correlate - the artifact boundary must not block it."""
    base = datetime.now(timezone.utc)
    server_1 = _evidence(
        1, artifact_id=101, source_format="web_server",
        request_id="req-shared", endpoint="/checkout", first_seen=base,
    )
    server_2 = _evidence(
        2, artifact_id=202, source_format="web_server",
        request_id="req-shared", endpoint="/checkout",
        first_seen=base + timedelta(milliseconds=50),
    )

    causal_edges, associations = build_correlation_edges(
        [server_1, server_2], build_correlation_indexes([server_1, server_2])
    )

    assert len(causal_edges) == 1
    assert associations == []
    assert any(s.value == "request_id" for s in causal_edges[0].signals)


def test_same_format_different_artifacts_do_not_artificially_correlate():
    """Two independent Nginx logs with genuinely nothing in common must
    stay separate - being the same format/artifact-adjacent is not itself
    evidence."""
    base = datetime.now(timezone.utc)
    server_1 = _evidence(
        1, artifact_id=101, source_format="web_server", endpoint="/a", host="host-a", first_seen=base
    )
    server_2 = _evidence(
        2, artifact_id=202, source_format="web_server", endpoint="/b", host="host-b",
        first_seen=base + timedelta(hours=3),
    )

    causal_edges, associations = build_correlation_edges(
        [server_1, server_2], build_correlation_indexes([server_1, server_2])
    )

    assert causal_edges == []
    assert associations == []


def test_artifact_id_is_never_itself_a_matching_signal():
    """Two evidence rows from the SAME artifact_id, otherwise unrelated,
    must not correlate just because they came from the same file - and
    conversely two rows sharing everything else but different artifact_id
    must still correlate. artifact_id must have zero influence on
    match_correlation_signals either way."""
    base = datetime.now(timezone.utc)
    same_artifact_unrelated = [
        _evidence(1, artifact_id=5, service="svc-a", first_seen=base),
        _evidence(2, artifact_id=5, service="svc-b", first_seen=base + timedelta(hours=1)),
    ]
    assert match_correlation_signals(*same_artifact_unrelated) == []

    different_artifact_related = [
        _evidence(3, artifact_id=5, trace_id="trace-x"),
        _evidence(4, artifact_id=9, trace_id="trace-x"),
    ]
    matches = match_correlation_signals(*different_artifact_related)
    assert any(m.signal.value == "trace_id" for m in matches)


# --- §5 Cross-format ------------------------------------------------------


def test_legitimate_cross_format_incident_via_shared_trace_id():
    """generic app log + web_server + database + opentelemetry + cloud_gateway,
    all genuinely part of one request via a shared trace_id, must form one
    connected incident."""
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, source_format="cloud_gateway", trace_id="trace-1", service="gateway", first_seen=base),
        _evidence(2, source_format="web_server", trace_id="trace-1", service="checkout-api", first_seen=base + timedelta(milliseconds=20)),
        _evidence(3, source_format="generic", trace_id="trace-1", service="checkout-api", first_seen=base + timedelta(milliseconds=60)),
        _evidence(4, source_format="database", trace_id="trace-1", service="orders-db", first_seen=base + timedelta(milliseconds=120)),
        _evidence(5, source_format="opentelemetry", trace_id="trace-1", span_id="span-5", service="orders-db", first_seen=base + timedelta(milliseconds=130)),
    ]

    run = run_correlation(analysis_id=1, evidence_rows=rows)

    assert len(run.result.components) == 1
    assert len(run.result.components[0].nodes) == 5


def test_conflicting_trace_ids_across_formats_do_not_merge():
    left = _evidence(1, source_format="web_server", trace_id="trace-A")
    right = _evidence(2, source_format="database", trace_id="trace-B")

    matches = match_correlation_signals(left, right)

    assert not any(m.signal.value == "trace_id" for m in matches)


def test_shared_host_alone_across_formats_is_not_sufficient_far_apart():
    """Two different formats reporting from the same host, hours apart,
    with nothing else shared, must not correlate - shared infrastructure
    alone is not an incident signal."""
    base = datetime.now(timezone.utc)
    left = _evidence(1, source_format="syslog", host="prod-host-1", first_seen=base)
    right = _evidence(
        2, source_format="container", host="prod-host-1",
        first_seen=base + timedelta(hours=6),
    )

    causal_edges, associations = build_correlation_edges([left, right], build_correlation_indexes([left, right]))

    assert causal_edges == []
    assert associations == []


def test_same_http_status_alone_is_never_a_structural_match():
    """http_status is intentionally NOT in has_structural_match's signal
    set - two unrelated requests that both happened to 500 must not be
    treated as structurally related, even within the temporal window
    (only genuinely identity/service/host/etc-linked pairs are)."""
    base = datetime.now(timezone.utc)
    left = _evidence(1, source_format="cloud_gateway", http_status=500, first_seen=base)
    right = _evidence(
        2, source_format="web_server", http_status=500,
        first_seen=base + timedelta(seconds=1),
    )

    assert has_structural_match(left, right) is False

    causal_edges, associations = build_correlation_edges([left, right], build_correlation_indexes([left, right]))
    assert causal_edges == []
    assert associations == []


def test_close_timestamps_without_structural_evidence_do_not_correlate_cross_format():
    base = datetime.now(timezone.utc)
    left = _evidence(1, source_format="ci_cd", service="build-pipeline", first_seen=base)
    right = _evidence(
        2, source_format="serverless", service="image-processor",
        first_seen=base + timedelta(seconds=1),
    )

    causal_edges, associations = build_correlation_edges([left, right], build_correlation_indexes([left, right]))

    assert causal_edges == []
    assert associations == []


def test_signal_priority_gating_uses_the_weaker_side_not_just_one():
    """web_server rates ENDPOINT as a real signal, database does not
    declare it at all (verified directly against FORMAT_SIGNAL_PRIORITY,
    not assumed) - _pair_strength's min() must fall back to None (no
    match) when either side omits the signal, not silently use only the
    declaring side's rating."""
    left = _evidence(1, source_format="web_server", endpoint="/checkout")
    right = _evidence(2, source_format="database", endpoint="/checkout")

    matches = match_correlation_signals(left, right)

    assert not any(m.signal.value == "endpoint" for m in matches)


def test_trace_id_and_request_id_are_universally_declared_across_all_13_formats():
    """Confirms by direct inspection (not assumption) that every supported
    format can participate in the two strongest identity signals - the gap
    found during this audit is elsewhere (module/host/endpoint/fingerprint
    are legitimately format-specific), not in the core identity signals."""
    from app.services.correlation_engine import (
        FORMAT_SIGNAL_PRIORITY,
        CorrelationSignal,
    )

    for fmt, priorities in FORMAT_SIGNAL_PRIORITY.items():
        assert CorrelationSignal.TRACE_ID in priorities, fmt
        assert CorrelationSignal.REQUEST_ID in priorities, fmt


def test_correlation_content_is_deterministic_regardless_of_evidence_row_order():
    """Proves the concrete concurrency-safety claim: since Task 1's
    cross-artifact concurrency means artifacts can finish in any order,
    what actually matters is that the ANSWER (which nodes group together,
    every edge and its score, every node's role and root-cause score)
    never depends on evidence_rows list order.

    NOTE - a real finding from writing this test: component LIST INDEX
    (used as the dict key into run.root_causes, and as "id" in
    build_correlation_payload) is assigned by connected-components
    DISCOVERY order, which does follow evidence_rows order - shuffling the
    input changed which index (0 vs 1) each component landed at, even
    though its nodes/edges/roles/scores were byte-identical. This is safe
    in production only because app/tasks/analysis.py always feeds
    run_correlation the same deterministic `ORDER BY first_seen, id` query
    regardless of concurrent artifact completion order (Task 1) - it is a
    property of that caller, not a guarantee run_correlation makes on its
    own. Documented, not fixed: changing component indexing to a stable
    key (e.g. sorted by earliest first_seen) would be a real behavioral
    change requiring its own justification, and the actual production
    path is already safe.
    """
    import random

    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, source_format="cloud_gateway", trace_id="trace-1", service="gateway", first_seen=base),
        _evidence(2, source_format="web_server", trace_id="trace-1", service="checkout-api", first_seen=base + timedelta(milliseconds=20)),
        _evidence(3, source_format="database", trace_id="trace-1", service="orders-db", first_seen=base + timedelta(milliseconds=120)),
        _evidence(4, source_format="generic", service="unrelated", first_seen=base + timedelta(hours=2)),
    ]

    def content_signature(run):
        role_by_node = {
            candidate.node_id: (candidate.role, round(candidate.score, 6))
            for candidates in run.root_causes.values()
            for candidate in candidates
        }
        return (
            sorted(
                tuple(sorted(n.id for n in c.nodes)) for c in run.result.components
            ),
            sorted(
                (e.source_id, e.target_id, round(e.score, 6))
                for c in run.result.components
                for e in c.edges
            ),
            sorted(role_by_node.items()),
        )

    baseline = content_signature(run_correlation(analysis_id=1, evidence_rows=rows))

    shuffled = list(rows)
    random.Random(42).shuffle(shuffled)
    reversed_order = list(reversed(rows))

    assert content_signature(run_correlation(analysis_id=1, evidence_rows=shuffled)) == baseline
    assert content_signature(run_correlation(analysis_id=1, evidence_rows=reversed_order)) == baseline
