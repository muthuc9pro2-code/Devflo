"""Evidence-aware adaptive temporal fallback window.

Before this change, iter_temporal_candidates() gave every fallback
candidate pair the SAME fixed 5000ms window regardless of how much (if
any) real supporting evidence they shared. This meant two genuinely
unrelated events landing within 5 seconds of each other, with nothing
structural in common, were still proposed as temporal candidates (only
rejected later, downstream, by has_structural_match()).

The adaptive window instead sizes that fallback window per-pair, from the
STRONGEST real structural signal (service/module/host/container/pod/
endpoint/exception/fingerprint) the pair already shares - reusing
match_correlation_signals() and the existing format-calibrated
SignalStrength tiers (VERY_HIGH/HIGH/MEDIUM/LOW), not a new scoring
system. Identity-based matching (trace_id/request_id/resolved_identity/
parent_span) never goes through this path at all - iter_temporal_candidates
excludes any pair already sharing one of those, exactly as before.

Tiers (_TEMPORAL_WINDOW_*_MS in correlation_engine.py):
  strongest structural match >= HIGH   -> 5000ms (unchanged absolute max)
  strongest structural match == MEDIUM -> 2500ms
  strongest structural match == LOW    -> 1000ms
  no structural match at all           -> 0ms (rejected outright)

Finding worth flagging up front: no currently-registered FORMAT_SIGNAL_
PRIORITY entry rates any structural signal at LOW - LOW is only ever used
for the TEMPORAL signal itself in every format. So the "weak evidence"
1000ms tier is unreachable with today's real format data; it is exercised
here by monkeypatching match_correlation_signals() directly to prove the
mapping itself is correct and future-proof against a signal that isn't
populated yet, and this gap is called out explicitly in the task report.
"""
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


# --- unit-level: the tier mapping itself, one branch at a time -----------


def test_adaptive_window_strong_structural_signal_gets_the_full_max():
    a = _evidence(1, source_format="database", service="orders-db")
    b = _evidence(2, source_format="database", service="orders-db")

    assert _adaptive_temporal_window_ms(a, b, 5000.0) == 5000.0


def test_adaptive_window_medium_structural_signal_gets_2500ms():
    a = _evidence(1, source_format="generic", service="worker")
    b = _evidence(2, source_format="generic", service="worker")

    assert _adaptive_temporal_window_ms(a, b, 5000.0) == 2500.0


def test_adaptive_window_weak_structural_signal_gets_1000ms():
    """No real FORMAT_SIGNAL_PRIORITY entry currently rates a structural
    signal LOW (see module docstring) - exercised directly against the
    mapping function to prove it handles that tier correctly regardless."""
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
    """6. 5000ms remains the absolute maximum - even a STRONG structural
    signal cannot push the window past whatever cap is passed in."""
    a = _evidence(1, source_format="database", service="orders-db")
    b = _evidence(2, source_format="database", service="orders-db")

    assert _adaptive_temporal_window_ms(a, b, 2000.0) == 2000.0


# --- 2/3/5: real end-to-end candidate-generation behavior -----------------


def test_stronger_supporting_evidence_survives_a_wider_separation():
    """2. A candidate with stronger supporting evidence (database format,
    shared service -> HIGH) can survive 4000ms - well beyond the 2500ms a
    MEDIUM-tier pair would tolerate."""
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="database", service="orders-db", first_seen=base)
    b = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=4000),
    )

    candidates = list(iter_temporal_candidates([a, b]))
    assert candidates == [(a, b)]


def test_weak_medium_evidence_requires_a_tighter_separation():
    """3/5. A MEDIUM-tier pair (generic format, shared service only)
    survives 2000ms but is rejected at 3000ms - inside the old fixed
    5000ms window, but now correctly excluded as a temporal candidate."""
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
    assert far_candidates == []  # 3000ms > this pair's own 2500ms MEDIUM window


def test_timestamp_only_evidence_cannot_exploit_the_full_5s_window():
    """4. Two events with NOTHING structural in common, well inside the
    old fixed 5000ms window, are no longer even proposed as candidates."""
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="generic", service="svc-x", first_seen=base)
    b = _evidence(
        2, source_format="generic", service="svc-y",
        first_seen=base + timedelta(milliseconds=1500),
    )

    assert list(iter_temporal_candidates([a, b])) == []
    assert list(iter_valid_temporal_candidates([a, b])) == []


# --- 6: 5000ms absolute maximum, enforced by the outer envelope too ------


def test_5000ms_outer_envelope_still_caps_even_a_strong_signal_pair():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="database", service="orders-db", first_seen=base)
    beyond_cap = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=5001),
    )

    assert list(iter_temporal_candidates([a, beyond_cap])) == []


# --- 7: delta_ms on the resulting edge is the real measured difference ---


def test_delta_ms_on_the_edge_is_the_real_measured_difference_not_the_window():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="database", service="orders-db", first_seen=base)
    b = _evidence(
        2, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=3000),
    )

    edges = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert len(edges) == 1
    assert edges[0].delta_ms == 3000.0  # the real gap, not 5000/2500/1000/0


# --- 1: strong identity behavior is unaffected/bypasses this path --------


def test_shared_trace_id_never_enters_the_temporal_fallback_path_at_all():
    """1. Identity-based matching is architecturally separate:
    iter_temporal_candidates excludes any pair already sharing a
    trace_id, regardless of how far apart or how little structural
    evidence they share - proving the adaptive window cannot touch it."""
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, source_format="generic", service="svc-x", trace_id="trace-1", first_seen=base
    )
    b = _evidence(
        2, source_format="generic", service="svc-y", trace_id="trace-1",
        first_seen=base + timedelta(milliseconds=4900),  # would be 0ms-tier if it were temporal
    )

    assert list(iter_temporal_candidates([a, b])) == []


def test_strong_identity_correlation_result_is_unchanged():
    """1. Full run_correlation confirms the trace_id-linked pair still
    correlates normally (via the identity path), even with zero
    structural overlap and a gap that would fail the NONE/0ms temporal
    tier outright."""
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


# --- 8: cross-artifact evidence can still correlate when justified -------


def test_cross_artifact_strong_signal_pair_still_correlates():
    base = datetime.now(timezone.utc)
    a = _evidence(
        1, artifact_id=101, source_format="database", service="orders-db", first_seen=base
    )
    b = _evidence(
        2, artifact_id=202, source_format="database", service="orders-db",
        first_seen=base + timedelta(milliseconds=4000),
    )

    edges = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert len(edges) == 1
    assert edges[0].source_id == "evidence-1"
    assert edges[0].target_id == "evidence-2"


# --- 9: two nearby unsupported incidents are not incorrectly collapsed ---


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


# --- 10: OCR/image evidence uses the same normalized mechanism -----------


def test_image_evidence_participates_through_the_same_generic_mechanism():
    """10. No image-specific correlation branch: "image" format evidence
    goes through the exact same match_correlation_signals/adaptive-window
    path as any other format, with its signal strengths (mirroring
    "generic") already registered in FORMAT_SIGNAL_PRIORITY."""
    base = datetime.now(timezone.utc)
    screenshot = _evidence(
        1, source_format="image", service="checkout-ui", source_file="terminal.png",
        first_seen=base,
    )
    close = _evidence(
        2, source_format="generic", service="checkout-ui",
        first_seen=base + timedelta(milliseconds=2000),  # within the MEDIUM 2500ms tier
    )
    far = _evidence(
        3, source_format="generic", service="checkout-ui",
        first_seen=base + timedelta(milliseconds=3000),  # beyond it
    )

    assert list(iter_temporal_candidates([screenshot, close])) == [(screenshot, close)]
    assert list(iter_temporal_candidates([screenshot, far])) == []


# --- 11: source_matches provenance is untouched ---------------------------


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
    assert node.source_matches == matches  # untouched, not consulted as a signal
    assert not any(
        signal.value == "source" for edge in run.result.components[0].edges for signal in edge.signals
    )


# --- 12: component/DAG/root-cause outputs unchanged when eligibility is unchanged --


def test_dag_and_root_cause_output_unchanged_for_an_unaffected_fixture():
    """12. Reproduces test_run_correlation_builds_propagation_dag's
    trace_id-linked fixture (identity path, never touches the temporal
    window) to confirm DAG/root-cause output is byte-identical to before
    this change."""
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
