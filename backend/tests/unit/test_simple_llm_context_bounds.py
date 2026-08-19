"""FIX 5: build_simple_llm_context() must not grow unbounded.

Bounded-memory ingestion (a 1 GiB input processed with bounded memory) is
worthless if every retained Evidence row is later dumped into a single
Gemini request for the SIMPLE (uncorrelated) investigation path. These
tests prove build_simple_llm_context()/_select_bounded_simple_evidence()
enforce explicit, deterministic bounds (SIMPLE_LLM_MAX_EVIDENCE_RECORDS,
SIMPLE_LLM_MAX_CONTEXT_BYTES in processing_config.py) - never an AI-based
ranking, never a second root-cause engine - while the final frontend/
database payload (build_simple_payload) stays unbounded/unaffected.
"""
import json
from datetime import datetime, timedelta, timezone

from app.core.processing_config import (
    SIMPLE_LLM_MAX_CONTEXT_BYTES,
    SIMPLE_LLM_MAX_EVIDENCE_RECORDS,
)
from app.models.evidence import Evidence
from app.services.investigation_context import (
    _select_bounded_simple_evidence,
    build_simple_llm_context,
    build_simple_payload,
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
        "first_line_number": 10,
        "last_line_number": 12,
        "representative_line": "ERROR something failed",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)


def test_large_synthetic_evidence_list_produces_bounded_llm_context():
    """The literal required regression: a large synthetic evidence list
    (well beyond SIMPLE_LLM_MAX_EVIDENCE_RECORDS) must not grow the Gemini
    context past the configured record bound."""
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, first_seen=base + timedelta(seconds=i))
        for i in range(SIMPLE_LLM_MAX_EVIDENCE_RECORDS * 3)
    ]

    context = build_simple_llm_context(analysis_id=1, evidence_rows=rows)

    assert len(context["evidence"]) <= SIMPLE_LLM_MAX_EVIDENCE_RECORDS
    assert context["evidence_count_total"] == len(rows)
    assert context["evidence_count_included"] == len(context["evidence"])
    assert context["evidence_count_included"] < context["evidence_count_total"]

    # Serialized size stays in the same ballpark as the configured byte
    # budget - not merely "smaller than before" but genuinely capped.
    serialized_bytes = len(json.dumps(context, default=str).encode("utf-8"))
    assert serialized_bytes < SIMPLE_LLM_MAX_CONTEXT_BYTES * 2


def test_full_frontend_payload_is_never_truncated_only_the_llm_context_is():
    """build_simple_payload (the real frontend/database result) must keep
    every retained evidence row regardless of how build_simple_llm_context
    bounds what reaches Gemini."""
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, first_seen=base + timedelta(seconds=i))
        for i in range(SIMPLE_LLM_MAX_EVIDENCE_RECORDS * 2)
    ]

    payload = build_simple_payload(analysis_id=1, evidence_rows=rows)

    assert len(payload["evidence"]) == len(rows)
    assert payload["evidence_count"] == len(rows)


def test_selection_prioritizes_source_matches_then_severity_then_occurrence():
    base = datetime.now(timezone.utc)
    weak = _evidence(1, severity="WARNING", occurrence_count=1, first_seen=base)
    strong_source_match = _evidence(
        2, severity="WARNING", occurrence_count=1, first_seen=base,
        source_matches=[{"relative_path": "app/worker.py", "confidence": "high"}],
    )
    high_severity = _evidence(3, severity="CRITICAL", occurrence_count=1, first_seen=base)
    high_occurrence = _evidence(4, severity="ERROR", occurrence_count=5000, first_seen=base)

    selected = _select_bounded_simple_evidence(
        [weak, strong_source_match, high_severity, high_occurrence],
        max_records=2,
        max_context_bytes=SIMPLE_LLM_MAX_CONTEXT_BYTES,
    )

    selected_ids = {e.id for e in selected}
    # The real source-code match ranks highest of all (a deterministic,
    # already-computed signal); CRITICAL severity is the next strongest
    # remaining signal - the plain WARNING and the high-occurrence-but-
    # lower-severity record are dropped first under a tight record cap.
    assert selected_ids == {2, 3}


def test_selection_respects_the_byte_budget_even_under_the_record_limit():
    """A handful of oversized records (e.g. huge source-match snippets)
    must be bounded by total bytes even when there are far fewer of them
    than max_records."""
    base = datetime.now(timezone.utc)
    huge_snippet = "x" * 10_000
    rows = [
        _evidence(
            i, first_seen=base + timedelta(seconds=i),
            source_matches=[{"relative_path": "a.py", "snippet": huge_snippet}],
        )
        for i in range(1, 6)
    ]

    selected = _select_bounded_simple_evidence(
        rows, max_records=100, max_context_bytes=25_000,
    )

    assert 0 < len(selected) < len(rows)


def test_a_single_oversized_record_is_still_included_not_dropped_to_empty():
    """The byte budget must never empty the context entirely just because
    the very first/highest-priority record alone exceeds it."""
    oversized = _evidence(
        1,
        source_matches=[{"relative_path": "a.py", "snippet": "x" * 50_000}],
    )

    selected = _select_bounded_simple_evidence(
        [oversized], max_records=100, max_context_bytes=100,
    )

    assert selected == [oversized]


def test_artifact_diversity_prevents_one_artifact_from_dominating():
    """One artifact with many low-priority rows must not crowd out a
    different artifact's evidence under a tight record cap - selection
    round-robins across artifacts."""
    base = datetime.now(timezone.utc)
    noisy_artifact = [
        _evidence(i, artifact_id=1, severity="WARNING", first_seen=base + timedelta(seconds=i))
        for i in range(1, 21)
    ]
    quiet_artifact = [
        _evidence(100, artifact_id=2, severity="ERROR", first_seen=base),
    ]

    selected = _select_bounded_simple_evidence(
        noisy_artifact + quiet_artifact,
        max_records=3,
        max_context_bytes=SIMPLE_LLM_MAX_CONTEXT_BYTES,
    )

    assert any(e.artifact_id == 2 for e in selected)


def test_selection_is_deterministic_regardless_of_input_order():
    import random

    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, severity=("CRITICAL" if i % 5 == 0 else "ERROR"), first_seen=base + timedelta(seconds=i))
        for i in range(1, 60)
    ]

    def selected_ids(order):
        return sorted(
            e.id
            for e in _select_bounded_simple_evidence(order, max_records=10, max_context_bytes=SIMPLE_LLM_MAX_CONTEXT_BYTES)
        )

    baseline = selected_ids(rows)
    shuffled = list(rows)
    random.Random(11).shuffle(shuffled)

    assert selected_ids(shuffled) == baseline
    assert selected_ids(list(reversed(rows))) == baseline


def test_small_evidence_list_is_returned_unchanged():
    """No truncation/reordering overhead when the whole list already fits
    comfortably within both bounds."""
    rows = [_evidence(1), _evidence(2)]

    context = build_simple_llm_context(analysis_id=1, evidence_rows=rows)

    assert context["evidence_count_total"] == 2
    assert context["evidence_count_included"] == 2
    assert len(context["evidence"]) == 2
