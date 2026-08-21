"""Item 5: parent-span semantics.

An exact parent.span_id == child.parent_span_id match (with compatible
trace identity) proves DIRECTION in trace topology - it does not by itself
prove that the parent's failure physically caused the child's failure.
relationship_type is now "explicit_parent_child" (never "causal", which
this pass stops generating - though legacy persisted result_snapshot JSON
may still contain it, and the frontend must keep rendering that safely).
"""
from datetime import datetime, timedelta, timezone

from app.models.evidence import Evidence
from app.services.correlation_engine import (
    build_correlation_edges,
    build_correlation_indexes,
    run_correlation,
)
from app.services.gemini_service import _SYSTEM_INSTRUCTION
from app.services.investigation_context import (
    _llm_component_priority_key,
    build_llm_context,
)


def _evidence(evidence_id, **kwargs):
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "occurrence_count": 1,
        "source_format": "opentelemetry",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)


def _parent_child_pair(same_timestamp: bool):
    base = datetime.now(timezone.utc)
    parent = _evidence(1, trace_id="trace-1", span_id="span-parent", first_seen=base, last_seen=base)
    child_seen = base if same_timestamp else base + timedelta(milliseconds=5)
    child = _evidence(
        2, trace_id="trace-1", parent_span_id="span-parent",
        first_seen=child_seen, last_seen=child_seen,
    )
    return parent, child


# --- 1-2: exact parent-span emits explicit_parent_child, confidence 1.0 ---


def test_exact_parent_span_emits_explicit_parent_child_with_full_direction_confidence():
    parent, child = _parent_child_pair(same_timestamp=False)

    edges, associations = build_correlation_edges([parent, child], build_correlation_indexes([parent, child]))

    assert associations == []
    assert len(edges) == 1
    assert edges[0].relationship_type == "explicit_parent_child"
    assert edges[0].direction_confidence == 1.0


# --- 3: equal timestamps do not remove explicit parent->child direction ---


def test_equal_timestamps_do_not_remove_explicit_parent_child_direction():
    parent, child = _parent_child_pair(same_timestamp=True)

    edges, associations = build_correlation_edges([parent, child], build_correlation_indexes([parent, child]))

    assert associations == []
    assert len(edges) == 1
    assert edges[0].relationship_type == "explicit_parent_child"
    assert edges[0].source_id == f"evidence-{parent.id}"
    assert edges[0].target_id == f"evidence-{child.id}"


# --- 4: inferred positive-time relationship remains inferred_propagation --


def test_inferred_positive_time_relationship_remains_inferred_propagation():
    base = datetime.now(timezone.utc)
    earlier = _evidence(1, source_format="generic", trace_id="trace-2", first_seen=base, last_seen=base)
    later = _evidence(
        2, source_format="generic", trace_id="trace-2",
        first_seen=base + timedelta(milliseconds=15), last_seen=base + timedelta(milliseconds=15),
    )

    edges, associations = build_correlation_edges([earlier, later], build_correlation_indexes([earlier, later]))

    assert associations == []
    assert len(edges) == 1
    assert edges[0].relationship_type == "inferred_propagation"


# --- 5: association relationship_type remains None -------------------------


def test_association_relationship_type_remains_none():
    base = datetime.now(timezone.utc)
    a = _evidence(1, source_format="generic", trace_id="trace-3", first_seen=base, last_seen=base)
    b = _evidence(2, source_format="generic", trace_id="trace-3", first_seen=base, last_seen=base)

    edges, associations = build_correlation_edges([a, b], build_correlation_indexes([a, b]))

    assert edges == []
    assert len(associations) == 1
    assert associations[0].relationship_type is None


# --- 6: Gemini instruction no longer claims proven physical causation -----


def test_gemini_instruction_no_longer_claims_parent_span_proves_causation():
    assert "explicit_parent_child" in _SYSTEM_INSTRUCTION
    assert (
        "a strongly evidenced, proven directional\n      relationship. You may describe this as one event causing"
        not in _SYSTEM_INSTRUCTION
    )
    lowered = _SYSTEM_INSTRUCTION.lower()
    parent_child_section = lowered.split('"explicit_parent_child"', 1)[1].split('"inferred_propagation"', 1)[0]
    assert "does not" in parent_child_section
    assert "physically caused" in parent_child_section or "causation" in parent_child_section
    assert "must not say one event" in parent_child_section or "must not" in parent_child_section


# --- 7: generated LLM context contains explicit_parent_child --------------


def test_llm_context_contains_explicit_parent_child():
    parent, child = _parent_child_pair(same_timestamp=False)
    rows = [parent, child]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    context = build_llm_context(run, rows)

    relationship_types = {
        edge["relationship_type"]
        for component in context["components"]
        for edge in component["propagation"]
    }
    assert "explicit_parent_child" in relationship_types


# --- 8: LLM component priority prefers explicit topology over inferred-only -


def test_llm_component_priority_prefers_explicit_parent_child_over_inferred_only():
    explicit_component = {
        "root_candidates": [],
        "propagation": [{"relationship_type": "explicit_parent_child"}],
        "associations": [],
        "root_evidence": [],
    }
    inferred_only_component = {
        "root_candidates": [],
        "propagation": [{"relationship_type": "inferred_propagation"}],
        "associations": [],
        "root_evidence": [],
    }

    assert (
        _llm_component_priority_key(explicit_component)
        > _llm_component_priority_key(inferred_only_component)
    )
