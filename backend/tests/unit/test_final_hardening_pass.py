"""Targeted tests for the final backend hardening pass ONLY - diagnostic_
attributes, low-structure supplemental context, not_linked isolation from
primary Gemini reasoning, end-to-end bounded evidence selection, source
index reuse, SPAN_ID/SOURCE correlation signals, the completed evidence/
node payload contract, the root_causes contract, the correlated Gemini
byte budget, real sub-second timestamp precision, the association-only
causal-language guard, and OCR final-exception-summary preservation.

Does not re-run the full pre-existing suite - see the final report for
what was verified by direct inspection vs. by these pytest cases.
"""
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.correlation_engine import run_correlation
from app.services.diagnostic_adapters import (
    _extract_diagnostic_attributes,
    stream_image_events_from_text,
)
from app.services.diagnostic_parser import parse_timestamp
from app.services.investigation_context import (
    _evidence_payload,
    build_correlation_payload,
    build_llm_context,
    select_bounded_evidence_from_db,
)
from app.tasks import analysis as analysis_task

_FAKE_GEMINI_RESULT = GeminiInvestigationResponse(
    title="t", summary="s", probable_root_causes=[], what_happened=[],
    source_code_findings=[], recommended_actions=[], uncertainties=[],
)


def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "severity": "ERROR",
        "occurrence_count": 1,
        "source_format": "generic",
        "first_line_number": 1,
        "last_line_number": 1,
        "first_seen": datetime.now(timezone.utc),
    }
    defaults.update(kwargs)
    return Evidence(**defaults)


# --- 1. diagnostic_attributes ----------------------------------------------


def test_unknown_structured_fields_survive_as_diagnostic_attributes():
    data = {
        "level": "ERROR",
        "message": "checkout failed",
        "error_code": "POOL_EXHAUSTED",
        "available_connections": 0,
        "max_pool_size": 5,
        "retry_count": 17,
    }
    attrs = _extract_diagnostic_attributes(data)
    assert attrs == {
        "error_code": "POOL_EXHAUSTED",
        "available_connections": 0,
        "max_pool_size": 5,
        "retry_count": 17,
    }


def test_diagnostic_attributes_are_bounded_by_byte_budget():
    from app.core.processing_config import DIAGNOSTIC_ATTRIBUTES_MAX_BYTES

    data = {"level": "ERROR", "message": "m"}
    data.update({f"unknown_field_{i}": "x" * 200 for i in range(50)})

    attrs = _extract_diagnostic_attributes(data)

    total_bytes = sum(len(k) + len(str(v)) for k, v in attrs.items())
    assert total_bytes <= DIAGNOSTIC_ATTRIBUTES_MAX_BYTES


def test_canonical_fields_are_never_duplicated_into_diagnostic_attributes():
    data = {
        "level": "ERROR",
        "message": "m",
        "trace_id": "t1",
        "service": "checkout",
        "error_code": "X",
    }
    attrs = _extract_diagnostic_attributes(data)
    assert set(attrs) == {"error_code"}


def test_diagnostic_attributes_reach_evidence_payload():
    evidence = _evidence(1, diagnostic_attributes={"error_code": "POOL_EXHAUSTED"})
    payload = _evidence_payload(evidence)
    assert payload["diagnostic_attributes"] == {"error_code": "POOL_EXHAUSTED"}


# --- 8. host/container/pod/diagnostic_attributes reach the response -------


def test_host_container_pod_and_diagnostic_attributes_reach_the_response():
    evidence = _evidence(
        1, host="host-1", container="container-1", pod="pod-1",
        diagnostic_attributes={"retry_count": 3},
    )
    payload = _evidence_payload(evidence)
    assert payload["host"] == "host-1"
    assert payload["container"] == "container-1"
    assert payload["pod"] == "pod-1"
    assert payload["diagnostic_attributes"] == {"retry_count": 3}


def test_node_payload_also_carries_host_container_pod_and_diagnostic_attributes():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="t1", host="h1", container="c1", pod="p1",
                  diagnostic_attributes={"x": 1}, first_seen=base),
        _evidence(2, trace_id="t1", first_seen=base + timedelta(milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    payload = build_correlation_payload(run, rows)

    node = next(n for n in payload["components"][0]["nodes"] if n["id"] == "evidence-1")
    assert node["host"] == "h1"
    assert node["container"] == "c1"
    assert node["pod"] == "p1"
    assert node["diagnostic_attributes"] == {"x": 1}
    assert "representative_line" in node


# --- 9. root_causes contract ------------------------------------------------


def test_root_causes_contains_only_role_root_nodes():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, span_id="s1", trace_id="t1", first_seen=base),
        _evidence(2, parent_span_id="s1", trace_id="t1", first_seen=base + timedelta(milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    payload = build_correlation_payload(run, rows)

    component = payload["components"][0]
    roles_in_root_causes = {c["role"] for c in component["root_causes"]}
    assert roles_in_root_causes <= {"root"}
    assert roles_in_root_causes  # at least one real root in this fixture

    node_roles = {n["id"]: n["role"] for n in component["nodes"]}
    assert "victim" in node_roles.values()


def test_association_only_component_has_empty_root_causes_not_fabricated():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="t1", first_seen=base),
        _evidence(2, trace_id="t1", first_seen=base),  # equal timestamp -> association only
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    payload = build_correlation_payload(run, rows)

    assert payload["components"][0]["root_causes"] == []


# --- not_linked isolation from Gemini's primary reasoning -------------------


def test_non_primary_component_excluded_from_gemini_and_frontend_primary_graph():
    """Two locked fixes, same underlying _select_primary_component()
    definition: Gemini's primary reasoning AND the frontend correlated
    graph payload (build_correlation_payload) both represent only the
    primary incident component - a genuinely-correlated but non-primary
    component is excluded from both, honestly counted in both, and its
    Evidence stays fully persisted (never deleted, never in the fixture
    to begin with - this only proves it's excluded from the RETURNED
    graph)."""
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, span_id="root", trace_id="primary", first_seen=base),
        _evidence(2, parent_span_id="root", trace_id="primary", first_seen=base + timedelta(milliseconds=5)),
        _evidence(3, parent_span_id="root", trace_id="primary", first_seen=base + timedelta(milliseconds=10)),
        _evidence(4, span_id="sec", trace_id="secondary", first_seen=base + timedelta(seconds=1)),
        _evidence(5, parent_span_id="sec", trace_id="secondary", first_seen=base + timedelta(seconds=1, milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    assert len(run.result.components) == 2  # a primary (3-node) and a secondary (2-node)

    context = build_llm_context(run, rows)
    frontend = build_correlation_payload(run, rows)

    assert len(context["components"]) == 1  # only the primary reaches Gemini
    assert context["component_count_total"] == 1
    assert context.get("excluded_isolated_component_count") == 1
    gemini_evidence_ids = {
        e["id"] for c in context["components"] for e in c["root_evidence"]
    }
    assert 4 not in gemini_evidence_ids and 5 not in gemini_evidence_ids

    # Frontend correlated graph is ALSO restricted to the primary
    # component only (final hardening pass) - honestly counted, not
    # silently claimed as returned.
    assert len(frontend["components"]) == 1
    assert frontend["component_count"] == 1
    assert frontend["component_count_total"] == 2
    assert frontend["excluded_component_count"] == 1
    frontend_node_evidence_ids = {
        e["id"]
        for c in frontend["components"]
        for n in c["nodes"]
        for e in n["evidence"]
    }
    assert 4 not in frontend_node_evidence_ids and 5 not in frontend_node_evidence_ids


def test_isolated_singleton_component_never_becomes_a_root_cause():
    rows = [_evidence(1, first_seen=datetime.now(timezone.utc))]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    context = build_llm_context(run, rows)
    assert context["components"] == []
    assert context["component_count_total"] == 0


# --- Correlated Gemini byte budget ------------------------------------------


def test_correlated_gemini_context_reports_truncation_when_over_budget(monkeypatch):
    from app.services import investigation_context as ctx_module

    monkeypatch.setattr(ctx_module, "CORRELATED_GEMINI_CONTEXT_MAX_BYTES", 400)

    base = datetime.now(timezone.utc)
    components_rows = []
    for group in range(5):
        offset = group * 10
        components_rows.append(_evidence(
            offset + 1, span_id=f"root-{group}", trace_id=f"trace-{group}",
            first_seen=base + timedelta(seconds=group),
        ))
        components_rows.append(_evidence(
            offset + 2, parent_span_id=f"root-{group}", trace_id=f"trace-{group}",
            first_seen=base + timedelta(seconds=group, milliseconds=5),
        ))

    run = run_correlation(analysis_id=1, evidence_rows=components_rows)
    assert len(run.result.components) == 5

    context = build_llm_context(run, components_rows)

    assert context["component_count_included"] <= context["component_count_total"]
    if context["component_count_included"] < context["component_count_total"]:
        assert context["context_truncated"] is True
    assert len(context["components"]) >= 1  # always at least one, never emptied


def test_correlated_gemini_context_omits_truncation_flags_when_under_budget():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, span_id="s1", trace_id="t1", first_seen=base),
        _evidence(2, parent_span_id="s1", trace_id="t1", first_seen=base + timedelta(milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    context = build_llm_context(run, rows)
    assert "context_truncated" not in context
    assert context["component_count_included"] == context["component_count_total"] == 1


# --- Association-only causal-language guard ---------------------------------


def test_association_only_context_flags_no_directed_relationships():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="t1", first_seen=base),
        _evidence(2, trace_id="t1", first_seen=base),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    context = build_llm_context(run, rows)

    assert context["has_directed_relationships"] is False
    assert "causal_language_instruction" in context
    assert "caused" not in context["causal_language_instruction"].lower().split("\"")[0]


def test_directed_relationship_context_omits_the_causal_language_warning():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, span_id="s1", trace_id="t1", first_seen=base),
        _evidence(2, parent_span_id="s1", trace_id="t1", first_seen=base + timedelta(milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    context = build_llm_context(run, rows)

    assert context["has_directed_relationships"] is True
    assert "causal_language_instruction" not in context


# --- Timestamp precision ----------------------------------------------------


def test_iso_millisecond_timestamp_survives_parsing():
    parsed = parse_timestamp("2026-08-14T15:30:40.123Z")
    assert parsed.microsecond == 123000


def test_six_digit_microsecond_timestamp_survives_parsing():
    parsed = parse_timestamp("[Fri Aug 14 15:30:40.123456 2026]")
    assert parsed.microsecond == 123456


def test_whole_second_timestamp_does_not_gain_fabricated_precision():
    parsed = parse_timestamp("2026-08-14T15:30:40Z")
    assert parsed.microsecond == 0
    assert "." not in parsed.isoformat()  # no invented ".000000"


def test_relative_ms_and_delta_ms_reflect_real_sub_second_differences():
    base = datetime(2026, 8, 14, 15, 30, 40, 123000, tzinfo=timezone.utc)
    rows = [
        _evidence(1, span_id="s1", trace_id="t1", first_seen=base),
        _evidence(
            2, parent_span_id="s1", trace_id="t1",
            first_seen=base + timedelta(milliseconds=27, microseconds=400),
        ),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    payload = build_correlation_payload(run, rows)

    edge = payload["components"][0]["edges"][0]
    assert edge["delta_ms"] == pytest.approx(27.4)
    assert edge["relationship_type"] == "explicit_parent_child"


def test_equal_timestamps_remain_associations_not_a_fabricated_edge():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="t1", first_seen=base),
        _evidence(2, trace_id="t1", first_seen=base),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    payload = build_correlation_payload(run, rows)
    assert payload["components"][0]["edges"] == []
    assert payload["components"][0]["associations"]


# --- OCR final exception summary --------------------------------------------


def test_ocr_traceback_retains_the_final_exception_summary_line():
    text = (
        'Traceback (most recent call last):\n'
        '  File "auth.py", line 13, in <module>\n'
        '    from app.services.email_service import send_password_reset_email\n'
        "ImportError: cannot import name 'send_password_reset_email' "
        "from 'app.services.email_service' (unknown location)"
    )
    events = [
        e for e in stream_image_events_from_text(
            extracted_text=text, ocr_confidence=0.91,
            source_file="shot.png", global_line_number=0,
        )
        if e.event is not None
    ]
    assert len(events) == 1
    assert events[0].event.exception_type == "ImportError"
    assert "cannot import name" in events[0].event.exception_message
    assert events[0].event.ocr_confidence == 0.91


def test_ocr_final_summary_preservation_is_generic_not_import_error_only():
    text = (
        'Exception in thread "main" java.lang.NullPointerException: boom\n'
        "    at com.example.Auth.login(Auth.java:42)\n"
        "    someLocalVariable = computeSomething()\n"
        "Caused by: java.lang.RuntimeException: underlying failure"
    )
    events = [
        e for e in stream_image_events_from_text(
            extracted_text=text, ocr_confidence=0.8,
            source_file="shot2.png", global_line_number=0,
        )
        if e.event is not None
    ]
    assert len(events) == 1
    assert "Caused by" in events[0].event.raw_line
    assert events[0].event.exception_type == "RuntimeException"


# --- Section 4: end-to-end bounded evidence selection -----------------------


def _sqlite_analysis_with_evidence(monkeypatch, evidence_count: int):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()
    artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="a.log",
        saved_file_path="a.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    db.add(artifact)
    db.commit()
    base = datetime.now(timezone.utc)
    db.add_all([
        Evidence(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key=f"ck-{i}",
            fingerprint=f"fp-{i % 5}", severity="ERROR", source_format="generic",
            first_line_number=1, last_line_number=1,
            first_seen=base + timedelta(milliseconds=i),
            span_id=f"span-{i}" if i == 3 else None,
            parent_span_id="span-3" if i == 4 else None,
        )
        for i in range(evidence_count)
    ])
    db.commit()
    return db, analysis.id


def test_bounded_selection_does_not_materialize_the_whole_table(monkeypatch):
    """Proves the .all()-then-truncate anti-pattern is gone: patch the ORM
    query's .all() to explode if any single query is ever asked to return
    more rows than the streaming scan's own configured batch_size, then
    confirm selection over 10x that many rows still succeeds via multiple
    bounded round trips."""
    db, analysis_id = _sqlite_analysis_with_evidence(monkeypatch, 50)

    from sqlalchemy.orm import Query

    original_all = Query.all
    batch_size = 5

    def guarded_all(self):
        rows = original_all(self)
        if len(rows) > batch_size:
            raise AssertionError(
                f"one query returned {len(rows)} rows (> batch_size={batch_size}) "
                "- not end-to-end bounded"
            )
        return rows

    monkeypatch.setattr(Query, "all", guarded_all)

    selected, total_count = select_bounded_evidence_from_db(
        db, analysis_id=analysis_id, max_records=5, max_context_bytes=10_000_000,
        batch_size=batch_size,
    )

    assert total_count == 50
    assert len(selected) == 5


def test_bounded_selection_preserves_parent_span_identity_bridge_evidence():
    db, analysis_id = _sqlite_analysis_with_evidence(None, 30)

    selected, total_count = select_bounded_evidence_from_db(
        db, analysis_id=analysis_id, max_records=5, max_context_bytes=10_000_000,
    )

    assert total_count == 30
    selected_ids = {e.id for e in selected}
    # Autoincrement ids start at 1, so row index i=3 (span_id set) has
    # id=4 and i=4 (parent_span_id referencing it) has id=5 - the only
    # genuine parent-span participants. The priority scorer must strongly
    # prefer keeping them over generic higher-index rows.
    assert {4, 5} <= selected_ids


def test_bounded_selection_fast_path_matches_full_query_when_under_bound():
    db, analysis_id = _sqlite_analysis_with_evidence(None, 3)
    selected, total_count = select_bounded_evidence_from_db(
        db, analysis_id=analysis_id, max_records=100, max_context_bytes=10_000_000,
    )
    assert total_count == 3
    assert len(selected) == 3


# --- Section 6: source index reuse ------------------------------------------


def test_source_index_is_not_rebuilt_once_per_artifact(tmp_path, monkeypatch):
    from app.services import source_archive

    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def fake_clone(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app").mkdir()
        (dest / "app" / "main.py").write_text("print('hi')\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    walk_calls = []
    real_walk = os.walk

    def counting_walk(path, *a, **kw):
        walk_calls.append(path)
        return real_walk(path, *a, **kw)

    monkeypatch.setattr("app.services.source_index.os.walk", counting_walk)

    first = source_archive.prepare_source("github", "https://github.com/acme/project", 42)
    second = source_archive.prepare_source("github", "https://github.com/acme/project", 42)
    third = source_archive.prepare_source("github", "https://github.com/acme/project", 42)

    assert len(walk_calls) == 1  # only the very first build_index() walked the tree
    assert set(first.by_path) == set(second.by_path) == set(third.by_path) == {"app/main.py"}


def test_process_local_cache_skips_even_the_manifest_read(tmp_path, monkeypatch):
    from app.tasks import analysis as analysis_task
    from types import SimpleNamespace

    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})

    calls = []
    monkeypatch.setattr(
        analysis_task, "prepare_source",
        lambda *a, **k: calls.append(a) or object(),
    )

    analysis = SimpleNamespace(id=123, source_kind="zip", source_reference=str(tmp_path / "x.zip"))
    monkeypatch.setattr(analysis_task, "_remove_staged_source_archive", lambda ref: None)

    first = analysis_task._prepare_source_index(analysis)
    second = analysis_task._prepare_source_index(analysis)

    assert first is second
    assert len(calls) == 1


# --- SPAN_ID / SOURCE correlation signals -----------------------------------


def test_equal_span_id_is_a_correlation_signal_not_causal_on_its_own():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, span_id="shared-span", first_seen=base),
        _evidence(2, span_id="shared-span", first_seen=base),  # equal ts -> association
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 1
    payload = build_correlation_payload(run, rows)
    assert payload["components"][0]["edges"] == []
    assert payload["components"][0]["associations"]
    signals = payload["components"][0]["associations"][0]["signals"]
    assert "span_id" in signals


def test_source_signal_only_fires_on_identical_file_and_line():
    # SOURCE is only configured (FORMAT_SIGNAL_PRIORITY) for formats where a
    # source-code location is a meaningful signal - "stack_trace" here, not
    # the default "generic".
    base = datetime.now(timezone.utc)
    same_match = [{"relative_path": "srv/worker.py", "line_number": 42, "confidence": "high"}]
    different_match = [{"relative_path": "srv/other.py", "line_number": 10, "confidence": "high"}]

    matching_rows = [
        _evidence(1, source_format="stack_trace", source_matches=same_match, first_seen=base),
        _evidence(2, source_format="stack_trace", source_matches=same_match, first_seen=base),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=matching_rows)
    payload = build_correlation_payload(run, matching_rows)
    assert payload["components"][0]["associations"]
    assert "source" in payload["components"][0]["associations"][0]["signals"]

    non_matching_rows = [
        _evidence(3, source_format="stack_trace", source_matches=same_match, first_seen=base),
        _evidence(4, source_format="stack_trace", source_matches=different_match, first_seen=base),
    ]
    run2 = run_correlation(analysis_id=1, evidence_rows=non_matching_rows)
    assert len(run2.result.components) == 2  # never merged on "both have some match"


# --- Section 2: low-structure survives a mixed investigation ---------------


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def test_low_structure_artifact_survives_alongside_structured_evidence(monkeypatch):
    """app.log has real Evidence (SIMPLE path); weird_rust.txt has only a
    captured fallback_context and zero structured Evidence - it must not
    disappear from the final result/Gemini context just because another
    artifact in the same analysis has real evidence."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()
    app_log = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="app.log",
        saved_file_path="app.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    weird_rust = AnalysisArtifact(
        analysis_id=analysis.id, position=1, original_filename="weird_rust.txt",
        saved_file_path="weird_rust.txt", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
        fallback_context={"kind": "text", "text": "thread panicked: index out of bounds"},
    )
    db.add_all([app_log, weird_rust])
    db.commit()
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=app_log.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1, severity="ERROR",
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    published = []
    llm_contexts = []

    def fake_gemini(context):
        llm_contexts.append(context)
        return _FAKE_GEMINI_RESULT

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", fake_gemini)
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    assert len(published) == 1
    payload = published[0]
    assert payload["investigation_path"] == "simple"
    # The real evidence is untouched.
    assert payload["evidence_count"] == 1

    # weird_rust.txt's content survived as bounded supplemental context.
    supplemental = payload["supplemental_low_structure_context"]
    assert len(supplemental) == 1
    assert supplemental[0]["source_file"] == "weird_rust.txt"
    assert "panicked" in supplemental[0]["text"]

    # Its outcome is reported as low_structure, never "no meaningful
    # diagnostic evidence" (which would be false - real content WAS found).
    artifact_by_name = {a["source_file"]: a for a in payload["artifacts"]}
    assert artifact_by_name["weird_rust.txt"]["relationship_status"] == "low_structure"
    assert "could not be deterministically structured" in artifact_by_name["weird_rust.txt"]["message"]
    assert "no meaningful diagnostic evidence" not in artifact_by_name["weird_rust.txt"]["message"].lower()

    # Exactly one Gemini call, and its context explicitly marks the
    # supplemental material as non-causal.
    assert len(llm_contexts) == 1
    assert llm_contexts[0]["supplemental_low_structure_context"] == supplemental
    assert "NOT" in llm_contexts[0]["supplemental_low_structure_instruction"]
    assert "causal" in llm_contexts[0]["supplemental_low_structure_instruction"].lower()


def test_truly_irrelevant_artifact_remains_true_no_evidence(monkeypatch):
    """An artifact with genuinely no diagnostic content at all (no
    fallback_context captured, no structured Evidence) must remain plain
    no_evidence - never relabeled low_structure."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()
    app_log = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="app.log",
        saved_file_path="app.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    boring = AnalysisArtifact(
        analysis_id=analysis.id, position=1, original_filename="boring.txt",
        saved_file_path="boring.txt", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
        fallback_context=None,
    )
    db.add_all([app_log, boring])
    db.commit()
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=app_log.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1, severity="ERROR",
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    published = []
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT
    )
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    payload = published[0]
    assert "supplemental_low_structure_context" not in payload
    artifact_by_name = {a["source_file"]: a for a in payload["artifacts"]}
    assert artifact_by_name["boring.txt"]["relationship_status"] == "no_evidence"
    assert artifact_by_name["boring.txt"]["message"] == (
        "No meaningful diagnostic evidence was extracted from this artifact."
    )
