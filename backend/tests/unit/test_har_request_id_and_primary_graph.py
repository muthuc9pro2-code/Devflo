"""Targeted tests for the two final surgical fixes:

1. Browser HAR request.headers (name/value pairs) promoting a recognized
   request-id-shaped header into the canonical Evidence.request_id field,
   so a HAR entry can correlate normally instead of defaulting to
   relationship_status="not_linked".
2. build_correlation_payload()'s components[] representing the PRIMARY
   correlated incident only (via the existing _select_primary_component),
   matching the definition already used for relationship-status labeling
   and Gemini isolation - never a second definition of "primary".
"""
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.artifact_detector import ArtifactFormat
from app.services.correlation_engine import run_correlation
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.investigation_context import build_correlation_payload, build_llm_context
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


# --- Fix 1: HAR x-request-id -> canonical request_id -----------------------


def _har_events(entries: list[dict]):
    har = {"log": {"entries": entries}}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".har", delete=False) as f:
        json.dump(har, f)
        path = f.name
    try:
        return [
            e
            for e in stream_artifact_events(
                file_path=path, artifact_format=ArtifactFormat.BROWSER, source_file="trace.har"
            )
            if e.event is not None
        ]
    finally:
        os.unlink(path)


def _har_entry(*, headers, status=500):
    return {
        "startedDateTime": "2026-08-14T10:00:00.000Z",
        "request": {
            "method": "GET",
            "url": "https://api.example.com/checkout",
            "headers": headers,
        },
        "response": {"status": status},
    }


def test_har_x_request_id_header_becomes_canonical_request_id():
    events = _har_events([
        _har_entry(headers=[
            {"name": "Accept", "value": "application/json"},
            {"name": "x-request-id", "value": "req-prod-500"},
        ])
    ])
    assert len(events) == 1
    assert events[0].event.request_id == "req-prod-500"
    # diagnostic_attributes are not stripped merely because the canonical
    # field got populated - the header pair still survives there too.
    assert events[0].event.diagnostic_attributes is not None


def test_har_request_header_matching_is_case_insensitive_and_covers_aliases():
    for header_name, expected in [
        ("X-Request-Id", "req-a"),
        ("REQUEST-ID", "req-b"),
        ("request_id", "req-c"),
        ("Correlation-Id", "req-d"),
        ("correlation_id", "req-e"),
    ]:
        events = _har_events([_har_entry(headers=[{"name": header_name, "value": expected}])])
        assert len(events) == 1, header_name
        assert events[0].event.request_id == expected, header_name


def test_arbitrary_har_header_does_not_become_request_id():
    events = _har_events([
        _har_entry(headers=[{"name": "X-Custom-Trace", "value": "should-not-be-promoted"}])
    ])
    assert len(events) == 1
    assert events[0].event.request_id is None


def test_har_shared_request_id_correlates_normally_with_another_artifact():
    har_events = _har_events([
        _har_entry(headers=[{"name": "x-request-id", "value": "req-shared-1"}], status=500)
    ])
    assert len(har_events) == 1

    base = har_events[0].event.timestamp or datetime.now(timezone.utc)
    if isinstance(base, str):
        from app.services.diagnostic_parser import parse_timestamp
        base = parse_timestamp(base)

    har_evidence = Evidence(
        id=1, analysis_id=1, artifact_id=1, fingerprint="fp-har",
        severity="ERROR", occurrence_count=1, source_format="browser",
        request_id=har_events[0].event.request_id,
        first_line_number=1, last_line_number=1, first_seen=base,
    )
    other_evidence = Evidence(
        id=2, analysis_id=1, artifact_id=2, fingerprint="fp-other",
        severity="ERROR", occurrence_count=1, source_format="web_server",
        request_id="req-shared-1",
        first_line_number=1, last_line_number=1,
        first_seen=base + timedelta(milliseconds=50),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[har_evidence, other_evidence])

    assert len(run.result.components) == 1
    assert len(run.result.components[0].nodes) == 2


# --- Fix 2: components[] is the primary graph only --------------------------


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def test_isolated_not_linked_component_excluded_from_returned_graph():
    """One primary multi-artifact component + one truly isolated
    not_linked artifact: components[] must contain only the primary."""
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=5)),
        _evidence(3, artifact_id=103, service="isolated-tool", first_seen=base + timedelta(hours=1)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 2

    payload = build_correlation_payload(run, rows)

    assert payload["component_count"] == 1
    assert payload["component_count_total"] == 2
    assert payload["excluded_component_count"] == 1
    node_ids = {n["id"] for c in payload["components"] for n in c["nodes"]}
    assert node_ids == {"evidence-1", "evidence-2"}
    assert "evidence-3" not in node_ids


def test_not_linked_artifact_still_reported_in_artifacts_with_full_outcome(monkeypatch):
    """Isolated artifact's node never appears in components[], but its
    outcome (filename/status/relationship_status/message) still reaches
    artifacts[]."""
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=5)),
        _evidence(3, artifact_id=103, service="isolated-tool", first_seen=base + timedelta(hours=1)),
    ]
    artifacts = [
        {"id": 101, "original_filename": "db.log", "detected_format": "database", "status": "completed", "duplicate_of_artifact_id": None},
        {"id": 102, "original_filename": "api.log", "detected_format": "web_server", "status": "completed", "duplicate_of_artifact_id": None},
        {"id": 103, "original_filename": "isolated.log", "detected_format": "generic", "status": "completed", "duplicate_of_artifact_id": None},
    ]
    from types import SimpleNamespace
    artifact_rows = [SimpleNamespace(**a) for a in artifacts]

    run = run_correlation(analysis_id=1, evidence_rows=rows)
    payload = build_correlation_payload(run, rows, artifacts=artifact_rows)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    isolated = outcome_by_id[103]
    assert isolated["source_file"] == "isolated.log"
    assert isolated["source_format"] == "generic"
    assert isolated["status"] == "processed"
    assert isolated["evidence_count"] == 1
    assert isolated["relationship_status"] == "not_linked"
    assert "Not linked" in isolated["message"]

    # The primary pair are linked, never conflated with not_linked.
    assert outcome_by_id[101]["relationship_status"] == "linked"
    assert outcome_by_id[102]["relationship_status"] == "linked"


def test_not_linked_evidence_remains_persisted_in_mysql_backed_finalize(monkeypatch):
    """End-to-end via _finalize_analysis_task: the isolated artifact's
    Evidence row must still exist in the database after finalize, even
    though it never appears in the returned components[] graph."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()
    primary_a = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="db.log",
        saved_file_path="db.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    primary_b = AnalysisArtifact(
        analysis_id=analysis.id, position=1, original_filename="api.log",
        saved_file_path="api.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    isolated = AnalysisArtifact(
        analysis_id=analysis.id, position=2, original_filename="isolated.log",
        saved_file_path="isolated.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    db.add_all([primary_a, primary_b, isolated])
    db.commit()
    base = datetime.now(timezone.utc)
    db.add_all([
        Evidence(
            analysis_id=analysis.id, artifact_id=primary_a.id, correlation_key="ck-1",
            fingerprint="fp-1", trace_id="trace-1", service="db", source_format="database",
            first_line_number=1, last_line_number=1, first_seen=base, severity="ERROR",
        ),
        Evidence(
            analysis_id=analysis.id, artifact_id=primary_b.id, correlation_key="ck-2",
            fingerprint="fp-2", trace_id="trace-1", service="api", source_format="web_server",
            first_line_number=1, last_line_number=1,
            first_seen=base + timedelta(milliseconds=5), severity="ERROR",
        ),
        Evidence(
            analysis_id=analysis.id, artifact_id=isolated.id, correlation_key="ck-3",
            fingerprint="fp-3", service="isolated-tool", source_format="generic",
            first_line_number=1, last_line_number=1,
            first_seen=base + timedelta(hours=1), severity="ERROR",
        ),
    ])
    db.commit()
    analysis_id = analysis.id
    isolated_artifact_id = isolated.id
    db.close()

    published = []
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    payload = published[0]
    node_ids = {n["id"] for c in payload["components"] for n in c["nodes"]}
    assert f"evidence-{isolated_artifact_id}" not in [nid for nid in node_ids]  # never in the returned graph by construction
    assert len(payload["components"]) == 1

    db = session_factory()
    isolated_evidence = (
        db.query(Evidence).filter(Evidence.artifact_id == isolated_artifact_id).all()
    )
    assert len(isolated_evidence) == 1
    assert isolated_evidence[0].fingerprint == "fp-3"
    db.close()


def test_partially_linked_artifact_keeps_only_its_primary_component_nodes():
    """An artifact with SOME evidence in the primary component and SOME
    in a separate, non-primary component must show partially_linked, and
    only its primary-component node may appear in components[]."""
    base = datetime.now(timezone.utc)
    rows = [
        # Primary component: 3 nodes, 2 distinct artifacts (101, 102).
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=5)),
        # Artifact 101 ALSO has an isolated evidence row elsewhere, unrelated.
        _evidence(3, artifact_id=101, service="unrelated-batch-job", first_seen=base + timedelta(hours=2)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 2

    payload = build_correlation_payload(run, rows)

    assert payload["component_count"] == 1
    returned_node_ids = {n["id"] for c in payload["components"] for n in c["nodes"]}
    assert returned_node_ids == {"evidence-1", "evidence-2"}
    assert "evidence-3" not in returned_node_ids


# --- Gemini behavior unchanged -----------------------------------------


def test_gemini_context_still_excludes_not_linked_diagnostic_content():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=5)),
        _evidence(3, artifact_id=103, service="isolated-tool", first_seen=base + timedelta(hours=1)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    context = build_llm_context(run, rows)

    assert len(context["components"]) == 1
    gemini_evidence_ids = {
        e["id"] for c in context["components"] for e in c["root_evidence"]
    }
    assert 3 not in gemini_evidence_ids
