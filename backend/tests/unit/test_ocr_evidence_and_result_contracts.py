from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services import diagnostic_adapters
from app.services.artifact_detector import ArtifactFormat, detect_artifact_sample
from app.services.correlation_engine import run_correlation
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.investigation_context import (
    build_correlation_payload,
    build_llm_context,
    build_simple_llm_context,
    build_simple_payload,
    build_zero_evidence_payload,
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
        "event_type": None,
        "severity": "ERROR",
        "occurrence_count": 1,
        "first_seen": datetime.now(timezone.utc),
        "last_seen": datetime.now(timezone.utc),
        "source_format": "generic",
        "first_line_number": 1,
        "last_line_number": 1,
        "representative_line": "ERROR something failed",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)

def _artifact_row(artifact_id: int, filename: str, fmt: str, status: str = "completed"):
    return SimpleNamespace(id=artifact_id, original_filename=filename, detected_format=fmt, status=status)

def test_detect_artifact_sample_recognizes_images_by_suffix_and_mime():
    fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64

    assert (
        detect_artifact_sample(fake_png, filename="terminal.png", mime_type="image/png")
        == ArtifactFormat.IMAGE
    )
    assert (
        detect_artifact_sample(fake_png, filename="terminal.png", mime_type=None)
        == ArtifactFormat.IMAGE
    )
    assert (
        detect_artifact_sample(fake_png, filename=None, mime_type="image/webp")
        == ArtifactFormat.IMAGE
    )
    assert (
        detect_artifact_sample(b"ERROR boom", filename="app.log", mime_type="text/plain")
        == ArtifactFormat.GENERIC
    )

def test_ocr_text_flows_through_the_existing_text_normalizer_with_provenance(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        diagnostic_adapters,
        "extract_text_from_image_with_confidence",
        lambda path: ("raw ocr", 0.87),
    )
    monkeypatch.setattr(
        diagnostic_adapters,
        "normalize_ocr_text",
        lambda text: "ERROR TypeError: Cannot read property 'foo' of undefined",
    )
    image_path = tmp_path / "terminal.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    records = list(
        stream_artifact_events(
            file_path=str(image_path),
            artifact_format=ArtifactFormat.IMAGE,
            source_file="terminal.png",
        )
    )

    assert len(records) == 1
    event = records[0].event
    assert event is not None
    assert event.source_format == "image"
    assert event.source_file == "terminal.png"
    assert event.level == "ERROR"
    assert event.ocr_confidence == 0.87

def test_image_with_no_readable_text_yields_no_fabricated_evidence(monkeypatch, tmp_path):
    monkeypatch.setattr(
        diagnostic_adapters,
        "extract_text_from_image_with_confidence",
        lambda path: ("", None),
    )
    monkeypatch.setattr(diagnostic_adapters, "normalize_ocr_text", lambda text: "")
    image_path = tmp_path / "blank.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    records = list(
        stream_artifact_events(
            file_path=str(image_path),
            artifact_format=ArtifactFormat.IMAGE,
            source_file="blank.png",
        )
    )

    assert records == []

def test_ocr_evidence_can_take_simple_path():
    evidence_rows = [
        _evidence(1, artifact_id=1, source_format="image", source_file="terminal.png", service="checkout-ui")
    ]
    artifacts = [_artifact_row(1, "terminal.png", "image")]

    payload = build_simple_payload(1, evidence_rows, artifacts=artifacts)

    assert payload["investigation_path"] == "simple"
    assert payload["evidence"][0]["source_format"] == "image"
    assert payload["evidence"][0]["source_file"] == "terminal.png"
    outcome = payload["artifacts"][0]
    assert outcome["source_format"] == "image"
    assert outcome["evidence_count"] == 1
    assert "message" not in outcome

def test_ocr_evidence_correlates_with_a_real_shared_trace_id():
    base = datetime.now(timezone.utc)
    web = _evidence(1, artifact_id=101, source_format="web_server", trace_id="trace-1", service="checkout-api", first_seen=base)
    screenshot = _evidence(
        2, artifact_id=102, source_format="image", trace_id="trace-1", service="checkout-ui",
        source_file="terminal.png", first_seen=base + timedelta(milliseconds=50),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[web, screenshot])

    assert len(run.result.components) == 1
    assert len(run.result.components[0].nodes) == 2
    assert len(run.result.components[0].edges) == 1
    assert any(s.value == "trace_id" for s in run.result.components[0].edges[0].signals)

def test_ocr_evidence_without_any_shared_signal_stays_its_own_component():
    base = datetime.now(timezone.utc)
    web = _evidence(1, artifact_id=101, source_format="web_server", service="checkout-api", first_seen=base)
    screenshot = _evidence(
        2, artifact_id=102, source_format="image", service="unrelated-tool",
        first_seen=base + timedelta(hours=5),
    )

    run = run_correlation(analysis_id=1, evidence_rows=[web, screenshot])

    assert len(run.result.components) == 2

def test_source_matches_survive_for_ocr_evidence_across_all_contexts():
    base = datetime.now(timezone.utc)
    matches = [
        {
            "relative_path": "src/checkout.tsx",
            "line_number": 88,
            "function": "handleSubmit",
            "snippet": "const handleSubmit = () => {\n  ...\n}",
            "match_method": "exact",
            "confidence": "high",
        }
    ]
    web = _evidence(1, artifact_id=101, source_format="web_server", trace_id="trace-1", service="checkout-api", first_seen=base)
    screenshot = _evidence(
        2, artifact_id=102, source_format="image", trace_id="trace-1", service="checkout-ui",
        source_file="terminal.png", source_matches=matches,
        first_seen=base + timedelta(milliseconds=10),
    )
    rows = [web, screenshot]
    artifacts = [_artifact_row(101, "nginx.log", "web_server"), _artifact_row(102, "terminal.png", "image")]

    run = run_correlation(analysis_id=1, evidence_rows=rows)

    correlated_payload = build_correlation_payload(run, rows, artifacts=artifacts)
    screenshot_node = next(
        n for n in correlated_payload["components"][0]["nodes"] if n["artifact_id"] == 102
    )
    assert screenshot_node["source_matches"] == matches

    simple_payload = build_simple_payload(1, rows, artifacts=artifacts)
    simple_item = next(e for e in simple_payload["evidence"] if e["artifact_id"] == 102)
    assert simple_item["source_matches"] == matches

    correlated_context = build_llm_context(run, rows, artifacts=artifacts)
    context_matches = [
        item["source_matches"]
        for component in correlated_context["components"]
        for item in component["root_evidence"]
        if item["artifact_id"] == 102
    ]
    assert matches in context_matches

    simple_context = build_simple_llm_context(1, rows, artifacts=artifacts)
    simple_context_item = next(e for e in simple_context["evidence"] if e["artifact_id"] == 102)
    assert simple_context_item["source_matches"] == matches

def test_correlated_payload_preserves_the_full_existing_field_contract():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, source_format="database", trace_id="trace-1", service="orders-db", first_seen=base),
        _evidence(2, artifact_id=102, source_format="opentelemetry", trace_id="trace-1", span_id="s1", service="payment", identity_strength=1.0, first_seen=base + timedelta(milliseconds=10)),
    ]
    artifacts = [_artifact_row(101, "database.log", "database"), _artifact_row(102, "otel.json", "opentelemetry")]
    run = run_correlation(analysis_id=9, evidence_rows=rows)

    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    for key in (
        "analysis_id", "investigation_path", "evidence_count", "component_count",
        "evidence_artifact_count", "artifacts", "components",
    ):
        assert key in payload
    assert payload["investigation_path"] == "correlated"

    component = payload["components"][0]
    assert component["root_causes"]
    for candidate in component["root_causes"]:
        assert "root_cause_strength" in candidate
        assert "graph_stats" in candidate
    node = component["nodes"][0]
    for key in (
        "artifact_id", "source_file", "source_format", "source_matches",
        "identity_strength", "occurrence_count", "first_seen", "last_seen",
    ):
        assert key in node
    for edge in component["edges"]:
        assert "correlation_strength" in edge
        assert "delta_ms" in edge
        assert "signals" in edge

def test_simple_payload_never_fabricates_correlation_concepts():
    import json

    evidence_rows = [_evidence(1, artifact_id=1, service="worker")]
    payload = build_simple_payload(1, evidence_rows, artifacts=[_artifact_row(1, "worker.log", "generic")])

    assert payload["investigation_path"] == "simple"
    assert "components" not in payload
    text = json.dumps(payload)
    assert "correlation_strength" not in text
    assert "root_cause_strength" not in text
    assert "propagation" not in text
    assert '"edges"' not in text

def test_zero_evidence_payload_has_neutral_message_and_artifact_outcomes():
    artifacts = [_artifact_row(1, "nginx.log", "web_server"), _artifact_row(2, "random.log", "generic")]

    payload = build_zero_evidence_payload(1, artifacts=artifacts)

    assert payload["investigation_path"] == "zero_evidence"
    assert payload["evidence_count"] == 0
    assert "unrelated" not in payload["message"].lower()
    assert len(payload["artifacts"]) == 2
    for outcome in payload["artifacts"]:
        assert outcome["evidence_count"] == 0
        assert outcome["status"] == "processed"
        assert "unrelated" not in outcome["message"].lower()

def test_simple_payload_per_artifact_counts_and_zero_evidence_artifact():
    rows = [
        _evidence(1, artifact_id=1, service="a"),
        _evidence(2, artifact_id=1, service="a"),
        _evidence(3, artifact_id=2, service="b"),
    ]
    artifacts = [
        _artifact_row(1, "a.log", "generic"),
        _artifact_row(2, "b.log", "generic"),
        _artifact_row(3, "c.log", "generic"),
    ]

    payload = build_simple_payload(1, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert outcome_by_id[1]["evidence_count"] == 2
    assert outcome_by_id[2]["evidence_count"] == 1
    assert outcome_by_id[3]["evidence_count"] == 0
    assert "message" in outcome_by_id[3]

def test_multiple_components_remain_separate_and_never_labeled_unrelated():
    import json

    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, source_format="database", trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, source_format="web_server", trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=5)),
        _evidence(3, artifact_id=103, source_format="image", service="batch-tool", source_file="batch.png", first_seen=base + timedelta(hours=6)),
    ]
    artifacts = [
        _artifact_row(101, "database.log", "database"),
        _artifact_row(102, "nginx.log", "web_server"),
        _artifact_row(103, "batch.png", "image"),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 2

    payload = build_correlation_payload(run, rows, artifacts=artifacts)
    assert payload["component_count"] == 1
    assert payload["component_count_total"] == 2
    assert payload["excluded_component_count"] == 1
    assert "unrelated" not in json.dumps(payload).lower()

def test_simple_gemini_context_includes_provenance_and_artifact_outcomes():
    rows = [_evidence(1, artifact_id=1, service="worker", source_format="image", source_file="log.png")]
    artifacts = [_artifact_row(1, "log.png", "image")]

    context = build_simple_llm_context(1, rows, artifacts=artifacts)

    assert context["investigation_path"] == "simple"
    item = context["evidence"][0]
    assert item["source_format"] == "image"
    assert item["source_file"] == "log.png"
    assert context["artifacts"][0]["evidence_count"] == 1

def test_correlated_gemini_context_preserves_deterministic_strengths_and_artifacts():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, source_format="database", trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, source_format="web_server", trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=10)),
    ]
    artifacts = [_artifact_row(101, "database.log", "database"), _artifact_row(102, "nginx.log", "web_server")]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    context = build_llm_context(run, rows, artifacts=artifacts)

    component = context["components"][0]
    assert component["root_candidates"]
    assert "root_cause_strength" in component["root_candidates"][0]
    edge = component["propagation"][0]
    assert "correlation_strength" in edge
    assert "delta_ms" in edge
    assert edge["signals"]
    assert context["artifacts"]
    assert {a["artifact_id"] for a in context["artifacts"]} == {101, 102}

def _sqlite_session(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()
    return session_factory, db, analysis

def test_correlated_end_to_end_publishes_investigation_result_once(monkeypatch):
    session_factory, db, analysis = _sqlite_session(monkeypatch)
    nginx = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="nginx.log", saved_file_path="nginx.log",
        size_bytes=10, detected_format="web_server", status="completed", last_processed_line=1, processed_bytes=10,
    )
    shot = AnalysisArtifact(
        analysis_id=analysis.id, position=1, original_filename="terminal.png", saved_file_path="terminal.png",
        size_bytes=10, detected_format="image", status="completed", last_processed_line=1, processed_bytes=10,
    )
    db.add_all([nginx, shot])
    db.commit()
    base = datetime.now(timezone.utc)
    db.add_all([
        Evidence(analysis_id=analysis.id, artifact_id=nginx.id, correlation_key="ck-1", fingerprint="fp-1",
                  trace_id="trace-1", service="checkout-api", source_format="web_server",
                  first_line_number=1, last_line_number=1, first_seen=base, last_seen=base),
        Evidence(analysis_id=analysis.id, artifact_id=shot.id, correlation_key="ck-2", fingerprint="fp-2",
                  trace_id="trace-1", service="checkout-ui", source_format="image", source_file="terminal.png",
                  first_line_number=1, last_line_number=1,
                  first_seen=base + timedelta(milliseconds=20), last_seen=base + timedelta(milliseconds=20)),
    ])
    db.commit()
    analysis_id = analysis.id
    db.close()

    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT
    )
    investigation_calls = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: investigation_calls.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(investigation_calls) == 1
    assert investigation_calls[0]["investigation_path"] == "correlated"
    outcome_by_format = {a["source_format"] for a in investigation_calls[0]["artifacts"]}
    assert outcome_by_format == {"web_server", "image"}

def test_correlation_result_event_no_longer_exists(monkeypatch):
    from app.services import analysis_events

    assert not hasattr(analysis_events, "publish_correlation_result")
    assert not hasattr(analysis_task, "publish_correlation_result")

def test_simple_end_to_end_publishes_investigation_result_only(monkeypatch):
    session_factory, db, analysis = _sqlite_session(monkeypatch)
    artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="worker.log", saved_file_path="worker.log",
        size_bytes=10, detected_format="generic", status="completed", last_processed_line=1, processed_bytes=10,
    )
    db.add(artifact)
    db.commit()
    db.add(
        Evidence(analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1", fingerprint="fp-1",
                  service="worker", source_format="generic", first_line_number=1, last_line_number=1)
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT
    )
    investigation_calls = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: investigation_calls.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(investigation_calls) == 1
    payload = investigation_calls[0]
    assert payload["investigation_path"] == "simple"
    assert "components" not in payload
    assert payload["evidence"][0]["service"] == "worker"

def test_zero_evidence_end_to_end_builds_no_gemini_context(monkeypatch):
    session_factory, db, analysis = _sqlite_session(monkeypatch)
    artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="random.log", saved_file_path="random.log",
        size_bytes=10, detected_format="generic", status="completed", last_processed_line=1, processed_bytes=10,
    )
    db.add(artifact)
    db.commit()
    analysis_id = analysis.id
    db.close()

    investigation_calls = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: investigation_calls.append(p))
    simple_context_builder = Mock()
    correlated_context_builder = Mock()
    monkeypatch.setattr(analysis_task, "build_simple_llm_context", simple_context_builder)
    monkeypatch.setattr(analysis_task, "build_llm_context", correlated_context_builder)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    simple_context_builder.assert_not_called()
    correlated_context_builder.assert_not_called()

    assert len(investigation_calls) == 1
    payload = investigation_calls[0]
    assert payload["investigation_path"] == "zero_evidence"
    assert payload["artifacts"][0]["source_file"] == "random.log"
    assert payload["artifacts"][0]["evidence_count"] == 0
