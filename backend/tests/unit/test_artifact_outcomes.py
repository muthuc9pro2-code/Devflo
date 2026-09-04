from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import (
    build_correlation_payload,
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
        "first_line_number": 10,
        "last_line_number": 12,
        "representative_line": "ERROR something failed",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)

def _artifact_row(
    artifact_id: int,
    filename: str,
    fmt: str,
    status: str = "completed",
    duplicate_of_artifact_id: int | None = None,
):
    return SimpleNamespace(
        id=artifact_id,
        original_filename=filename,
        detected_format=fmt,
        status=status,
        duplicate_of_artifact_id=duplicate_of_artifact_id,
    )

def test_artifact_with_evidence_is_reported_processed_with_no_message():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="a", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="b", first_seen=base + timedelta(milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    artifacts = [
        _artifact_row(101, "nginx.log", "web_server"),
        _artifact_row(102, "otel.json", "opentelemetry"),
    ]

    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert outcome_by_id[101]["evidence_count"] == 1
    assert outcome_by_id[101]["status"] == "processed"
    assert outcome_by_id[101]["source_file"] == "nginx.log"
    assert outcome_by_id[101]["source_format"] == "web_server"
    assert "message" not in outcome_by_id[101]

def test_zero_evidence_artifact_is_reported_processed_with_neutral_message():
    base = datetime.now(timezone.utc)
    rows = [_evidence(1, artifact_id=101, service="a", first_seen=base)]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    artifacts = [
        _artifact_row(101, "nginx.log", "web_server"),
        _artifact_row(999, "random.log", "generic"),
    ]

    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    zero = outcome_by_id[999]
    assert zero["evidence_count"] == 0
    assert zero["status"] == "processed"
    assert zero["source_file"] == "random.log"
    assert zero["source_format"] == "generic"
    assert zero["message"] == (
        "No meaningful diagnostic evidence was extracted from this artifact."
    )

def test_zero_evidence_artifact_does_not_alter_other_artifacts_evidence():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="payment", first_seen=base + timedelta(milliseconds=10)),
        _evidence(3, artifact_id=103, trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=20)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    artifacts = [
        _artifact_row(101, "database.log", "database"),
        _artifact_row(102, "otel.json", "opentelemetry"),
        _artifact_row(103, "nginx.log", "web_server"),
        _artifact_row(104, "random.log", "generic"),
    ]

    without_artifacts = build_correlation_payload(run, rows)
    with_artifacts = build_correlation_payload(run, rows, artifacts=artifacts)

    for key in ("evidence_count", "component_count", "evidence_artifact_count", "components"):
        assert with_artifacts[key] == without_artifacts[key]

    assert with_artifacts["evidence_count"] == 3
    assert with_artifacts["evidence_artifact_count"] == 3

    outcome_by_id = {a["artifact_id"]: a for a in with_artifacts["artifacts"]}
    assert len(outcome_by_id) == 4
    for real_artifact_id in (101, 102, 103):
        assert outcome_by_id[real_artifact_id]["evidence_count"] == 1
        assert "message" not in outcome_by_id[real_artifact_id]
    assert outcome_by_id[104]["evidence_count"] == 0
    assert outcome_by_id[104]["message"]

def test_zero_evidence_message_never_claims_unrelated():
    rows = [_evidence(1, artifact_id=101)]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    artifacts = [_artifact_row(101, "a.log", "generic"), _artifact_row(202, "random.log", "generic")]

    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    zero = next(a for a in payload["artifacts"] if a["artifact_id"] == 202)
    assert "unrelated" not in zero["message"].lower()
    assert "corrupt" not in zero["message"].lower()
    assert "unsupported" not in zero["message"].lower()

def test_evidence_in_a_separate_correlation_component_is_not_zero_evidence():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="api", first_seen=base + timedelta(milliseconds=10)),
        _evidence(
            3, artifact_id=103, service="unrelated-batch-job",
            first_seen=base + timedelta(hours=6),
        ),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 2

    artifacts = [
        _artifact_row(101, "database.log", "database"),
        _artifact_row(102, "nginx.log", "web_server"),
        _artifact_row(103, "batch.log", "generic"),
    ]
    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert outcome_by_id[103]["evidence_count"] == 1
    assert outcome_by_id[103]["relationship_status"] == "not_linked"
    assert outcome_by_id[103]["message"] == "Not linked to the correlated incident evidence."
    assert outcome_by_id[101]["relationship_status"] == "linked"
    assert outcome_by_id[102]["relationship_status"] == "linked"

def test_artifact_with_evidence_split_across_primary_and_isolated_is_partially_linked():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="db", first_seen=base),
        _evidence(
            2, artifact_id=102, trace_id="trace-1", service="api",
            first_seen=base + timedelta(milliseconds=10),
        ),
        _evidence(
            3, artifact_id=101, fingerprint="fp-unrelated", service="unrelated-svc",
            first_seen=base + timedelta(hours=6),
        ),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 2

    artifacts = [
        _artifact_row(101, "database.log", "database"),
        _artifact_row(102, "nginx.log", "web_server"),
    ]
    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert outcome_by_id[101]["relationship_status"] == "partially_linked"
    assert "linked" in outcome_by_id[101]["message"].lower()
    assert outcome_by_id[102]["relationship_status"] == "linked"

def test_scenario_e_four_related_artifacts_plus_one_unrelated_log():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="gateway", source_format="cloud_gateway", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="checkout-api", source_format="web_server", first_seen=base + timedelta(milliseconds=20)),
        _evidence(3, artifact_id=103, trace_id="trace-1", service="orders-db", source_format="database", first_seen=base + timedelta(milliseconds=60)),
        _evidence(4, artifact_id=104, trace_id="trace-1", service="orders-db", source_format="opentelemetry", span_id="span-4", first_seen=base + timedelta(milliseconds=70)),
        _evidence(5, artifact_id=105, service="batch-job", first_seen=base + timedelta(days=1)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    assert len(run.result.components) == 2

    artifacts = [
        _artifact_row(101, "gateway.log", "cloud_gateway"),
        _artifact_row(102, "nginx.log", "web_server"),
        _artifact_row(103, "database.log", "database"),
        _artifact_row(104, "otel.json", "opentelemetry"),
        _artifact_row(105, "batch.log", "generic"),
    ]
    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    for artifact_id in (101, 102, 103, 104):
        assert outcome_by_id[artifact_id]["relationship_status"] == "linked"
    assert outcome_by_id[105]["relationship_status"] == "not_linked"
    assert outcome_by_id[105]["evidence_count"] == 1

    primary_component = max(payload["components"], key=lambda c: len(c["nodes"]))
    linked_artifact_ids = {node["artifact_id"] for node in primary_component["nodes"]}
    assert 105 not in linked_artifact_ids

    component_ids_with_artifact_103 = [
        component["id"]
        for component in payload["components"]
        if any(node["artifact_id"] == 103 for node in component["nodes"])
    ]
    assert component_ids_with_artifact_103

def test_artifacts_key_omitted_entirely_when_not_provided():
    rows = [_evidence(1, artifact_id=101)]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    payload = build_correlation_payload(run, rows)

    assert "artifacts" not in payload

def _sqlite_analysis_with_mixed_evidence(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    db.add(analysis)
    db.commit()

    artifacts = [
        AnalysisArtifact(
            analysis_id=analysis.id, position=0, original_filename="nginx.log",
            saved_file_path="nginx.log", size_bytes=10, detected_format="web_server",
            status="completed", last_processed_line=8, processed_bytes=10,
        ),
        AnalysisArtifact(
            analysis_id=analysis.id, position=1, original_filename="otel.json",
            saved_file_path="otel.json", size_bytes=10, detected_format="opentelemetry",
            status="completed", last_processed_line=12, processed_bytes=10,
        ),
        AnalysisArtifact(
            analysis_id=analysis.id, position=2, original_filename="random.log",
            saved_file_path="random.log", size_bytes=10, detected_format="generic",
            status="completed", last_processed_line=4, processed_bytes=10,
        ),
    ]
    db.add_all(artifacts)
    db.commit()

    base = datetime.now(timezone.utc)
    evidence_rows = [
        Evidence(
            analysis_id=analysis.id, artifact_id=artifacts[0].id,
            correlation_key="ck-1", fingerprint="fp-1", trace_id="trace-1",
            service="checkout-api", source_format="web_server",
            first_line_number=1, last_line_number=1, first_seen=base, last_seen=base,
        ),
        Evidence(
            analysis_id=analysis.id, artifact_id=artifacts[1].id,
            correlation_key="ck-2", fingerprint="fp-2", trace_id="trace-1",
            service="checkout-api", source_format="opentelemetry",
            first_line_number=1, last_line_number=1,
            first_seen=base + timedelta(milliseconds=15),
            last_seen=base + timedelta(milliseconds=15),
        ),
    ]
    db.add_all(evidence_rows)
    db.commit()

    analysis_id = analysis.id
    artifact_ids = [artifact.id for artifact in artifacts]
    db.close()
    return analysis_id, artifact_ids

def test_finalize_publishes_artifact_outcomes_for_mixed_evidence_analysis(monkeypatch):
    analysis_id, artifact_ids = _sqlite_analysis_with_mixed_evidence(monkeypatch)

    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT
    )
    investigation_results = []
    monkeypatch.setattr(
        analysis_task,
        "publish_investigation_result",
        lambda analysis_id, payload: investigation_results.append(payload),
    )
    published_progress = []
    monkeypatch.setattr(
        analysis_task,
        "publish_progress",
        lambda analysis_id, stage, message, progress=None: published_progress.append(progress),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    session_factory = analysis_task.sessionLocal
    db = session_factory()
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        assert analysis.status == "completed"
    finally:
        db.close()

    assert len(investigation_results) == 1
    payload = investigation_results[0]
    assert payload["investigation_path"] == "correlated"

    assert payload["evidence_count"] == 2
    assert payload["evidence_artifact_count"] == 2

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert len(outcome_by_id) == 3

    nginx_id, otel_id, random_id = artifact_ids
    assert outcome_by_id[nginx_id]["evidence_count"] == 1
    assert outcome_by_id[nginx_id]["status"] == "processed"
    assert "message" not in outcome_by_id[nginx_id]

    assert outcome_by_id[otel_id]["evidence_count"] == 1
    assert "message" not in outcome_by_id[otel_id]

    zero = outcome_by_id[random_id]
    assert zero["source_file"] == "random.log"
    assert zero["evidence_count"] == 0
    assert zero["status"] == "processed"
    assert zero["message"] == (
        "No meaningful diagnostic evidence was extracted from this artifact."
    )
    assert "unrelated" not in zero["message"].lower()

    assert 100 not in published_progress
    assert published_progress

def test_finalize_zero_evidence_whole_analysis_path_is_unchanged(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    db.add(analysis)
    db.commit()
    db.add(
        AnalysisArtifact(
            analysis_id=analysis.id, position=0, original_filename="random.log",
            saved_file_path="random.log", size_bytes=10, detected_format="generic",
            status="completed", last_processed_line=4, processed_bytes=10,
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    investigation_results = []
    monkeypatch.setattr(
        analysis_task,
        "publish_investigation_result",
        lambda analysis_id, payload: investigation_results.append(payload),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(investigation_results) == 1
    assert investigation_results[0]["investigation_path"] == "zero_evidence"
    assert "components" not in investigation_results[0]
    assert "edges" not in investigation_results[0]

    db = session_factory()
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        assert analysis.status == "completed"
    finally:
        db.close()

def test_all_four_outcome_kinds_are_distinguishable_in_the_simple_payload():
    rows = [_evidence(1, artifact_id=1, source_format="web_server")]
    artifacts = [
        _artifact_row(1, "nginx.log", "web_server"),
        _artifact_row(2, "random.log", "generic"),
        _artifact_row(3, "something.xyz", None, status="unsupported"),
        _artifact_row(
            4, "nginx-copy.log", "web_server", status="duplicate",
            duplicate_of_artifact_id=1,
        ),
    ]

    payload = build_simple_payload(1, rows, artifacts=artifacts)
    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}

    processed = outcome_by_id[1]
    assert processed["status"] == "processed"
    assert processed["evidence_count"] == 1
    assert "message" not in processed

    zero_evidence = outcome_by_id[2]
    assert zero_evidence["status"] == "processed"
    assert zero_evidence["evidence_count"] == 0
    assert "unrelated" not in zero_evidence["message"].lower()

    unsupported = outcome_by_id[3]
    assert unsupported["status"] == "unsupported"
    assert unsupported["source_file"] == "something.xyz"
    assert unsupported["source_format"] is None
    assert unsupported["evidence_count"] == 0
    assert "not supported" in unsupported["message"].lower()

    duplicate = outcome_by_id[4]
    assert duplicate["status"] == "duplicate"
    assert duplicate["source_file"] == "nginx-copy.log"
    assert duplicate["evidence_count"] == 0
    assert duplicate["duplicate_of_artifact_id"] == 1
    assert duplicate["duplicate_of_source_file"] == "nginx.log"
    assert "nginx.log" in duplicate["message"]

    assert {o["status"] for o in payload["artifacts"]} == {
        "processed", "unsupported", "duplicate",
    }

def test_unsupported_and_duplicate_outcomes_available_in_correlated_payload():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, artifact_id=101, trace_id="trace-1", service="api", first_seen=base),
        _evidence(2, artifact_id=102, trace_id="trace-1", service="ui", first_seen=base + timedelta(milliseconds=5)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    artifacts = [
        _artifact_row(101, "nginx.log", "web_server"),
        _artifact_row(102, "otel.json", "opentelemetry"),
        _artifact_row(103, "something.xyz", None, status="unsupported"),
        _artifact_row(
            104, "nginx-copy.log", "web_server", status="duplicate",
            duplicate_of_artifact_id=101,
        ),
    ]

    payload = build_correlation_payload(run, rows, artifacts=artifacts)
    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}

    assert outcome_by_id[103]["status"] == "unsupported"
    assert outcome_by_id[104]["status"] == "duplicate"
    assert outcome_by_id[104]["duplicate_of_source_file"] == "nginx.log"
    assert payload["evidence_count"] == 2
    assert payload["component_count"] == 1

def test_unsupported_and_duplicate_outcomes_available_in_zero_evidence_payload():
    artifacts = [
        _artifact_row(1, "random.log", "generic"),
        _artifact_row(2, "something.xyz", None, status="unsupported"),
        _artifact_row(
            3, "random-copy.log", "generic", status="duplicate",
            duplicate_of_artifact_id=1,
        ),
    ]

    payload = build_zero_evidence_payload(1, artifacts=artifacts)
    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}

    assert outcome_by_id[2]["status"] == "unsupported"
    assert outcome_by_id[3]["status"] == "duplicate"
    assert outcome_by_id[3]["duplicate_of_source_file"] == "random.log"

def test_duplicate_never_counts_as_evidence_or_strengthens_correlation():
    rows = [_evidence(1, artifact_id=1, source_format="web_server")]
    artifacts = [
        _artifact_row(1, "nginx.log", "web_server"),
        _artifact_row(2, "nginx-copy.log", "web_server", status="duplicate", duplicate_of_artifact_id=1),
    ]

    payload = build_simple_payload(1, rows, artifacts=artifacts)

    duplicate = next(a for a in payload["artifacts"] if a["artifact_id"] == 2)
    assert duplicate["evidence_count"] == 0
    assert payload["evidence_count"] == 1
    assert payload["evidence_artifact_count"] == 1
