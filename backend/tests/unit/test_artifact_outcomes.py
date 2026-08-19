"""Per-artifact outcome information added to the existing correlation
payload (frontend contract): a supported artifact that was fully processed
but retained zero diagnostic evidence must be distinguishable from one that
found evidence, without ever being mislabeled "unrelated" and without
disturbing the existing zero-evidence-whole-analysis path, correlation
results, or SSE progress semantics.

Covers both the pure `build_correlation_payload()` contract (mirroring
test_investigation_context.py's style, with a real run_correlation()) and an
end-to-end real-sqlite `_finalize_analysis_task` run proving the production
wiring: a zero-evidence artifact never blocks the analysis, never removes
other artifacts' evidence, and shows up in the published correlation_result
payload.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import build_correlation_payload
from app.tasks import analysis as analysis_task


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


def _artifact_row(artifact_id: int, filename: str, fmt: str, status: str = "completed"):
    return SimpleNamespace(
        id=artifact_id,
        original_filename=filename,
        detected_format=fmt,
        status=status,
    )


# --- 1/2: build_correlation_payload artifact outcomes ----------------------


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


# --- 3/4: multiple artifacts, one zero-evidence, does not disturb others ---


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

    # Adding artifact outcomes is purely additive: every other field (the
    # real, correlated evidence graph) is byte-identical either way.
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


# --- 5: zero evidence is never described as "unrelated" --------------------


def test_zero_evidence_message_never_claims_unrelated():
    rows = [_evidence(1, artifact_id=101)]
    run = run_correlation(analysis_id=1, evidence_rows=rows)
    artifacts = [_artifact_row(101, "a.log", "generic"), _artifact_row(202, "random.log", "generic")]

    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    zero = next(a for a in payload["artifacts"] if a["artifact_id"] == 202)
    assert "unrelated" not in zero["message"].lower()
    assert "corrupt" not in zero["message"].lower()
    assert "unsupported" not in zero["message"].lower()


# --- 6: evidence in a separate/weakly-connected component isn't "zero" -----


def test_evidence_in_a_separate_correlation_component_is_not_zero_evidence():
    """A third artifact's evidence has no shared trace/request/etc. with
    the main incident and so lands in its own connected component - it must
    still be reported as evidence_count > 0 with no zero-evidence message,
    distinguishable via `components` (existing provenance), not conflated
    with "no evidence extracted"."""
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
    assert len(run.result.components) == 2  # confirms the fixture is genuinely disconnected

    artifacts = [
        _artifact_row(101, "database.log", "database"),
        _artifact_row(102, "nginx.log", "web_server"),
        _artifact_row(103, "batch.log", "generic"),
    ]
    payload = build_correlation_payload(run, rows, artifacts=artifacts)

    outcome_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert outcome_by_id[103]["evidence_count"] == 1
    assert "message" not in outcome_by_id[103]

    # Its evidence is genuinely present, just in the other component.
    component_ids_with_artifact_103 = [
        component["id"]
        for component in payload["components"]
        if any(node["artifact_id"] == 103 for node in component["nodes"])
    ]
    assert component_ids_with_artifact_103


# --- backward compatibility: artifacts param stays fully optional ---------


def test_artifacts_key_omitted_entirely_when_not_provided():
    rows = [_evidence(1, artifact_id=101)]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    payload = build_correlation_payload(run, rows)

    assert "artifacts" not in payload


# --- end-to-end: real sqlite finalize run, mixed-evidence artifacts -------


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
        # artifacts[2] ("random.log") intentionally has NO Evidence rows.
    ]
    db.add_all(evidence_rows)
    db.commit()

    analysis_id = analysis.id
    artifact_ids = [artifact.id for artifact in artifacts]
    db.close()
    return analysis_id, artifact_ids


def test_finalize_publishes_artifact_outcomes_for_mixed_evidence_analysis(monkeypatch):
    analysis_id, artifact_ids = _sqlite_analysis_with_mixed_evidence(monkeypatch)

    correlation_results = []
    monkeypatch.setattr(
        analysis_task,
        "publish_correlation_result",
        lambda analysis_id, payload: correlation_results.append(payload),
    )
    published_progress = []
    monkeypatch.setattr(
        analysis_task,
        "publish_progress",
        lambda analysis_id, stage, message, progress=None: published_progress.append(progress),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    # Requirement 3/7: overall analysis still succeeds normally.
    session_factory = analysis_task.sessionLocal
    db = session_factory()
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        assert analysis.status == "completed"
    finally:
        db.close()

    assert len(correlation_results) == 1
    payload = correlation_results[0]

    # Requirement 4: the real evidence from the two contributing artifacts
    # is untouched - correlated normally, not diluted by the zero-evidence one.
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

    # Requirements 2/5: zero-evidence artifact reported neutrally, never as
    # failed/unsupported/unrelated.
    zero = outcome_by_id[random_id]
    assert zero["source_file"] == "random.log"
    assert zero["evidence_count"] == 0
    assert zero["status"] == "processed"
    assert zero["message"] == (
        "No meaningful diagnostic evidence was extracted from this artifact."
    )
    assert "unrelated" not in zero["message"].lower()

    # Requirement 10: existing SSE progress semantics (never 100) untouched.
    assert 100 not in published_progress
    assert published_progress


def test_finalize_zero_evidence_whole_analysis_path_is_unchanged(monkeypatch):
    """Requirement 7: when NO artifact produced any evidence at all, the
    existing early-return "no meaningful diagnostic evidence found" path
    must still fire, with no correlation_result published at all - this
    task only adds per-artifact outcomes to the CORRELATED payload, it does
    not invent one for the whole-analysis-zero-evidence path."""
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

    correlation_results = []
    monkeypatch.setattr(
        analysis_task,
        "publish_correlation_result",
        lambda analysis_id, payload: correlation_results.append(payload),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    assert correlation_results == []

    db = session_factory()
    try:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        assert analysis.status == "completed"
    finally:
        db.close()
