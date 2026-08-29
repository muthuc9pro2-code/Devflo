"""Sections 20-21: hard safety-net bounds on how much evidence a single
CORRELATED graph or SIMPLE frontend result is allowed to carry/serialize.
Evidence itself always stays fully persisted in MySQL - these bounds only
cap what one request/response cycle has to build and transmit, and always
expose the real total honestly rather than silently understating it.
"""
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import build_correlation_payload, build_simple_payload
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


# --- build_correlation_payload(): truncation-field exposure ---------------


def test_correlation_payload_exposes_truncation_fields_when_bounded():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="trace-1", first_seen=base),
        _evidence(2, trace_id="trace-1", first_seen=base + timedelta(milliseconds=10)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    payload = build_correlation_payload(run, rows, total_evidence_count=10_000)

    assert payload["evidence_count"] == 10_000
    assert payload["evidence_count_returned"] == 2
    assert payload["evidence_truncated"] is True


def test_correlation_payload_omits_truncation_fields_when_not_bounded():
    base = datetime.now(timezone.utc)
    rows = [
        _evidence(1, trace_id="trace-1", first_seen=base),
        _evidence(2, trace_id="trace-1", first_seen=base + timedelta(milliseconds=10)),
    ]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    payload = build_correlation_payload(run, rows)

    assert payload["evidence_count"] == 2
    assert "evidence_count_returned" not in payload
    assert "evidence_truncated" not in payload


# --- build_simple_payload(): bounded evidence array ------------------------


def test_simple_payload_bounds_a_large_evidence_array():
    from app.core.processing_config import SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS

    base = datetime.now(timezone.utc)
    rows = [
        _evidence(i, first_seen=base + timedelta(seconds=i))
        for i in range(SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS + 500)
    ]

    payload = build_simple_payload(analysis_id=1, evidence_rows=rows)

    assert payload["evidence_count"] == len(rows)  # real total, never understated
    assert len(payload["evidence"]) <= SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS
    assert payload["evidence_count_returned"] == len(payload["evidence"])
    assert payload["evidence_truncated"] is True


def test_simple_payload_small_list_is_never_truncated():
    rows = [_evidence(1), _evidence(2)]

    payload = build_simple_payload(analysis_id=1, evidence_rows=rows)

    assert payload["evidence_count"] == 2
    assert len(payload["evidence"]) == 2
    assert "evidence_truncated" not in payload


# --- end-to-end: _finalize_analysis_task actually applies the bound -------


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def test_finalize_applies_the_correlated_bound_end_to_end(monkeypatch):
    """A tiny CORRELATED_MAX_EVIDENCE_RECORDS makes the bound trivial to
    trigger without needing thousands of real rows in the test."""
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)

    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
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
    db.add_all(
        [
            Evidence(
                analysis_id=analysis.id, artifact_id=artifact.id, correlation_key=f"ck-{i}",
                fingerprint=f"fp-{i}", trace_id="trace-1", service="api",
                source_format="generic", first_line_number=1, last_line_number=1,
                first_seen=base + timedelta(milliseconds=i), severity="ERROR",
            )
            for i in range(1, 4)  # 3 rows, exceeds the monkeypatched bound of 2
        ]
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

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(published) == 1
    payload = published[0]
    assert payload["evidence_count"] == 3
    assert payload["evidence_count_returned"] == 2
    assert payload["evidence_truncated"] is True
