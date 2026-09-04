from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.api import analysis as analysis_api
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.gemini_service import GeminiUnavailableError
from app.tasks import analysis as analysis_task

def _upload(filename: str, content: bytes):
    return SimpleNamespace(filename=filename, content_type="text/plain", file=BytesIO(content))

def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory

def _raise_gemini_unavailable(_context):
    raise GeminiUnavailableError("temporarily unavailable")

_FAKE_GEMINI_RESULT = GeminiInvestigationResponse(
    title="t",
    summary="s",
    probable_root_causes=[],
    what_happened=[],
    source_code_findings=[],
    recommended_actions=[],
    uncertainties=[],
)

def _seed_source_analysis(
    session_factory,
    *,
    source_kind,
    evidence_kwargs: list[dict],
    fallback_context=None,
) -> int:
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    analysis = Analysis(
        user_id=user.id,
        original_filename="a",
        saved_file_path="a",
        status="processing",
        source_kind=source_kind,
        source_reference="irrelevant-for-this-test",
        source_status="ready",
        source_failure_reason=None,
    )
    db.add(analysis)
    db.commit()

    artifact = AnalysisArtifact(
        analysis_id=analysis.id,
        position=0,
        original_filename="artifact-0",
        saved_file_path="artifact-0",
        size_bytes=10,
        status="completed",
        last_processed_line=1,
        processed_bytes=10,
        fallback_context=fallback_context,
    )
    db.add(artifact)
    db.commit()

    base = datetime.now(timezone.utc)
    for i, kwargs in enumerate(evidence_kwargs):
        defaults = dict(
            analysis_id=analysis.id,
            artifact_id=artifact.id,
            correlation_key=f"ck-{i}",
            fingerprint=f"fp-{i}",
            first_line_number=1,
            last_line_number=1,
            first_seen=base,
            severity="ERROR",
        )
        defaults.update(kwargs)
        db.add(Evidence(**defaults))
    db.commit()

    analysis_id = analysis.id
    db.close()
    return analysis_id

def _quiet(monkeypatch):
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

def _capture_gemini_context(monkeypatch):
    captured = {}

    def _fake(context):
        captured["context"] = context
        return _FAKE_GEMINI_RESULT

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _fake)
    return captured

_SOURCE_MATCH = [
    {
        "relative_path": "app/main.py",
        "requested_path": "app/main.py",
        "line_number": 5,
        "function": "run",
        "snippet": "print('hi')",
        "match_method": "exact",
        "confidence": "high",
    }
]

def test_source_zip_with_matches_reports_match_count(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet(monkeypatch)
    captured = _capture_gemini_context(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        evidence_kwargs=[
            {"service": "worker", "source_matches": _SOURCE_MATCH},
            {"service": "api", "source_matches": None},
        ],
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    db.close()

    source = analysis.result_snapshot["source"]
    assert source["status"] == "ready"
    assert source["match_count"] == 1
    assert source["failure_reason"] is None
    assert "source_context" not in captured["context"]

def test_source_zip_with_zero_matches_stays_ready_not_unavailable(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet(monkeypatch)
    captured = _capture_gemini_context(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        evidence_kwargs=[
            {"service": "worker", "source_matches": None},
            {"service": "api", "source_matches": []},
        ],
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    db.close()

    source = analysis.result_snapshot["source"]
    assert source["status"] == "ready"
    assert source["match_count"] == 0
    assert source["failure_reason"] is None
    assert captured["context"]["source_context"] == {
        "status": "ready",
        "match_count": 0,
    }

def test_correlated_zero_match_source_context_reaches_gemini(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet(monkeypatch)
    captured = _capture_gemini_context(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        evidence_kwargs=[
            {
                "service": "payment-api",
                "trace_id": "trace-1",
                "source_matches": None,
            },
            {
                "service": "database",
                "trace_id": "trace-1",
                "source_matches": [],
            },
        ],
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert captured["context"]["investigation_path"] == "correlated"
    assert captured["context"]["source_context"] == {
        "status": "ready",
        "match_count": 0,
    }

def test_github_source_with_zero_matches_stays_ready_not_unavailable(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="github",
        evidence_kwargs=[{"service": "worker", "source_matches": None}],
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    db.close()

    source = analysis.result_snapshot["source"]
    assert source["status"] == "ready"
    assert source["match_count"] == 0

def test_invalid_zip_content_gives_a_specific_safe_reason_and_diagnostics_continue(
    tmp_path, monkeypatch
):
    create_analysis = Mock(return_value=SimpleNamespace(id=30, artifacts=[]))
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    result = analysis_api.upload_file(
        file=_upload("diagnostic.log", b"ERROR failed"),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
        source_zip=_upload("source.zip", b"not actually a zip file"),
    )

    assert result.id == 30
    kwargs = create_analysis.call_args.kwargs
    assert kwargs["source_kind"] == "zip"
    assert kwargs["source_status"] == "unavailable"
    assert kwargs["source_failure_reason"] == (
        "Uploaded source ZIP could not be prepared: Uploaded file is not a valid ZIP archive"
    )
    assert [row["original_filename"] for row in kwargs["artifacts"]] == ["diagnostic.log"]
    task.delay.assert_called_once_with(30)

def test_no_source_supplied_omits_the_source_key_entirely(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet(monkeypatch)
    captured = _capture_gemini_context(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind=None,
        evidence_kwargs=[{"service": "worker"}],
    )
    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    analysis.source_status = None
    db.commit()
    db.close()

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    db.close()

    assert "source" not in analysis.result_snapshot
    assert "source_context" not in captured["context"]

def test_unstructured_fallback_carries_ready_zero_match_source_context(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet(monkeypatch)
    captured = _capture_gemini_context(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        evidence_kwargs=[],
        fallback_context={
            "kind": "text",
            "text": "payment worker stopped after restart",
        },
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert captured["context"]["context_kind"] == "unstructured_fallback"
    assert captured["context"]["source_context"] == {
        "status": "ready",
        "match_count": 0,
    }
