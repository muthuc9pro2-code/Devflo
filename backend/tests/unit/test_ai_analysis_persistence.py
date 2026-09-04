from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.gemini_service import GeminiUnavailableError
from app.tasks import analysis as analysis_task

_FAKE_GEMINI_RESULT = GeminiInvestigationResponse(
    title="Database timeout in payment worker",
    summary="A database timeout caused cascading payment failures.",
    probable_root_causes=[],
    what_happened=["The database connection pool was exhausted."],
    source_code_findings=[],
    recommended_actions=["Increase the connection pool size."],
    uncertainties=[],
)

def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory

def _seed_analysis(session_factory, *, evidence_rows_kwargs: list[dict]) -> int:
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
        analysis_id=analysis.id,
        position=0,
        original_filename="artifact-0",
        saved_file_path="artifact-0",
        size_bytes=10,
        status="completed",
        last_processed_line=1,
        processed_bytes=10,
    )
    db.add(artifact)
    db.commit()

    base = datetime.now(timezone.utc)
    for i, kwargs in enumerate(evidence_rows_kwargs):
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

def _mock_gemini(monkeypatch, calls: list):
    def fake(context):
        calls.append(context)
        return _FAKE_GEMINI_RESULT

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", fake)

def test_live_correlated_completion_persists_ai_analysis(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[
            {"trace_id": "trace-1", "service": "db"},
            {"trace_id": "trace-1", "service": "api"},
        ],
    )
    calls: list = []
    _mock_gemini(monkeypatch, calls)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(calls) == 1

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis == _FAKE_GEMINI_RESULT.model_dump()
    db.close()

def test_live_simple_completion_persists_ai_analysis(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[{"service": "worker"}],
    )
    calls: list = []
    _mock_gemini(monkeypatch, calls)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(calls) == 1

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis == _FAKE_GEMINI_RESULT.model_dump()
    db.close()

def test_reconnect_returns_persisted_ai_analysis_without_calling_gemini_again(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[
            {"trace_id": "trace-1", "service": "db"},
            {"trace_id": "trace-1", "service": "api"},
        ],
    )
    live_calls: list = []
    _mock_gemini(monkeypatch, live_calls)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)
    assert len(live_calls) == 1

    def _must_not_be_called(context):
        raise AssertionError("reconnect must not call Gemini again")

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _must_not_be_called)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    state = analysis_task.compute_current_analysis_state(db, analysis)
    db.close()

    assert state["status"] == "completed"
    assert state["investigation_result"]["ai_analysis"] == _FAKE_GEMINI_RESULT.model_dump()

def test_zero_evidence_analysis_has_no_ai_analysis(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(session_factory, evidence_rows_kwargs=[])
    calls: list = []
    _mock_gemini(monkeypatch, calls)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert calls == []

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.ai_analysis is None
    state = analysis_task.compute_current_analysis_state(db, analysis)
    db.close()

    assert "ai_analysis" not in state["investigation_result"]

def _raise_gemini_unavailable(context):
    raise GeminiUnavailableError("This model is currently experiencing high demand.")

def test_live_correlated_completion_survives_gemini_unavailable(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[
            {"trace_id": "trace-1", "service": "db"},
            {"trace_id": "trace-1", "service": "api"},
        ],
    )
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert analysis.result_snapshot["investigation_path"] == "correlated"
    assert len(analysis.result_snapshot["components"]) == 1
    assert "ai_analysis" not in analysis.result_snapshot
    db.close()

def test_live_simple_completion_survives_gemini_unavailable(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[{"service": "worker"}],
    )
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert analysis.result_snapshot["investigation_path"] == "simple"
    assert "ai_analysis" not in analysis.result_snapshot
    db.close()

def test_reconnect_after_gemini_unavailable_completion_shows_deterministic_result_without_calling_gemini_again(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[
            {"trace_id": "trace-1", "service": "db"},
            {"trace_id": "trace-1", "service": "api"},
        ],
    )
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    def _must_not_be_called(context):
        raise AssertionError("reconnect must not call Gemini again")

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _must_not_be_called)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    state = analysis_task.compute_current_analysis_state(db, analysis)
    db.close()

    assert state["status"] == "completed"
    assert state["investigation_result"]["investigation_path"] == "correlated"
    assert "ai_analysis" not in state["investigation_result"]
