"""Zero-structured-evidence SIMPLE unstructured fallback.

A user should be able to upload a small TXT with no formal ERROR/Traceback
token, or a screenshot whose OCR text the parser could not fully structure,
and still get a useful Gemini explanation - WITHOUT inventing a second
investigation engine, without a graph, and without re-reading a text
artifact or re-running RapidOCR merely to build this context.
"""
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.fallback_context import (
    capture_ocr_fallback_context,
    capture_text_fallback_context,
)
from app.tasks import analysis as analysis_task

_FAKE_GEMINI_RESULT = GeminiInvestigationResponse(
    title="Payment worker cannot reach the database",
    summary="The worker stops shortly after restart, likely a DB connectivity issue.",
    probable_root_causes=[],
    what_happened=["The worker restarted and then stopped."],
    source_code_findings=[],
    recommended_actions=["Verify the database is reachable from the worker host."],
    uncertainties=["No formal error was captured, only free text."],
)


# --- capture_text_fallback_context(): permissive gate for small TXT -------


def test_scenario_a_small_developer_txt_without_error_token_passes_the_gate():
    text = (
        "payment worker stops after I restart the service\n"
        "it can no longer reach the database"
    )
    context = capture_text_fallback_context(text)
    assert context == {"kind": "text", "text": text}


def test_empty_or_tiny_text_does_not_pass_the_gate():
    assert capture_text_fallback_context("") is None
    assert capture_text_fallback_context("   ") is None
    assert capture_text_fallback_context("hi") is None


def test_binary_control_garbage_does_not_pass_the_gate():
    garbage = "".join(chr(c) for c in range(0, 20)) * 5
    assert capture_text_fallback_context(garbage) is None


def test_text_fallback_is_bounded_to_the_configured_byte_limit():
    from app.core.processing_config import SIMPLE_FALLBACK_MAX_TEXT_BYTES

    huge = "diagnostic detail line\n" * 10_000
    context = capture_text_fallback_context(huge)
    assert context is not None
    assert len(context["text"].encode("utf-8")) <= SIMPLE_FALLBACK_MAX_TEXT_BYTES


# --- capture_ocr_fallback_context(): stronger gate for images -------------


def test_scenario_d_irrelevant_photograph_ocr_does_not_pass_the_stronger_gate():
    """EXIT / ROOM 3 / WELCOME - non-trivial readable text, but not
    developer diagnostic context. Must not become fallback evidence."""
    text = "EXIT\nROOM 3\nWELCOME\nHave a nice day!"
    assert capture_ocr_fallback_context(text, 0.95) is None


def test_scenario_c_technical_screenshot_text_passes_the_stronger_gate():
    text = "Traceback shows failure at /srv/app/worker.py:42 ConnectionError raised"
    context = capture_ocr_fallback_context(text, 0.9)
    assert context is not None
    assert context["kind"] == "ocr"
    assert context["ocr_confidence"] == 0.9


def test_ocr_text_with_only_http_status_still_passes_the_gate():
    context = capture_ocr_fallback_context("request failed with status 503 upstream", 0.8)
    assert context is not None


def test_ocr_text_with_trace_id_still_passes_the_gate():
    context = capture_ocr_fallback_context("trace_id=4bf92f3577b34da6a3ce929d0e0e4736 failed", 0.8)
    assert context is not None


def test_ocr_confidence_is_never_fabricated():
    text = "ConnectionError raised while starting worker"
    context = capture_ocr_fallback_context(text, None)
    assert context is not None
    assert context["ocr_confidence"] is None


# --- end-to-end: _finalize_analysis_task wiring ----------------------------


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def _seed_zero_evidence_analysis(
    session_factory, *, artifact_fallback_contexts: list[dict | None]
) -> int:
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    db.add(analysis)
    db.commit()

    for position, fallback_context in enumerate(artifact_fallback_contexts):
        db.add(
            AnalysisArtifact(
                analysis_id=analysis.id,
                position=position,
                original_filename=f"artifact-{position}.txt",
                saved_file_path=f"artifact-{position}.txt",
                size_bytes=10,
                status="completed",
                last_processed_line=1,
                processed_bytes=10,
                fallback_context=fallback_context,
            )
        )
    db.commit()

    analysis_id = analysis.id
    db.close()
    return analysis_id


def _mock_gemini(monkeypatch, calls: list):
    def fake(context):
        calls.append(context)
        return _FAKE_GEMINI_RESULT

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", fake)


def test_finalize_uses_fallback_when_whole_analysis_has_zero_evidence(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_zero_evidence_analysis(
        session_factory,
        artifact_fallback_contexts=[
            {"kind": "text", "text": "payment worker stops after restart"}
        ],
    )
    calls: list = []
    _mock_gemini(monkeypatch, calls)
    published = []
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(calls) == 1  # exactly one Gemini call
    assert calls[0]["context_kind"] == "unstructured_fallback"

    assert len(published) == 1
    payload = published[0]
    assert payload["investigation_path"] == "simple"
    assert payload["context_kind"] == "unstructured_fallback"
    assert payload["evidence_count"] == 0
    assert "components" not in payload
    assert payload["fallback_context"][0]["text"] == "payment worker stops after restart"

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis == _FAKE_GEMINI_RESULT.model_dump()
    db.close()


def test_finalize_stays_true_zero_evidence_when_no_artifact_has_fallback_context(monkeypatch):
    """End to end: no usable fallback anywhere -> no Gemini call
    at all, the existing zero_evidence contract is unchanged."""
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_zero_evidence_analysis(
        session_factory, artifact_fallback_contexts=[None, None]
    )
    calls: list = []
    _mock_gemini(monkeypatch, calls)
    published = []
    monkeypatch.setattr(
        analysis_task, "publish_investigation_result", lambda aid, p: published.append(p)
    )
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert calls == []
    assert len(published) == 1
    assert published[0]["investigation_path"] == "zero_evidence"

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.ai_analysis is None
    db.close()


def test_reconnect_restores_fallback_result_without_calling_gemini_again(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_zero_evidence_analysis(
        session_factory,
        artifact_fallback_contexts=[{"kind": "text", "text": "cannot reach the database"}],
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
    result = state["investigation_result"]
    assert result["context_kind"] == "unstructured_fallback"
    assert result["ai_analysis"] == _FAKE_GEMINI_RESULT.model_dump()


# --- Zero-evidence artifact inside a STRONG investigation ------------------


def test_fallback_never_fires_when_the_whole_analysis_already_has_real_evidence(monkeypatch):
    """A random/unrelated artifact's fallback context must not contaminate
    a strong investigation - the fallback path is only reachable through
    the evidence_count == 0 branch for the WHOLE analysis."""
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
    real_artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=0, original_filename="nginx.log",
        saved_file_path="nginx.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    random_artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=1, original_filename="notes.txt",
        saved_file_path="notes.txt", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
        fallback_context={"kind": "text", "text": "unrelated notes"},
    )
    db.add_all([real_artifact, random_artifact])
    db.commit()
    base = datetime.now(timezone.utc)
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=real_artifact.id, correlation_key="ck-1",
            fingerprint="fp-1", service="api", source_format="web_server",
            first_line_number=1, last_line_number=1, first_seen=base, severity="ERROR",
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    calls: list = []
    _mock_gemini(monkeypatch, calls)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    # Exactly one Gemini call for the REAL evidence (SIMPLE path, single
    # evidence row) - never a second "fallback" call for notes.txt.
    assert len(calls) == 1
    assert calls[0].get("context_kind") != "unstructured_fallback"


# --- Gemini failure isolation: the unstructured-fallback call site -------


def test_fallback_path_survives_gemini_unavailable(monkeypatch):
    """The third of the finalizer's three Gemini call sites (zero-
    structured-evidence unstructured fallback, alongside CORRELATED and
    SIMPLE in test_ai_analysis_persistence.py) must also complete with the
    deterministic fallback payload persisted and status="completed" when
    generate_investigation_explanation raises GeminiUnavailableError -
    never fail the analysis merely because the explanation layer is
    unavailable."""
    from app.services.gemini_service import GeminiUnavailableError

    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_zero_evidence_analysis(
        session_factory,
        artifact_fallback_contexts=[
            {"kind": "text", "text": "payment worker stops after restart"}
        ],
    )

    def _raise_unavailable(context):
        raise GeminiUnavailableError("This model is currently experiencing high demand.")

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert analysis.result_snapshot["context_kind"] == "unstructured_fallback"
    assert "ai_analysis" not in analysis.result_snapshot
    db.close()
