"""Section I: Analysis.result_snapshot - the exact bounded final payload
persisted BEFORE it is published as investigation_result, for every one of
_finalize_analysis_task's four terminal branches (correlated / simple /
zero_evidence / unstructured fallback). History/reconnect for a NEW
analysis reads this column directly rather than recomputing anything.
"""
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.tasks import analysis as analysis_task

_FAKE_GEMINI_RESULT = GeminiInvestigationResponse(
    title="t", summary="s", probable_root_causes=[], what_happened=[],
    source_code_findings=[], recommended_actions=[], uncertainties=[],
)


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def _new_analysis(db, *, status="processing") -> Analysis:
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status=status
    )
    db.add(analysis)
    db.commit()
    return analysis


def _artifact(db, analysis, **kwargs) -> AnalysisArtifact:
    defaults = dict(
        position=0, original_filename="a.log", saved_file_path="a.log",
        size_bytes=10, status="completed", last_processed_line=1, processed_bytes=10,
    )
    defaults.update(kwargs)
    artifact = AnalysisArtifact(analysis_id=analysis.id, **defaults)
    db.add(artifact)
    db.commit()
    return artifact


# --- result_snapshot is populated identically to the published payload ----


def test_simple_finalize_persists_the_exact_published_payload(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    analysis = _new_analysis(db)
    artifact = _artifact(db, analysis)
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1, severity="ERROR",
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    published = []
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    stored = db.query(Analysis).filter_by(id=analysis_id).first()
    assert stored.status == "completed"
    assert stored.result_snapshot == published[0]
    assert stored.result_snapshot["investigation_path"] == "simple"


def test_correlated_finalize_persists_the_exact_published_payload(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    analysis = _new_analysis(db)
    artifact = _artifact(db, analysis)
    base = datetime.now(timezone.utc)
    db.add_all(
        [
            Evidence(
                analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
                fingerprint="fp-1", trace_id="trace-1", service="api", source_format="generic",
                first_line_number=1, last_line_number=1, severity="ERROR", first_seen=base,
            ),
            Evidence(
                analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-2",
                fingerprint="fp-2", trace_id="trace-1", service="db", source_format="generic",
                first_line_number=2, last_line_number=2, severity="ERROR", first_seen=base,
            ),
        ]
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    published = []
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    stored = db.query(Analysis).filter_by(id=analysis_id).first()
    assert stored.status == "completed"
    assert stored.result_snapshot == published[0]
    assert stored.result_snapshot["investigation_path"] == "correlated"


def test_zero_evidence_finalize_persists_the_exact_published_payload(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    analysis = _new_analysis(db)
    _artifact(db, analysis)
    analysis_id = analysis.id
    db.close()

    published = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    stored = db.query(Analysis).filter_by(id=analysis_id).first()
    assert stored.status == "completed"
    assert stored.result_snapshot == published[0]
    assert stored.result_snapshot["investigation_path"] == "zero_evidence"


def test_unstructured_fallback_finalize_persists_the_exact_published_payload(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    analysis = _new_analysis(db)
    _artifact(db, analysis, fallback_context={"kind": "text", "text": "some free-form notes about the crash"})
    analysis_id = analysis.id
    db.close()

    published = []
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    stored = db.query(Analysis).filter_by(id=analysis_id).first()
    assert stored.status == "completed"
    assert stored.result_snapshot == published[0]
    assert stored.result_snapshot["context_kind"] == "unstructured_fallback"


# --- persist-before-publish ordering ---------------------------------------


def test_result_is_committed_to_the_database_before_it_is_published(monkeypatch):
    """Test scenario 15 (History spec, Section L): at the moment
    publish_investigation_result is called, a fresh read of the row from a
    SEPARATE session must already see status=completed and the snapshot -
    proving the commit genuinely happened first, not merely that the two
    statements appear in this order in the source."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    analysis = _new_analysis(db)
    artifact = _artifact(db, analysis)
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1, severity="ERROR",
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    observed = {}

    def fake_publish(aid, payload):
        fresh = session_factory().query(Analysis).filter_by(id=aid).first()
        observed["status"] = fresh.status
        observed["result_snapshot"] = fresh.result_snapshot

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", fake_publish)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert observed["status"] == "completed"
    assert observed["result_snapshot"] is not None
    assert observed["result_snapshot"]["investigation_path"] == "simple"


# --- reconstruct_current_investigation_result: snapshot vs legacy fallback -


def test_reconstruct_returns_the_snapshot_directly_with_no_recomputation(monkeypatch):
    """Test scenarios 8-9 (History spec, Section L): when a result_snapshot
    is present, no correlation is rerun and no Gemini call is made. Proven
    here by making both raise if touched at all - the function must never
    reach them because it returns the snapshot before querying anything."""

    def _must_not_run_correlation(**kwargs):
        raise AssertionError("must not rerun correlation when a snapshot exists")

    def _must_not_call_gemini(context):
        raise AssertionError("must not call Gemini when a snapshot exists")

    monkeypatch.setattr(analysis_task, "run_correlation", _must_not_run_correlation)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _must_not_call_gemini)

    snapshot = {"investigation_path": "correlated", "components": [], "ai_analysis": {"title": "stored"}}
    result = analysis_task.reconstruct_current_investigation_result(
        db=None,  # would blow up immediately if the function tried to query it
        analysis_id=999,
        ai_analysis={"title": "ignored - snapshot already has its own"},
        result_snapshot=snapshot,
    )

    assert result is snapshot


def test_legacy_completed_analysis_without_snapshot_uses_the_reconstruction_fallback(monkeypatch):
    """Test scenario 16: an analysis finalized before result_snapshot
    existed (column is NULL) must still work via the pre-existing
    recompute-from-Evidence compatibility path."""
    session_factory = _db_with_schema(monkeypatch)
    db = session_factory()
    analysis = _new_analysis(db, status="completed")
    artifact = _artifact(db, analysis)
    db.add(
        Evidence(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1, severity="ERROR",
        )
    )
    db.commit()
    # Simulates a pre-migration row: status=completed but result_snapshot
    # was never populated (finalize predates the column).
    assert analysis.result_snapshot is None

    result = analysis_task.reconstruct_current_investigation_result(
        db, analysis.id, ai_analysis=None, result_snapshot=analysis.result_snapshot
    )
    db.close()

    assert result["investigation_path"] == "simple"
    assert result["evidence"][0]["service"] == "worker"
