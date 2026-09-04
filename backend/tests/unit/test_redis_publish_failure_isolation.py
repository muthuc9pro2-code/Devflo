from datetime import datetime, timezone
from unittest.mock import Mock
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services import analysis_events
from app.tasks import analysis as analysis_task

def test_publish_analysis_event_swallows_redis_connection_error(monkeypatch):
    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("connection refused")),
    )

    analysis_events.publish_analysis_event(1, "progress", {"stage": "ingestion"})

def test_publish_analysis_event_logs_the_analysis_id_and_event(monkeypatch, caplog):
    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("connection refused")),
    )

    with caplog.at_level("WARNING", logger="app.services.analysis_events"):
        analysis_events.publish_analysis_event(42, "investigation_result", {"x": 1})

    assert any(
        "42" in record.getMessage() and "investigation_result" in record.getMessage()
        for record in caplog.records
    )

def test_publish_progress_does_not_raise_on_redis_failure(monkeypatch):
    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("down")),
    )

    analysis_events.publish_progress(1, "ingestion", "in progress", progress=42)

def test_publish_artifact_outcome_does_not_raise_on_redis_failure(monkeypatch):
    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("down")),
    )

    analysis_events.publish_artifact_outcome(1, {"status": "unsupported"})

def test_publish_investigation_result_does_not_raise_on_redis_failure(monkeypatch):
    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("down")),
    )

    analysis_events.publish_investigation_result(1, {"investigation_path": "simple"})

def test_publish_analysis_event_does_not_swallow_non_redis_exceptions(monkeypatch):
    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=TypeError("not a redis problem")),
    )

    with pytest.raises(TypeError):
        analysis_events.publish_analysis_event(1, "progress", {})

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

def test_finalize_survives_a_real_redis_publish_failure_at_the_final_event(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[{"service": "worker"}],
    )

    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("connection refused")),
    )
    from app.services.gemini_service import GeminiUnavailableError

    monkeypatch.setattr(
        analysis_task,
        "generate_investigation_explanation",
        lambda ctx: (_ for _ in ()).throw(GeminiUnavailableError("unavailable")),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    assert analysis.result_snapshot["investigation_path"] == "simple"
    db.close()

def test_finalize_correlated_path_survives_a_real_redis_publish_failure(monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_analysis(
        session_factory,
        evidence_rows_kwargs=[
            {"trace_id": "trace-1", "service": "db"},
            {"trace_id": "trace-1", "service": "api"},
        ],
    )

    monkeypatch.setattr(
        analysis_events.redis_client,
        "publish",
        Mock(side_effect=RedisConnectionError("connection refused")),
    )
    from app.services.gemini_service import GeminiUnavailableError

    monkeypatch.setattr(
        analysis_task,
        "generate_investigation_explanation",
        lambda ctx: (_ for _ in ()).throw(GeminiUnavailableError("unavailable")),
    )

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    assert analysis.result_snapshot["investigation_path"] == "correlated"
    db.close()
