"""Raw diagnostic artifact file lifecycle (Change 5):

- unsupported/duplicate files are already reclaimed at upload time
  (existing behavior, untouched here).
- a "completed" artifact's raw staged bytes are reclaimed only AFTER the
  whole analysis's final durable result (result_snapshot + status=
  "completed") is committed - never before, since parsing/fallback/OCR/
  source-correlation for THAT artifact already finished earlier, but a
  sibling artifact could still be mid-processing.
- a resource_limited/processing_error artifact's raw staged bytes are
  reclaimed immediately once ITS OWN terminal status/failure_reason is
  durably persisted - it is unconditionally terminal (see
  _process_artifact_task's completed/resource_limited/processing_error
  resume-skip guard), so no later analysis-wide event needs to wait for it.
- cleanup is strictly scoped to the configured upload root and is best-
  effort: a failure is logged and never turns a completed investigation
  into a failed one.
- reconnect/History reconstruction after physical files are gone still
  works, since it is driven entirely by persisted Evidence/result_snapshot.
"""
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.artifact_detector import ArtifactFormat
from app.services.gemini_service import GeminiUnavailableError
from app.tasks import analysis as analysis_task


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def _use_sqlite_compatible_evidence_persistence(monkeypatch):
    """persist_evidence_batch's real implementation uses a MySQL-only
    insert(...).on_duplicate_key_update(...) statement that cannot compile
    against sqlite - swapped for a plain per-event insert, sufficient for
    these tests to get real, queryable Evidence rows."""
    counter = {"n": 0}

    def fake_persist(*, db, analysis_id, events, artifact_id=None):
        for event in events:
            if event is None:
                continue
            counter["n"] += 1
            resolved_artifact_id = (
                artifact_id if artifact_id is not None else getattr(event, "artifact_id", None)
            )
            db.add(
                Evidence(
                    analysis_id=analysis_id,
                    artifact_id=resolved_artifact_id,
                    correlation_key=f"ck-{counter['n']}",
                    fingerprint=getattr(event, "fingerprint", None) or f"fp-{counter['n']}",
                    service=getattr(event, "service", None),
                    source_format=getattr(event, "source_format", None),
                    first_line_number=getattr(event, "line_number", None) or 1,
                    last_line_number=getattr(event, "line_number", None) or 1,
                    severity=getattr(event, "level", None),
                    representative_line=getattr(event, "raw_line", None),
                )
            )
        db.commit()

    monkeypatch.setattr(analysis_task, "persist_evidence_batch", fake_persist)


def _quiet_sse(monkeypatch):
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_artifact_outcome", lambda *a, **k: None)


def _raise_gemini_unavailable(_context):
    raise GeminiUnavailableError("temporarily unavailable")


def _seed_user_and_analysis(session_factory) -> int:
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    db.add(analysis)
    db.commit()
    analysis_id = analysis.id
    db.close()
    return analysis_id


def _add_artifact(session_factory, *, analysis_id, position, filename, path, detected_format):
    db = session_factory()
    artifact = AnalysisArtifact(
        analysis_id=analysis_id,
        position=position,
        original_filename=filename,
        saved_file_path=str(path),
        size_bytes=path.stat().st_size,
        detected_format=detected_format,
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
    )
    db.add(artifact)
    db.commit()
    artifact_id = artifact.id
    db.close()
    return artifact_id


def _valid_generic_log(tmp_path, name: str, marker: str):
    path = tmp_path / name
    path.write_text(f"2026-08-12 10:00:00 ERROR service=api ConnectionError: {marker}\n")
    return path


def _oversized_json_array(tmp_path, good_count: int = 1):
    records = [
        {"level": "ERROR", "message": f"connection refused {i}", "service": "orders"}
        for i in range(good_count)
    ]
    records.append({"level": "ERROR", "message": "x" * (1024 * 1024 + 100), "service": "orders"})
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(records))
    return path


# --- completed artifact: cleaned only after the whole analysis finalizes ---


def test_completed_diagnostic_file_survives_until_analysis_finalizes(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "_UPLOAD_ROOT", tmp_path.resolve())
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )

    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)

    # This artifact's own ingestion is done, but the finalizer has not run
    # yet - its physical bytes must still be there (a sibling artifact
    # could still be mid-processing in a real multi-artifact analysis).
    assert valid_path.exists()

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    # The final durable result is committed now - the file is reclaimed.
    assert not valid_path.exists()

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    db.close()


# --- controlled-failure artifact: cleaned immediately, per-artifact ------


def test_resource_limited_diagnostic_file_is_reclaimed_immediately(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "_UPLOAD_ROOT", tmp_path.resolve())

    analysis_id = _seed_user_and_analysis(session_factory)
    bad_path = _oversized_json_array(tmp_path)
    # A second, still-"pending" artifact that has NOT been processed yet -
    # the whole analysis is nowhere near finalized when the first artifact
    # fails, proving resource_limited cleanup does not wait for it.
    other_path = _valid_generic_log(tmp_path, "other.log", "unrelated")

    bad_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="bad.json",
        path=bad_path, detected_format=ArtifactFormat.JSON.value,
    )
    _add_artifact(
        session_factory, analysis_id=analysis_id, position=1, filename="other.log",
        path=other_path, detected_format=ArtifactFormat.GENERIC.value,
    )

    result = analysis_task._process_artifact_task.run(analysis_id, bad_id, 0)

    assert result == 0
    assert not bad_path.exists()  # reclaimed immediately
    assert other_path.exists()  # untouched - it is not this artifact's file

    db = session_factory()
    artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == bad_id).first()
    assert artifact.status == "resource_limited"
    db.close()


# --- safety scoping ---------------------------------------------------


def test_cleanup_refuses_to_delete_outside_the_upload_root(tmp_path, monkeypatch):
    monkeypatch.setattr(analysis_task, "_UPLOAD_ROOT", (tmp_path / "uploads").resolve())
    outside_path = tmp_path / "outside.log"
    outside_path.write_bytes(b"do not delete me")

    analysis_task._cleanup_diagnostic_artifact_file(str(outside_path))

    assert outside_path.exists()


def test_cleanup_is_idempotent_when_file_already_gone(tmp_path, monkeypatch):
    monkeypatch.setattr(analysis_task, "_UPLOAD_ROOT", tmp_path.resolve())
    already_gone = tmp_path / "gone.log"

    analysis_task._cleanup_diagnostic_artifact_file(str(already_gone))  # must not raise


# --- cleanup failure does not fail an otherwise-completed analysis -------


def test_cleanup_failure_does_not_fail_the_completed_analysis(tmp_path, monkeypatch, caplog):
    """A genuine file-deletion failure (e.g. a permission/disk problem) must
    be caught by the cleanup helper itself and never propagate into
    _finalize_analysis_task's outer exception handler - that handler would
    otherwise mark an already-completed, already-persisted analysis as
    "failed" purely because temporary-file cleanup could not run."""
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "_UPLOAD_ROOT", tmp_path.resolve())
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)

    def _boom_unlink(self, missing_ok=False):
        raise OSError("disk unavailable")

    monkeypatch.setattr(analysis_task.Path, "unlink", _boom_unlink)

    with caplog.at_level("WARNING", logger="app.tasks.analysis"):
        analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    db.close()

    assert any(
        "could not remove diagnostic artifact file" in record.getMessage().lower()
        for record in caplog.records
    )


# --- reconnect/History reconstruction after files are gone ---------------


def test_reconnect_reconstruction_works_after_diagnostic_files_are_gone(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "_UPLOAD_ROOT", tmp_path.resolve())
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)
    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert not valid_path.exists()

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    state = analysis_task.compute_current_analysis_state(db, analysis)
    db.close()

    assert state["status"] == "completed"
    assert state["investigation_result"]["investigation_path"] == "simple"
    assert state["investigation_result"]["artifacts"][0]["source_file"] == "valid.log"
