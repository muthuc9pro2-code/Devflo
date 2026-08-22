"""Controlled artifact/source failure isolation.

Expected USER-INPUT / RESOURCE / OPTIONAL-ENRICHMENT failures must degrade
gracefully; internal/infrastructure failures must remain fatal. Covers:

- a JSON artifact whose individual scalar exceeds the supported bound
  (resource_limited), with partial Evidence from that one failed artifact
  cleaned up and every other artifact's Evidence untouched;
- an image that fails RapidOCR after passing validation (processing_error);
- optional source ZIP/GitHub acquisition failing in a controlled way
  (source_status="unavailable") without failing the diagnostic investigation;
- unexpected/internal failures (artifact and source) remaining fatal, never
  silently converted into a controlled/degraded outcome;
- resource_limited/processing_error artifact outcomes surviving the live
  event, a mid-processing reconnect snapshot, and the final result payload;
- Devflo AI (Gemini) unavailability across CORRELATED/SIMPLE/fallback paths
  without ever leaking the provider name into user-facing payloads.
"""
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.artifact_detector import ArtifactFormat
from app.services.gemini_service import GeminiUnavailableError
from app.services.image_text_extractor import OcrProcessingError
from app.services.source_archive import SourceInputError
from app.tasks import analysis as analysis_task
from app.services import source_archive


# --- shared fixtures -------------------------------------------------------


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def _force_single_item_batches(monkeypatch):
    """Every ArtifactEvent becomes its own batch/commit - so a fixture only
    needs a couple of valid records (not thousands) to prove that earlier,
    already-committed batches of a since-failed artifact get cleaned up."""
    from app.services.batch_processor import create_batches as real_create_batches

    monkeypatch.setattr(
        analysis_task,
        "create_batches",
        lambda items: real_create_batches(items, max_batch_bytes=64, max_batch_items=1),
    )


def _use_sqlite_compatible_evidence_persistence(monkeypatch):
    """persist_evidence_batch's real implementation uses a MySQL-only
    insert(...).on_duplicate_key_update(...) statement that cannot compile
    against sqlite (the same limitation test_multifile_processing.py/
    test_artifact_outcome_live_events.py document and route around). This
    swaps in a plain per-event insert equivalent enough for these tests:
    real Evidence rows actually land in the sqlite DB, so a controlled
    artifact failure's cleanup query has real rows to delete."""
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


def _seed_user_and_analysis(session_factory, **analysis_kwargs) -> int:
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    defaults = dict(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    defaults.update(analysis_kwargs)
    analysis = Analysis(**defaults)
    db.add(analysis)
    db.commit()
    analysis_id = analysis.id
    db.close()
    return analysis_id


def _add_artifact(
    session_factory,
    *,
    analysis_id: int,
    position: int,
    filename: str,
    path,
    detected_format: str,
) -> int:
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


def _oversized_json_array(tmp_path, good_count: int = 2):
    """A single top-level JSON array (never JSON-lines) so it routes through
    _stream_json_document's BoundedJsonStream-protected ijson parse. The
    first `good_count` records are real, evidence-worthy ERROR records; the
    final record's message exceeds the supported 1 MiB scalar limit."""
    records = [
        {
            "level": "ERROR",
            "message": f"connection refused attempt {i}",
            "service": "orders",
            "timestamp": "2026-08-12T10:11:16Z",
        }
        for i in range(good_count)
    ]
    records.append(
        {
            "level": "ERROR",
            "message": "x" * (1024 * 1024 + 100),
            "service": "orders",
            "timestamp": "2026-08-12T10:11:20Z",
        }
    )
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(records))
    return path


def _valid_generic_log(tmp_path, name: str, marker: str):
    path = tmp_path / name
    path.write_text(f"2026-08-12 10:00:00 ERROR service=api ConnectionError: {marker}\n")
    return path


# --- TEST 1: mixed artifact resource failure --------------------------------


def test_oversized_json_scalar_is_resource_limited_without_poisoning_the_chord(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _force_single_item_batches(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)

    analysis_id = _seed_user_and_analysis(session_factory)

    valid1_path = _valid_generic_log(tmp_path, "valid1.log", "db refused (1)")
    bad_path = _oversized_json_array(tmp_path, good_count=2)
    valid2_path = _valid_generic_log(tmp_path, "valid2.log", "db refused (2)")

    valid1_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid1.log",
        path=valid1_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    bad_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=1, filename="bad.json",
        path=bad_path, detected_format=ArtifactFormat.JSON.value,
    )
    valid2_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=2, filename="valid2.log",
        path=valid2_path, detected_format=ArtifactFormat.GENERIC.value,
    )

    # None of these raise - a controlled artifact failure must never poison
    # the chord (Celery only invokes the finalizer once every task in the
    # group has "completed successfully", which .run() returning normally
    # here proves).
    analysis_task._process_artifact_task.run(analysis_id, valid1_id)
    result = analysis_task._process_artifact_task.run(analysis_id, bad_id)
    analysis_task._process_artifact_task.run(analysis_id, valid2_id)

    assert result == 0

    db = session_factory()
    bad_artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == bad_id).first()
    assert bad_artifact.status == "resource_limited"
    assert bad_artifact.failure_reason == "JSON value exceeded the supported 1 MiB per-value limit."
    assert bad_artifact.processed_bytes == 0
    assert bad_artifact.last_processed_line == 0
    assert bad_artifact.fallback_context is None

    # The two good records from the failed artifact's own earlier-committed
    # batches must be gone - partial evidence from an incomplete ingestion
    # must never survive or participate in correlation.
    bad_evidence = db.query(Evidence).filter(Evidence.artifact_id == bad_id).all()
    assert bad_evidence == []

    # The other two artifacts' Evidence is completely untouched.
    valid1_evidence = db.query(Evidence).filter(Evidence.artifact_id == valid1_id).all()
    valid2_evidence = db.query(Evidence).filter(Evidence.artifact_id == valid2_id).all()
    assert len(valid1_evidence) == 1
    assert len(valid2_evidence) == 1
    db.close()

    # The finalizer runs (chord callback) and completes using only the
    # valid evidence.
    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    statuses = {a["source_file"]: a["status"] for a in analysis.result_snapshot["artifacts"]}
    assert statuses["bad.json"] == "resource_limited"
    assert statuses["valid1.log"] == "processed"
    assert statuses["valid2.log"] == "processed"
    db.close()


# --- TEST 2: OCR engine failure ---------------------------------------------


def test_ocr_engine_failure_is_processing_error_without_poisoning_the_chord(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)

    def _raise_ocr_processing_error(_path):
        raise OcrProcessingError("Image OCR could not be completed")

    monkeypatch.setattr(
        analysis_task, "extract_text_from_image_with_confidence", _raise_ocr_processing_error
    )

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    image_path = tmp_path / "broken.jpg"
    image_path.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 32)  # content is irrelevant; OCR is mocked

    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    image_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=1, filename="broken.jpg",
        path=image_path, detected_format=ArtifactFormat.IMAGE.value,
    )

    analysis_task._process_artifact_task.run(analysis_id, valid_id)
    result = analysis_task._process_artifact_task.run(analysis_id, image_id)
    assert result == 0

    db = session_factory()
    image_artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == image_id).first()
    assert image_artifact.status == "processing_error"
    assert image_artifact.failure_reason == "Image OCR could not be completed."
    db.close()

    # Finalizer still runs.
    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    db.close()


# --- TEST 3/4: controlled optional-source failure (ZIP and GitHub) ---------


def test_source_zip_controlled_failure_degrades_gracefully(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def _raise_source_input_error(_path, _dest):
        raise SourceInputError("Uploaded file is not a valid ZIP archive")

    monkeypatch.setattr(source_archive, "_extract_zip", _raise_source_input_error)

    analysis_id = _seed_user_and_analysis(
        session_factory, source_kind="zip", source_reference=str(tmp_path / "upload.zip")
    )

    # Controlled source failure must not raise - process_analysis's chain
    # (_prepare_source_task -> chord) must be able to continue.
    analysis_task._prepare_source_task.run(analysis_id)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.source_status == "unavailable"
    assert analysis.source_failure_reason.startswith("Uploaded source ZIP could not be prepared:")
    db.close()

    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    analysis_task._process_artifact_task.run(analysis_id, valid_id)

    db = session_factory()
    evidence = db.query(Evidence).filter(Evidence.artifact_id == valid_id).first()
    assert evidence is not None
    assert evidence.source_matches in (None, [])  # no source index was ever built
    db.close()

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot["source"]["status"] == "unavailable"
    assert "ZIP" in analysis.result_snapshot["source"]["failure_reason"]
    db.close()


def test_source_github_controlled_failure_degrades_gracefully(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def _raise_source_input_error(_url, _dest):
        raise SourceInputError("Could not clone repository: https://github.com/acme/project")

    monkeypatch.setattr(source_archive, "_clone_github", _raise_source_input_error)

    analysis_id = _seed_user_and_analysis(
        session_factory, source_kind="github", source_reference="https://github.com/acme/project"
    )

    analysis_task._prepare_source_task.run(analysis_id)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.source_status == "unavailable"
    assert analysis.source_failure_reason.startswith(
        "Source repository could not be accessed or prepared:"
    )
    db.close()

    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    analysis_task._process_artifact_task.run(analysis_id, valid_id)
    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot["source"]["status"] == "unavailable"
    assert "repository" in analysis.result_snapshot["source"]["failure_reason"]
    db.close()


# --- TEST 5: unexpected/internal artifact failure remains fatal ------------


def test_unexpected_internal_artifact_failure_remains_fatal(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)

    def _boom(**_kwargs):
        raise RuntimeError("programming bug")

    monkeypatch.setattr(analysis_task, "_process_artifact", _boom)

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )

    with pytest.raises(RuntimeError, match="programming bug"):
        analysis_task._process_artifact_task.run(analysis_id, valid_id)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "failed"
    artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == valid_id).first()
    assert artifact.status not in ("resource_limited", "processing_error")
    db.close()


# --- TEST 6: unexpected/internal source failure remains fatal --------------


def test_unexpected_internal_source_failure_remains_fatal(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def _boom(_url, _dest):
        raise RuntimeError("database connection lost")

    monkeypatch.setattr(source_archive, "_clone_github", _boom)

    analysis_id = _seed_user_and_analysis(
        session_factory, source_kind="github", source_reference="https://github.com/acme/project"
    )

    with pytest.raises(RuntimeError, match="database connection lost"):
        analysis_task._prepare_source_task.run(analysis_id)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "failed"
    # Never silently reframed as a controlled optional-source degradation.
    assert analysis.source_status != "unavailable"
    db.close()


# --- TEST 7: artifact outcome reconstruction --------------------------------


def test_resource_limited_outcome_survives_live_reconnect_and_final_result(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _force_single_item_batches(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)

    live_events = []
    monkeypatch.setattr(
        analysis_task, "publish_artifact_outcome", lambda aid, payload: live_events.append(payload)
    )

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    bad_path = _oversized_json_array(tmp_path, good_count=1)

    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    bad_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=1, filename="bad.json",
        path=bad_path, detected_format=ArtifactFormat.JSON.value,
    )

    analysis_task._process_artifact_task.run(analysis_id, bad_id)

    # 1) live artifact_outcome payload
    assert len(live_events) == 1
    live_payload = live_events[0]
    assert live_payload["source_file"] == "bad.json"
    assert live_payload["status"] == "resource_limited"
    assert live_payload["message"] == "JSON value exceeded the supported 1 MiB per-value limit."

    # 2) DB reconstruction/reconnect - the OTHER artifact ("valid.log") is
    # still "pending" (not yet processed), so analysis.status is still
    # "processing" here - exactly the mid-processing reconnect window a
    # client's SSE state snapshot must reconstruct correctly.
    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "processing"
    state = analysis_task.compute_current_analysis_state(db, analysis)
    db.close()

    reconnect_outcomes = {a["source_file"]: a for a in state["artifacts"]}
    assert "bad.json" in reconnect_outcomes
    assert reconnect_outcomes["bad.json"]["status"] == "resource_limited"
    assert reconnect_outcomes["bad.json"]["message"] == (
        "JSON value exceeded the supported 1 MiB per-value limit."
    )

    # 3) final result payload
    analysis_task._process_artifact_task.run(analysis_id, valid_id)
    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    final_outcomes = {a["source_file"]: a for a in analysis.result_snapshot["artifacts"]}
    db.close()

    assert final_outcomes["bad.json"]["status"] == "resource_limited"
    assert final_outcomes["bad.json"]["message"] == (
        "JSON value exceeded the supported 1 MiB per-value limit."
    )


# --- TEST 8: Devflo AI (Gemini) unavailable, across all three paths --------


def _seed_evidence_analysis(session_factory, *, evidence_rows_kwargs: list[dict]) -> int:
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
        analysis_id=analysis.id, position=0, original_filename="artifact-0",
        saved_file_path="artifact-0", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    db.add(artifact)
    db.commit()

    base = datetime.now(timezone.utc)
    for i, kwargs in enumerate(evidence_rows_kwargs):
        defaults = dict(
            analysis_id=analysis.id, artifact_id=artifact.id, correlation_key=f"ck-{i}",
            fingerprint=f"fp-{i}", first_line_number=1, last_line_number=1,
            first_seen=base, severity="ERROR",
        )
        defaults.update(kwargs)
        db.add(Evidence(**defaults))
    db.commit()

    analysis_id = analysis.id
    db.close()
    return analysis_id


@pytest.mark.parametrize(
    "evidence_kwargs",
    [
        pytest.param(
            [{"trace_id": "trace-1", "service": "db"}, {"trace_id": "trace-1", "service": "api"}],
            id="correlated",
        ),
        pytest.param([{"service": "worker"}], id="simple"),
    ],
)
def test_devflo_ai_unavailable_preserves_deterministic_result_without_naming_provider(
    monkeypatch, evidence_kwargs
):
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_evidence_analysis(session_factory, evidence_rows_kwargs=evidence_kwargs)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert "ai_analysis" not in analysis.result_snapshot
    # Deterministic content survives regardless of AI availability.
    if analysis.result_snapshot["investigation_path"] == "correlated":
        assert len(analysis.result_snapshot["components"]) == 1
    db.close()

    serialized = json.dumps(analysis.result_snapshot)
    assert "gemini" not in serialized.lower()
    assert "Gemini" not in serialized


def test_devflo_ai_unavailable_fallback_path_preserves_deterministic_result(monkeypatch):
    """The third Gemini call site: zero-structured-evidence with a usable
    unstructured fallback context (captured OCR/text)."""
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
    db.add(
        AnalysisArtifact(
            analysis_id=analysis.id, position=0, original_filename="notes.txt",
            saved_file_path="notes.txt", size_bytes=10, status="completed",
            last_processed_line=1, processed_bytes=10,
            fallback_context={"kind": "text", "text": "payment worker stops after restart"},
        )
    )
    db.commit()
    analysis_id = analysis.id
    db.close()

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert analysis.result_snapshot["context_kind"] == "unstructured_fallback"
    assert "ai_analysis" not in analysis.result_snapshot
    db.close()

    serialized = json.dumps(analysis.result_snapshot)
    assert "gemini" not in serialized.lower()
