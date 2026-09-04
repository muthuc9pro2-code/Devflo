import json
from datetime import datetime, timezone
from io import BytesIO
import pytest
from PIL import Image
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.artifact_detector import ArtifactFormat
from app.services.gemini_service import GeminiUnavailableError
from app.services.image_text_extractor import OcrProcessingError
from app.services.source_archive import SourceInputError
from app.services.source_index import SourceIndexLimitError
from app.services.diagnostic_parser import parse_timestamp
from app.services import gemini_service, image_text_extractor
from app.tasks import analysis as analysis_task
from app.services import source_archive

def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory

def _force_single_item_batches(monkeypatch):
    from app.services.batch_processor import create_batches as real_create_batches

    monkeypatch.setattr(
        analysis_task,
        "create_batches",
        lambda items: real_create_batches(items, max_batch_bytes=64, max_batch_items=1),
    )

def _use_sqlite_compatible_evidence_persistence(monkeypatch):
    counter = {"n": 0}

    def fake_persist(*, db, analysis_id, events, artifact_id=None):
        for event in events:
            if event is None:
                continue
            counter["n"] += 1
            resolved_artifact_id = (
                artifact_id if artifact_id is not None else getattr(event, "artifact_id", None)
            )
            trace_id = getattr(event, "trace_id", None)
            request_id = getattr(event, "request_id", None)
            if trace_id:
                resolved_identity = f"trace:{trace_id}"
                identity_match_type = "trace_id"
                identity_strength = 1.0
            elif request_id:
                resolved_identity = f"request:{request_id}"
                identity_match_type = "request_id"
                identity_strength = 0.9
            else:
                resolved_identity = f"unresolved:test-{counter['n']}"
                identity_match_type = "unresolved"
                identity_strength = 0.0
            timestamp = parse_timestamp(getattr(event, "timestamp", None))
            db.add(
                Evidence(
                    analysis_id=analysis_id,
                    artifact_id=resolved_artifact_id,
                    correlation_key=f"ck-{counter['n']}",
                    fingerprint=getattr(event, "fingerprint", None) or f"fp-{counter['n']}",
                    service=getattr(event, "service", None),
                    trace_id=trace_id,
                    request_id=request_id,
                    span_id=getattr(event, "span_id", None),
                    parent_span_id=getattr(event, "parent_span_id", None),
                    source_file=getattr(event, "source_file", None),
                    source_format=getattr(event, "source_format", None),
                    source_matches=getattr(event, "source_matches", None),
                    first_seen=timestamp,
                    last_seen=timestamp,
                    resolved_identity=resolved_identity,
                    identity_match_type=identity_match_type,
                    identity_strength=identity_strength,
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

def _valid_png(path):
    buffer = BytesIO()
    Image.new("RGB", (4, 4), "white").save(buffer, format="PNG")
    path.write_bytes(buffer.getvalue())
    return path

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

    analysis_task._process_artifact_task.run(analysis_id, valid1_id, 0)
    result = analysis_task._process_artifact_task.run(analysis_id, bad_id, 0)
    analysis_task._process_artifact_task.run(analysis_id, valid2_id, 0)

    assert result == 0

    db = session_factory()
    bad_artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == bad_id).first()
    assert bad_artifact.status == "resource_limited"
    assert bad_artifact.failure_reason == "JSON value exceeded the supported 1 MiB per-value limit."
    assert bad_artifact.processed_bytes == 0
    assert bad_artifact.last_processed_line == 0
    assert bad_artifact.fallback_context is None

    bad_evidence = db.query(Evidence).filter(Evidence.artifact_id == bad_id).all()
    assert bad_evidence == []

    valid1_evidence = db.query(Evidence).filter(Evidence.artifact_id == valid1_id).all()
    valid2_evidence = db.query(Evidence).filter(Evidence.artifact_id == valid2_id).all()
    assert len(valid1_evidence) == 1
    assert len(valid2_evidence) == 1
    db.close()

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    statuses = {a["source_file"]: a["status"] for a in analysis.result_snapshot["artifacts"]}
    assert statuses["bad.json"] == "resource_limited"
    assert statuses["valid1.log"] == "processed"
    assert statuses["valid2.log"] == "processed"
    db.close()

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
    image_path.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 32)

    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    image_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=1, filename="broken.jpg",
        path=image_path, detected_format=ArtifactFormat.IMAGE.value,
    )

    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)
    result = analysis_task._process_artifact_task.run(analysis_id, image_id, 0)
    assert result == 0

    db = session_factory()
    image_artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == image_id).first()
    assert image_artifact.status == "processing_error"
    assert image_artifact.failure_reason == "Image OCR could not be completed."
    db.close()

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    db.close()

def test_corrupt_source_zip_degrades_gracefully_and_diagnostics_continue(
    tmp_path,
    monkeypatch,
):
    session_factory = _db_with_schema(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    archive = tmp_path / "upload.zip"
    corrupt_payload = b"unique-payload-that-will-have-a-bad-crc"
    with source_archive.zipfile.ZipFile(archive, "w", source_archive.zipfile.ZIP_STORED) as zf:
        zf.writestr("first.py", b"print('valid file extracted first')\n")
        zf.writestr("second.py", corrupt_payload)
    archive_bytes = bytearray(archive.read_bytes())
    payload_offset = archive_bytes.index(corrupt_payload)
    archive_bytes[payload_offset] ^= 0xFF
    archive.write_bytes(archive_bytes)

    source_archive.validate_source_zip(archive)

    analysis_id = _seed_user_and_analysis(
        session_factory, source_kind="zip", source_reference=str(archive)
    )

    analysis_task._prepare_source_task.run(analysis_id, 0)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.source_status == "unavailable"
    assert analysis.source_failure_reason.startswith("Uploaded source ZIP could not be prepared:")
    assert not (tmp_path / "sources" / str(analysis_id)).exists()
    db.close()

    valid_path = _valid_generic_log(tmp_path, "valid.log", "db refused")
    valid_id = _add_artifact(
        session_factory, analysis_id=analysis_id, position=0, filename="valid.log",
        path=valid_path, detected_format=ArtifactFormat.GENERIC.value,
    )
    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)

    db = session_factory()
    evidence = db.query(Evidence).filter(Evidence.artifact_id == valid_id).first()
    assert evidence is not None
    assert evidence.source_matches in (None, [])
    db.close()

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

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

    analysis_task._prepare_source_task.run(analysis_id, 0)

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
    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)
    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot["source"]["status"] == "unavailable"
    assert "repository" in analysis.result_snapshot["source"]["failure_reason"]
    db.close()

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
        analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "failed"
    artifact = db.query(AnalysisArtifact).filter(AnalysisArtifact.id == valid_id).first()
    assert artifact.status not in ("resource_limited", "processing_error")
    db.close()

def test_unexpected_source_acquisition_failure_degrades(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def _boom(_url, _dest):
        raise RuntimeError("source SDK crashed")

    monkeypatch.setattr(source_archive, "_clone_github", _boom)

    analysis_id = _seed_user_and_analysis(
        session_factory, source_kind="github", source_reference="https://github.com/acme/project"
    )

    analysis_task._prepare_source_task.run(analysis_id, 0)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "processing"
    assert analysis.source_status == "unavailable"
    assert "Optional source processing failed" in analysis.source_failure_reason
    db.close()

def test_source_index_construction_failure_degrades_and_diagnostics_continue(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def fake_clone(_url, destination):
        destination.mkdir(parents=True)
        (destination / "app.py").write_text("raise RuntimeError('boom')\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)
    monkeypatch.setattr(
        source_archive,
        "build_index",
        lambda _root: (_ for _ in ()).throw(RuntimeError("index builder crashed")),
    )

    analysis_id = _seed_user_and_analysis(
        session_factory,
        source_kind="github",
        source_reference="https://github.com/acme/project",
    )

    analysis_task._prepare_source_task.run(analysis_id, 0)

    valid_path = _valid_generic_log(tmp_path, "valid.log", "diagnostic survived")
    valid_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename="valid.log",
        path=valid_path,
        detected_format=ArtifactFormat.GENERIC.value,
    )
    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "processing"
    assert analysis.source_status == "unavailable"
    assert db.query(Evidence).filter(Evidence.artifact_id == valid_id).count() == 1
    db.close()

def test_source_matching_failure_retains_evidence_without_source_matches(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)

    analysis_id = _seed_user_and_analysis(
        session_factory,
        source_kind="github",
        source_reference="https://github.com/acme/project",
    )
    valid_path = _valid_generic_log(tmp_path, "valid.log", "matcher survived")
    valid_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename="valid.log",
        path=valid_path,
        detected_format=ArtifactFormat.GENERIC.value,
    )

    monkeypatch.setattr(
        analysis_task, "_load_ready_source_index_for_artifact",
        lambda _analysis, _generation: object()
    )
    monkeypatch.setattr(
        analysis_task,
        "correlate_event",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("matcher crashed")),
    )

    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)

    db = session_factory()
    evidence = db.query(Evidence).filter(Evidence.artifact_id == valid_id).one()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).one()
    assert evidence.source_matches in (None, [])
    assert analysis.status == "processing"
    assert analysis.source_status == "unavailable"
    assert "retained without source enrichment" in analysis.source_failure_reason
    db.close()

def test_malformed_otlp_before_first_record_fails_only_that_artifact(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(
        analysis_task,
        "generate_investigation_explanation",
        _raise_gemini_unavailable,
    )

    analysis_id = _seed_user_and_analysis(session_factory)
    valid_path = _valid_generic_log(tmp_path, "valid.log", "OTLP sibling survived")
    malformed_path = tmp_path / "bad-otlp.json"
    malformed_path.write_text('{"resourceLogs":[{"scopeLogs":[', encoding="utf-8")
    valid_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename="valid.log",
        path=valid_path,
        detected_format=ArtifactFormat.GENERIC.value,
    )
    malformed_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=1,
        filename="bad-otlp.json",
        path=malformed_path,
        detected_format=ArtifactFormat.OPENTELEMETRY.value,
    )

    valid_result = analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)
    malformed_result = analysis_task._process_artifact_task.run(
        analysis_id, malformed_id, 0
    )
    assert malformed_result == 0

    db = session_factory()
    malformed = db.query(AnalysisArtifact).filter_by(id=malformed_id).one()
    analysis = db.query(Analysis).filter_by(id=analysis_id).one()
    assert malformed.status == "processing_error"
    assert db.query(Evidence).filter_by(artifact_id=malformed_id).count() == 0
    assert db.query(Evidence).filter_by(artifact_id=valid_id).count() == 1
    assert analysis.status == "processing"
    db.close()

    analysis_task._finalize_analysis_task.run(
        [valid_result, malformed_result], analysis_id, 0, None
    )
    db = session_factory()
    assert db.query(Analysis).filter_by(id=analysis_id).one().status == "completed"
    db.close()

def test_unsafe_structured_resume_cleans_checkpoint_evidence_but_keeps_sibling(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(
        analysis_task,
        "generate_investigation_explanation",
        _raise_gemini_unavailable,
    )

    analysis_id = _seed_user_and_analysis(session_factory)
    bad_path = tmp_path / "resume.json"
    bad_path.write_text(
        '[{"level":"ERROR","message":"already committed"},{"level":',
        encoding="utf-8",
    )
    sibling_path = _valid_generic_log(tmp_path, "sibling.log", "resume sibling survived")
    bad_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename="resume.json",
        path=bad_path,
        detected_format=ArtifactFormat.JSON.value,
    )
    sibling_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=1,
        filename="sibling.log",
        path=sibling_path,
        detected_format=ArtifactFormat.GENERIC.value,
    )

    db = session_factory()
    bad = db.query(AnalysisArtifact).filter_by(id=bad_id).one()
    bad.status = "pending"
    bad.last_processed_line = 1
    bad.processed_bytes = 0
    bad.fallback_context = {"text": "unsafe stale fallback"}
    db.add(
        Evidence(
            analysis_id=analysis_id,
            artifact_id=bad_id,
            correlation_key="prior-checkpoint",
            fingerprint="prior-checkpoint",
            first_line_number=1,
            last_line_number=1,
            severity="ERROR",
            representative_line="already committed",
        )
    )
    db.commit()
    db.close()

    bad_result = analysis_task._process_artifact_task.run(analysis_id, bad_id, 0)
    sibling_result = analysis_task._process_artifact_task.run(analysis_id, sibling_id, 0)

    db = session_factory()
    bad = db.query(AnalysisArtifact).filter_by(id=bad_id).one()
    assert bad.status == "processing_error"
    assert bad.processed_bytes == 0
    assert bad.last_processed_line == 0
    assert bad.fallback_context is None
    assert db.query(Evidence).filter_by(artifact_id=bad_id).count() == 0
    assert db.query(Evidence).filter_by(artifact_id=sibling_id).count() == 1
    db.close()

    analysis_task._finalize_analysis_task.run(
        [bad_result, sibling_result], analysis_id, 0, None
    )
    db = session_factory()
    assert db.query(Analysis).filter_by(id=analysis_id).one().status == "completed"
    db.close()

def test_evidence_persistence_failure_remains_analysis_fatal(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    analysis_id = _seed_user_and_analysis(session_factory)
    path = _valid_generic_log(tmp_path, "valid.log", "core persistence")
    artifact_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename="valid.log",
        path=path,
        detected_format=ArtifactFormat.GENERIC.value,
    )
    monkeypatch.setattr(
        analysis_task,
        "persist_evidence_batch",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("evidence DB write failed")),
    )

    with pytest.raises(RuntimeError, match="evidence DB write failed"):
        analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)

    db = session_factory()
    assert db.query(Analysis).filter_by(id=analysis_id).one().status == "failed"
    artifact = db.query(AnalysisArtifact).filter_by(id=artifact_id).one()
    assert artifact.status not in ("resource_limited", "processing_error")
    db.close()

def test_deterministic_correlation_failure_remains_analysis_fatal(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    analysis_id = _seed_user_and_analysis(session_factory)
    generic_path = tmp_path / "generic.log"
    generic_path.write_text(
        "2026-08-12T10:00:00Z ERROR trace_id=fatal-trace service=api RuntimeError: failed\n"
    )
    json_path = tmp_path / "events.jsonl"
    json_path.write_text(
        json.dumps(
            {
                "timestamp": "2026-08-12T10:00:01Z",
                "level": "ERROR",
                "message": "database timed out",
                "trace_id": "fatal-trace",
                "service": "database",
            }
        )
        + "\n"
    )
    artifact_ids = [
        _add_artifact(
            session_factory,
            analysis_id=analysis_id,
            position=position,
            filename=path.name,
            path=path,
            detected_format=artifact_format.value,
        )
        for position, (path, artifact_format) in enumerate(
            ((generic_path, ArtifactFormat.GENERIC), (json_path, ArtifactFormat.JSON))
        )
    ]
    results = [
        analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)
        for artifact_id in artifact_ids
    ]
    monkeypatch.setattr(
        analysis_task,
        "run_correlation",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("correlation engine bug")),
    )

    with pytest.raises(RuntimeError, match="correlation engine bug"):
        analysis_task._finalize_analysis_task.run(results, analysis_id, 0, None)

    db = session_factory()
    assert db.query(Analysis).filter_by(id=analysis_id).one().status == "failed"
    db.close()

def test_mixed_investigation_isolates_artifact_source_ocr_and_gemini_failures(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    monkeypatch.setattr(
        source_archive,
        "_clone_github",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("optional source SDK failed")),
    )

    analysis_id = _seed_user_and_analysis(
        session_factory,
        source_kind="github",
        source_reference="https://github.com/acme/project",
    )
    analysis_task._prepare_source_task.run(analysis_id, 0)

    generic_path = tmp_path / "generic.log"
    generic_path.write_text(
        "2026-08-12T10:00:00Z ERROR trace_id=mixed-trace service=api RuntimeError: request failed\n"
    )
    json_path = tmp_path / "events.jsonl"
    json_path.write_text(
        json.dumps(
            {
                "timestamp": "2026-08-12T10:00:01Z",
                "level": "ERROR",
                "message": "database timed out",
                "trace_id": "mixed-trace",
                "service": "database",
            }
        )
        + "\n"
    )
    web_path = tmp_path / "nginx.log"
    web_path.write_text(
        '127.0.0.1 - - [12/Aug/2026:10:00:02 +0000] "GET /checkout HTTP/1.1" 503 123\n'
    )
    otlp_path = tmp_path / "broken-otlp.json"
    otlp_path.write_text('{"resourceLogs":[{"scopeLogs":[', encoding="utf-8")
    image_path = _valid_png(tmp_path / "screenshot.png")

    artifacts = [
        (generic_path, ArtifactFormat.GENERIC),
        (json_path, ArtifactFormat.JSON),
        (web_path, ArtifactFormat.WEB_SERVER),
        (otlp_path, ArtifactFormat.OPENTELEMETRY),
        (image_path, ArtifactFormat.IMAGE),
    ]
    artifact_ids = [
        _add_artifact(
            session_factory,
            analysis_id=analysis_id,
            position=position,
            filename=path.name,
            path=path,
            detected_format=artifact_format.value,
        )
        for position, (path, artifact_format) in enumerate(artifacts)
    ]

    monkeypatch.setattr(image_text_extractor, "_ocr", None)
    monkeypatch.setattr(
        "rapidocr_onnxruntime.RapidOCR",
        lambda: (_ for _ in ()).throw(RuntimeError("OCR model failed to load")),
    )

    results = [
        analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)
        for artifact_id in artifact_ids
    ]

    identity_calls = []
    correlation_calls = []
    real_identity = analysis_task.persist_resolved_identities
    real_run_correlation = analysis_task.run_correlation

    def tracked_identity(**kwargs):
        identity_calls.append(kwargs["analysis_id"])
        return real_identity(**kwargs)

    def tracked_correlation(**kwargs):
        correlation_calls.append(kwargs["analysis_id"])
        return real_run_correlation(**kwargs)

    monkeypatch.setattr(analysis_task, "persist_resolved_identities", tracked_identity)
    monkeypatch.setattr(analysis_task, "run_correlation", tracked_correlation)
    monkeypatch.setattr(gemini_service._client, "_resolved_client", None)
    monkeypatch.setattr(
        gemini_service.genai,
        "Client",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("Gemini SDK failed")),
    )

    analysis_task._finalize_analysis_task.run(results, analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter_by(id=analysis_id).one()
    rows = db.query(AnalysisArtifact).filter_by(analysis_id=analysis_id).all()
    status_by_name = {row.original_filename: row.status for row in rows}
    evidence = db.query(Evidence).filter_by(analysis_id=analysis_id).all()

    assert status_by_name["broken-otlp.json"] == "processing_error"
    assert status_by_name["screenshot.png"] == "processing_error"
    assert status_by_name["generic.log"] == "completed"
    assert status_by_name["events.jsonl"] == "completed"
    assert status_by_name["nginx.log"] == "completed"
    assert {row.source_format for row in evidence} >= {"generic", "json", "web_server"}
    assert all(
        row.artifact_id not in {artifact_ids[3], artifact_ids[4]} for row in evidence
    )
    assert analysis.source_status == "unavailable"
    assert analysis.status == "completed"
    assert analysis.result_snapshot["investigation_path"] == "correlated"
    assert analysis.result_snapshot.get("ai_analysis") is None
    assert identity_calls == [analysis_id]
    assert correlation_calls == [analysis_id]
    db.close()

def test_gemini_response_object_failure_preserves_deterministic_result(
    tmp_path, monkeypatch
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    analysis_id = _seed_user_and_analysis(session_factory)
    path = _valid_generic_log(tmp_path, "valid.log", "Gemini response failed")
    artifact_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename="valid.log",
        path=path,
        detected_format=ArtifactFormat.GENERIC.value,
    )
    result = analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)

    class ThrowingResponse:
        @property
        def text(self):
            raise RuntimeError("response decoding failed")

    class Models:
        def generate_content(self, **_kwargs):
            return ThrowingResponse()

    class Client:
        models = Models()

    monkeypatch.setattr(gemini_service._client, "_resolved_client", Client())

    analysis_task._finalize_analysis_task.run([result], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter_by(id=analysis_id).one()
    assert analysis.status == "completed"
    assert analysis.result_snapshot["investigation_path"] == "simple"
    assert analysis.result_snapshot.get("ai_analysis") is None
    db.close()

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

    analysis_task._process_artifact_task.run(analysis_id, bad_id, 0)

    assert len(live_events) == 1
    live_payload = live_events[0]
    assert live_payload["source_file"] == "bad.json"
    assert live_payload["status"] == "resource_limited"
    assert live_payload["message"] == "JSON value exceeded the supported 1 MiB per-value limit."

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

    analysis_task._process_artifact_task.run(analysis_id, valid_id, 0)
    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    final_outcomes = {a["source_file"]: a for a in analysis.result_snapshot["artifacts"]}
    db.close()

    assert final_outcomes["bad.json"]["status"] == "resource_limited"
    assert final_outcomes["bad.json"]["message"] == (
        "JSON value exceeded the supported 1 MiB per-value limit."
    )

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

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert "ai_analysis" not in analysis.result_snapshot
    if analysis.result_snapshot["investigation_path"] == "correlated":
        assert len(analysis.result_snapshot["components"]) == 1
    db.close()

    serialized = json.dumps(analysis.result_snapshot)
    assert "gemini" not in serialized.lower()
    assert "Gemini" not in serialized

def test_devflo_ai_unavailable_fallback_path_preserves_deterministic_result(monkeypatch):
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

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.ai_analysis is None
    assert analysis.result_snapshot["context_kind"] == "unstructured_fallback"
    assert "ai_analysis" not in analysis.result_snapshot
    db.close()

    serialized = json.dumps(analysis.result_snapshot)
    assert "gemini" not in serialized.lower()

@pytest.mark.parametrize(
    "source_kind",
    [
        "github",
        "zip",
    ],
)
@pytest.mark.parametrize(
    "failure_stage",
    [
        "acquisition",
        "index",
        "index_limit",
        "manifest",
    ],
)
def test_optional_source_preparation_failure_matrix_never_poison_diagnostics(
    tmp_path,
    monkeypatch,
    source_kind,
    failure_stage,
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(
        analysis_task,
        "_source_index_process_cache",
        {},
    )
    monkeypatch.setattr(
        analysis_task,
        "generate_investigation_explanation",
        _raise_gemini_unavailable,
    )
    monkeypatch.setattr(
        source_archive,
        "SOURCE_STORAGE_ROOT",
        str(tmp_path / "sources"),
    )

    if source_kind == "github":
        source_reference = "https://github.com/acme/project"

        def successful_acquisition(_reference, destination):
            destination.mkdir(parents=True, exist_ok=True)
            (destination / "app.py").write_text(
                "raise RuntimeError('source fixture')\n",
                encoding="utf-8",
            )

        acquisition_name = "_clone_github"
    else:
        archive = tmp_path / "source.zip"
        archive.write_bytes(b"placeholder: extraction is monkeypatched")
        source_reference = str(archive)

        def successful_acquisition(_reference, destination):
            destination.mkdir(parents=True, exist_ok=True)
            (destination / "app.py").write_text(
                "raise RuntimeError('source fixture')\n",
                encoding="utf-8",
            )

        acquisition_name = "_extract_zip"

    if failure_stage == "acquisition":
        def failing_acquisition(_reference, _destination):
            raise SourceInputError("controlled acquisition failure")

        monkeypatch.setattr(source_archive, acquisition_name, failing_acquisition)
    else:
        monkeypatch.setattr(source_archive, acquisition_name, successful_acquisition)

    if failure_stage == "index":
        monkeypatch.setattr(
            source_archive,
            "build_index",
            lambda _root: (_ for _ in ()).throw(
                RuntimeError("index construction failed")
            ),
        )
    elif failure_stage == "index_limit":
        monkeypatch.setattr(
            source_archive,
            "build_index",
            lambda _root: (_ for _ in ()).throw(
                SourceIndexLimitError(
                    "Source path depth exceeds the supported index limit"
                )
            ),
        )
    elif failure_stage == "manifest":
        monkeypatch.setattr(
            source_archive,
            "save_index_manifest",
            lambda _index, _path: (_ for _ in ()).throw(
                OSError("manifest persistence failed")
            ),
        )

    analysis_id = _seed_user_and_analysis(
        session_factory,
        source_kind=source_kind,
        source_reference=source_reference,
    )

    analysis_task._prepare_source_task.run(analysis_id, 0)

    db = session_factory()
    after_source = db.query(Analysis).filter_by(id=analysis_id).one()
    assert after_source.status == "processing"
    assert after_source.source_status == "unavailable"
    assert after_source.source_failure_reason
    if failure_stage == "index_limit":
        assert "path depth" in after_source.source_failure_reason
    db.close()

    diagnostic_path = _valid_generic_log(
        tmp_path,
        f"diagnostic-{source_kind}-{failure_stage}.log",
        f"diagnostic survived {source_kind} {failure_stage}",
    )
    artifact_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename=diagnostic_path.name,
        path=diagnostic_path,
        detected_format=ArtifactFormat.GENERIC.value,
    )
    parsed = analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)

    db = session_factory()
    assert db.query(Evidence).filter_by(artifact_id=artifact_id).count() == 1
    assert db.query(AnalysisArtifact).filter_by(id=artifact_id).one().status == "completed"
    db.close()

    analysis_task._finalize_analysis_task.run([parsed], analysis_id, 0, None)

    db = session_factory()
    completed = db.query(Analysis).filter_by(id=analysis_id).one()
    assert completed.status == "completed"
    assert completed.result_snapshot["source"]["status"] == "unavailable"
    assert completed.result_snapshot["source"]["failure_reason"]
    db.close()

@pytest.mark.parametrize(
    "source_kind",
    [
        "github",
        "zip",
    ],
)
@pytest.mark.parametrize(
    "failure_stage",
    [
        "missing_ready_tree",
        "ready_index_loader",
        "matcher",
    ],
)
def test_optional_source_post_publication_failure_matrix_retains_evidence_and_completes(
    tmp_path,
    monkeypatch,
    source_kind,
    failure_stage,
):
    session_factory = _db_with_schema(monkeypatch)
    _quiet_sse(monkeypatch)
    _use_sqlite_compatible_evidence_persistence(monkeypatch)
    monkeypatch.setattr(
        analysis_task,
        "_source_index_process_cache",
        {},
    )
    monkeypatch.setattr(
        analysis_task,
        "generate_investigation_explanation",
        _raise_gemini_unavailable,
    )

    source_reference = (
        "https://github.com/acme/project"
        if source_kind == "github"
        else str(tmp_path / "source.zip")
    )
    analysis_id = _seed_user_and_analysis(
        session_factory,
        source_kind=source_kind,
        source_reference=source_reference,
        source_status="ready",
    )
    diagnostic_path = _valid_generic_log(
        tmp_path,
        f"post-publish-{source_kind}-{failure_stage}.log",
        f"diagnostic survived {failure_stage}",
    )
    artifact_id = _add_artifact(
        session_factory,
        analysis_id=analysis_id,
        position=0,
        filename=diagnostic_path.name,
        path=diagnostic_path,
        detected_format=ArtifactFormat.GENERIC.value,
    )

    if failure_stage == "missing_ready_tree":
        monkeypatch.setattr(
            analysis_task,
            "_load_ready_source_index_for_artifact",
            lambda _analysis, _generation: None,
        )
    elif failure_stage == "ready_index_loader":
        monkeypatch.setattr(
            analysis_task,
            "load_ready_source_index",
            lambda _analysis_id: (_ for _ in ()).throw(
                OSError("published source index became unreadable")
            ),
        )
    else:
        monkeypatch.setattr(
            analysis_task,
            "_load_ready_source_index_for_artifact",
            lambda _analysis, _generation: object(),
        )
        monkeypatch.setattr(
            analysis_task,
            "correlate_event",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("source matcher failed")
            ),
        )

    parsed = analysis_task._process_artifact_task.run(analysis_id, artifact_id, 0)

    db = session_factory()
    analysis = db.query(Analysis).filter_by(id=analysis_id).one()
    evidence = db.query(Evidence).filter_by(artifact_id=artifact_id).one()
    assert analysis.status == "processing"
    assert analysis.source_status == "unavailable"
    assert analysis.source_failure_reason
    assert evidence.source_matches in (None, [])
    assert db.query(AnalysisArtifact).filter_by(id=artifact_id).one().status == "completed"
    db.close()

    analysis_task._finalize_analysis_task.run([parsed], analysis_id, 0, None)

    db = session_factory()
    completed = db.query(Analysis).filter_by(id=analysis_id).one()
    assert completed.status == "completed"
    assert completed.result_snapshot["source"]["status"] == "unavailable"
    db.close()
