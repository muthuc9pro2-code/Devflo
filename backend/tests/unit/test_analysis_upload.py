from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.api import analysis as analysis_api
from app.crud.analysis import ActiveAnalysisLimitReached
from app.db.database import Base
from app.models import Analysis, User

def test_multiple_files_are_streamed_into_one_analysis(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=17, artifacts=[]))
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "UPLOAD_COPY_CHUNK_BYTES", 3)
    monkeypatch.setattr(analysis_api, "MAX_INVESTIGATION_UPLOAD_BYTES", 20)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    result = analysis_api.upload_file(
        file=[_upload("first.txt", b"12345"), _upload("second.log", b"6789")],
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 17
    artifact_rows = create_analysis.call_args.kwargs["artifacts"]
    assert [row["size_bytes"] for row in artifact_rows] == [5, 4]
    assert all(
        (tmp_path / row["saved_file_path"].split("/")[-1]).exists()
        for row in artifact_rows
    )
    task.delay.assert_called_once_with(17)

def test_direct_single_file_call_remains_compatible(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=18, artifacts=[]))
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    result = analysis_api.upload_file(
        file=_upload("single.txt", b"one artifact"),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 18
    rows = create_analysis.call_args.kwargs["artifacts"]
    assert [row["original_filename"] for row in rows] == ["single.txt"]
    task.delay.assert_called_once_with(18)

def test_combined_upload_limit_cleans_partial_files(tmp_path, monkeypatch):
    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "UPLOAD_COPY_CHUNK_BYTES", 4)
    monkeypatch.setattr(analysis_api, "MAX_INVESTIGATION_UPLOAD_BYTES", 6)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=[_upload("first.txt", b"1234"), _upload("second.txt", b"5678")],
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 413
    assert list(tmp_path.iterdir()) == []
    create_analysis.assert_not_called()

def test_optional_github_source_is_canonicalized_separately(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=19, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())
    monkeypatch.setattr(
        analysis_api,
        "validate_github_url",
        Mock(return_value="https://github.com/acme/project"),
    )

    analysis_api.upload_file(
        file=_upload("diagnostic.log", b"ERROR failed"),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
        github_url=" https://github.com/acme/project.git ",
    )

    kwargs = create_analysis.call_args.kwargs
    assert kwargs["source_kind"] == "github"
    assert kwargs["source_reference"] == "https://github.com/acme/project"
    assert [row["original_filename"] for row in kwargs["artifacts"]] == [
        "diagnostic.log"
    ]

def test_optional_source_zip_is_staged_but_not_a_diagnostic_artifact(
    tmp_path, monkeypatch
):
    create_analysis = Mock(return_value=SimpleNamespace(id=20, artifacts=[]))
    validate = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())
    monkeypatch.setattr(analysis_api, "validate_source_zip", validate)

    analysis_api.upload_file(
        file=_upload("diagnostic.log", b"ERROR failed"),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
        source_zip=_upload("source.zip", b"zip bytes"),
    )

    kwargs = create_analysis.call_args.kwargs
    assert kwargs["source_kind"] == "zip"
    assert len(kwargs["artifacts"]) == 1
    assert kwargs["source_reference"].endswith("_source.zip")
    validate.assert_called_once()

def test_github_and_source_zip_are_mutually_exclusive(tmp_path, monkeypatch):
    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=_upload("diagnostic.log", b"ERROR failed"),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
            github_url="https://github.com/acme/project",
            source_zip=_upload("source.zip", b"zip bytes"),
        )

    assert error.value.status_code == 400
    assert list(tmp_path.iterdir()) == []
    create_analysis.assert_not_called()

def test_oversized_source_zip_degrades_source_but_still_processes_diagnostics(
    tmp_path, monkeypatch
):
    create_analysis = Mock(return_value=SimpleNamespace(id=24, artifacts=[]))
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "MAX_SOURCE_ARCHIVE_BYTES", 3)
    monkeypatch.setattr(analysis_api, "UPLOAD_COPY_CHUNK_BYTES", 2)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    result = analysis_api.upload_file(
        file=_upload("diagnostic.log", b"ERROR failed"),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
        source_zip=_upload("source.zip", b"1234"),
    )

    assert result.id == 24
    kwargs = create_analysis.call_args.kwargs
    assert kwargs["source_kind"] == "zip"
    assert kwargs["source_reference"] is None
    assert kwargs["source_status"] == "unavailable"
    assert kwargs["source_failure_reason"].startswith(
        "Uploaded source ZIP could not be prepared:"
    )
    assert [row["original_filename"] for row in kwargs["artifacts"]] == ["diagnostic.log"]
    task.delay.assert_called_once_with(24)

    remaining = [p.name for p in tmp_path.iterdir()]
    assert not any(name.endswith("_source.zip") for name in remaining)
    assert any(name.endswith("diagnostic.log") for name in remaining)

def test_invalid_github_url_degrades_source_but_still_processes_diagnostics(
    tmp_path, monkeypatch
):
    create_analysis = Mock(return_value=SimpleNamespace(id=25, artifacts=[]))
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    result = analysis_api.upload_file(
        file=_upload("diagnostic.log", b"ERROR failed"),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
        github_url="https://not-github.example.com/acme/project",
    )

    assert result.id == 25
    kwargs = create_analysis.call_args.kwargs
    assert kwargs["source_kind"] == "github"
    assert kwargs["source_reference"] is None
    assert kwargs["source_status"] == "unavailable"
    assert kwargs["source_failure_reason"] == "Invalid GitHub repository URL."
    assert [row["original_filename"] for row in kwargs["artifacts"]] == ["diagnostic.log"]
    task.delay.assert_called_once_with(25)

def test_unexpected_source_exception_remains_fatal_and_is_not_degraded(
    tmp_path, monkeypatch
):
    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(
        analysis_api, "validate_source_zip", Mock(side_effect=RuntimeError("disk exploded"))
    )

    with pytest.raises(RuntimeError, match="disk exploded"):
        analysis_api.upload_file(
            file=_upload("diagnostic.log", b"ERROR failed"),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
            source_zip=_upload("source.zip", b"zip bytes"),
        )

    create_analysis.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_unsupported_binary_artifact_is_rejected_before_celery(tmp_path, monkeypatch):
    create_analysis = Mock()
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    binary_content = b"\x00\x01\x02\xffPK\x03\x04random binary payload"

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=_upload("payload.bin", binary_content),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 415
    assert "payload.bin" in error.value.detail
    create_analysis.assert_not_called()
    task.delay.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_mixed_batch_persists_unsupported_artifact_and_still_processes_the_rest(
    tmp_path, monkeypatch
):
    create_analysis = Mock(return_value=SimpleNamespace(id=22, artifacts=[]))
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    binary_content = b"\x00\x01\x02\xffPK\x03\x04random binary payload"

    result = analysis_api.upload_file(
        file=[_upload("app.log", b"ERROR failed"), _upload("payload.bin", binary_content)],
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 22
    rows = create_analysis.call_args.kwargs["artifacts"]
    assert [row["original_filename"] for row in rows] == ["app.log", "payload.bin"]
    assert rows[0]["detected_format"] is not None
    assert rows[0].get("status", "pending") == "pending"
    assert rows[1]["detected_format"] is None
    assert rows[1]["status"] == "unsupported"
    task.delay.assert_called_once_with(22)

def test_all_unsupported_multi_file_batch_still_rejects_the_whole_request(
    tmp_path, monkeypatch
):
    create_analysis = Mock()
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    binary_content = b"\x00\x01\x02\xffPK\x03\x04random binary payload"

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=[_upload("a.bin", binary_content), _upload("b.bin", binary_content)],
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 415
    assert "a.bin" in error.value.detail
    assert "b.bin" in error.value.detail
    create_analysis.assert_not_called()
    task.delay.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_identical_content_different_filenames_share_the_same_content_hash(
    tmp_path, monkeypatch
):
    create_analysis = Mock(return_value=SimpleNamespace(id=23, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())

    same_bytes = b"ERROR identical payload\n"

    analysis_api.upload_file(
        file=[_upload("original.log", same_bytes), _upload("copy.log", same_bytes)],
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    rows = create_analysis.call_args.kwargs["artifacts"]
    assert rows[0]["content_sha256"] == rows[1]["content_sha256"]
    assert rows[0]["content_sha256"] is not None

def test_generic_log_with_no_error_in_first_bytes_is_accepted(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=21, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())

    content = ("INFO startup ok\n" * 5000).encode() + b"ERROR late failure\n"

    result = analysis_api.upload_file(
        file=_upload("app.log", content),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 21
    create_analysis.assert_called_once()

def test_active_analysis_quota_preflight_maps_to_429(monkeypatch):
    monkeypatch.setattr(
        analysis_api, "user_has_analysis_capacity", lambda _db, _user_id: False
    )

    with pytest.raises(HTTPException) as error:
        analysis_api._require_analysis_capacity(
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 429
    assert "3 active investigations" in error.value.detail

def test_active_analysis_quota_race_cleans_staged_bytes_and_never_dispatches(
    tmp_path, monkeypatch
):
    create_analysis = Mock(side_effect=ActiveAnalysisLimitReached())
    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=_upload("diagnostic.log", b"ERROR failed"),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 429
    assert "3 active investigations" in error.value.detail
    assert list(tmp_path.iterdir()) == []
    task.delay.assert_not_called()

def test_post_commit_refresh_failure_preserves_staged_inputs_for_recovery(
    tmp_path, monkeypatch
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(
        username="post-commit-user",
        email="post-commit@example.com",
        hashed_password="x",
        is_verified=True,
    )
    db.add(user)
    db.commit()

    task = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "process_analysis", task)

    refresh = Mock(side_effect=RuntimeError("database unavailable after commit"))
    monkeypatch.setattr(db, "refresh", refresh)

    payload = b"ERROR durable upload\n"

    with pytest.raises(RuntimeError, match="database unavailable after commit"):
        analysis_api.upload_file(
            file=_upload("diagnostic.log", payload),
            db=db,
            current_user=user,
        )

    refresh.assert_called_once()
    task.delay.assert_not_called()

    verify_db = session_factory()
    try:
        created = verify_db.query(Analysis).one()
        assert created.status == "pending"
        assert len(created.artifacts) == 1
        with open(created.artifacts[0].saved_file_path, "rb") as staged_file:
            assert staged_file.read() == payload
        assert len(list(tmp_path.iterdir())) == 1
    finally:
        verify_db.close()
        db.close()

def _upload(filename: str, content: bytes):
    return SimpleNamespace(
        filename=filename,
        content_type="text/plain",
        file=BytesIO(content),
    )
