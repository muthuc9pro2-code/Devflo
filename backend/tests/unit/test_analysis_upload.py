from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from app.api import analysis as analysis_api


def test_multiple_files_are_streamed_into_one_analysis(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=17))
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
    create_analysis = Mock(return_value=SimpleNamespace(id=18))
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
    create_analysis = Mock(return_value=SimpleNamespace(id=19))
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
    create_analysis = Mock(return_value=SimpleNamespace(id=20))
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


def test_source_zip_limit_cleans_all_staged_files(tmp_path, monkeypatch):
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "MAX_SOURCE_ARCHIVE_BYTES", 3)
    monkeypatch.setattr(analysis_api, "UPLOAD_COPY_CHUNK_BYTES", 2)
    monkeypatch.setattr(analysis_api, "create_analysis", Mock())

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=_upload("diagnostic.log", b"ERROR failed"),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
            source_zip=_upload("source.zip", b"1234"),
        )

    assert error.value.status_code == 413
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


def test_generic_log_with_no_error_in_first_bytes_is_accepted(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=21))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())

    # Plenty of ordinary INFO/startup noise up front; the only failure would
    # show up far later than a bounded detection sample could ever see.
    content = ("INFO startup ok\n" * 5000).encode() + b"ERROR late failure\n"

    result = analysis_api.upload_file(
        file=_upload("app.log", content),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 21
    create_analysis.assert_called_once()


def _upload(filename: str, content: bytes):
    return SimpleNamespace(
        filename=filename,
        content_type="text/plain",
        file=BytesIO(content),
    )
