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


def _upload(filename: str, content: bytes):
    return SimpleNamespace(
        filename=filename,
        content_type="text/plain",
        file=BytesIO(content),
    )
