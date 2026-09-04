from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from fastapi import HTTPException
from PIL import Image
from app.api import analysis as analysis_api
from app.core.processing_config import MAX_OCR_IMAGE_PIXELS
from app.services import image_text_extractor

def _upload(filename: str, content: bytes, content_type: str = "text/plain"):
    return SimpleNamespace(filename=filename, content_type=content_type, file=BytesIO(content))

def _image_upload(filename: str, content: bytes, content_type: str = "image/png"):
    return _upload(filename, content, content_type=content_type)

def _png_bytes(width: int, height: int, mode: str = "RGB") -> bytes:
    buffer = BytesIO()
    Image.new(mode, (width, height), 0).save(buffer, format="PNG")
    return buffer.getvalue()

def test_more_than_max_diagnostic_artifacts_is_rejected(tmp_path, monkeypatch):
    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)

    uploads = [
        _upload(f"f{i}.txt", b"ERROR x")
        for i in range(analysis_api.MAX_DIAGNOSTIC_ARTIFACTS + 1)
    ]

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(file=uploads, db=Mock(), current_user=SimpleNamespace(id=4))

    assert error.value.status_code == 413
    create_analysis.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_exactly_max_diagnostic_artifacts_is_permitted(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=30, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())

    uploads = [
        _upload(f"f{i}.txt", b"ERROR x")
        for i in range(analysis_api.MAX_DIAGNOSTIC_ARTIFACTS)
    ]

    result = analysis_api.upload_file(file=uploads, db=Mock(), current_user=SimpleNamespace(id=4))

    assert result.id == 30
    create_analysis.assert_called_once()

def test_more_than_max_ocr_images_is_rejected(tmp_path, monkeypatch):
    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)

    tiny_png = _png_bytes(4, 4)
    uploads = [
        _image_upload(f"img{i}.png", tiny_png)
        for i in range(analysis_api.MAX_OCR_IMAGES_PER_INVESTIGATION + 1)
    ]

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(file=uploads, db=Mock(), current_user=SimpleNamespace(id=4))

    assert error.value.status_code == 413
    create_analysis.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_exactly_max_ocr_images_is_permitted(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=33, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())

    tiny_png = _png_bytes(4, 4)
    uploads = [
        _image_upload(f"img{i}.png", tiny_png)
        for i in range(analysis_api.MAX_OCR_IMAGES_PER_INVESTIGATION)
    ]

    result = analysis_api.upload_file(file=uploads, db=Mock(), current_user=SimpleNamespace(id=4))

    assert result.id == 33

def test_oversized_image_is_rejected_by_the_streaming_ceiling_before_ocr(tmp_path, monkeypatch):
    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "MAX_OCR_IMAGE_BYTES", 10)
    monkeypatch.setattr(analysis_api, "UPLOAD_COPY_CHUNK_BYTES", 4)

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=_image_upload("big.png", b"0" * 50),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 413
    create_analysis.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_image_exactly_at_the_byte_limit_is_accepted(tmp_path, monkeypatch):
    content = _png_bytes(4, 4)
    create_analysis = Mock(return_value=SimpleNamespace(id=34, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())
    monkeypatch.setattr(analysis_api, "MAX_OCR_IMAGE_BYTES", len(content))
    monkeypatch.setattr(image_text_extractor, "MAX_OCR_IMAGE_BYTES", len(content))

    result = analysis_api.upload_file(
        file=_image_upload("exact.png", content),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 34

def test_excessive_decoded_pixel_count_is_rejected_before_ocr(tmp_path, monkeypatch):
    width = 6000
    height = (MAX_OCR_IMAGE_PIXELS // width) + 100
    assert width * height > MAX_OCR_IMAGE_PIXELS
    content = _png_bytes(width, height, mode="1")
    assert len(content) < analysis_api.MAX_OCR_IMAGE_BYTES

    create_analysis = Mock()
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    ocr_spy = Mock()
    monkeypatch.setattr(image_text_extractor, "_ocr", ocr_spy)

    with pytest.raises(HTTPException) as error:
        analysis_api.upload_file(
            file=_image_upload("huge_pixels.png", content),
            db=Mock(),
            current_user=SimpleNamespace(id=4),
        )

    assert error.value.status_code == 413
    create_analysis.assert_not_called()
    ocr_spy.assert_not_called()
    assert list(tmp_path.iterdir()) == []

def test_safe_small_image_still_reaches_ocr(tmp_path, monkeypatch):
    content = _png_bytes(4, 4)
    path = tmp_path / "safe.png"
    path.write_bytes(content)

    ocr_spy = Mock(return_value=([], None))
    monkeypatch.setattr(image_text_extractor, "_ocr", ocr_spy)

    image_text_extractor.extract_text_from_image_with_confidence(str(path))

    ocr_spy.assert_called_once_with(str(path))

def test_analysis_upload_and_ocr_engine_share_the_same_validator_and_constants():
    assert (
        analysis_api.validate_ocr_image
        is image_text_extractor.validate_ocr_image
    )
    assert (
        analysis_api.MAX_OCR_IMAGE_BYTES
        == image_text_extractor.MAX_OCR_IMAGE_BYTES
    )

def test_ocr_engine_called_at_most_once_per_accepted_image(tmp_path, monkeypatch):
    content = _png_bytes(4, 4)
    path = tmp_path / "a.png"
    path.write_bytes(content)

    ocr_spy = Mock(return_value=([], None))
    monkeypatch.setattr(image_text_extractor, "_ocr", ocr_spy)

    image_text_extractor.extract_text_from_image_with_confidence(str(path))

    assert ocr_spy.call_count == 1

def test_non_image_upload_is_not_bounded_by_the_ocr_image_byte_cap(tmp_path, monkeypatch):
    create_analysis = Mock(return_value=SimpleNamespace(id=32, artifacts=[]))
    monkeypatch.setattr(analysis_api, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(analysis_api, "create_analysis", create_analysis)
    monkeypatch.setattr(analysis_api, "process_analysis", Mock())
    monkeypatch.setattr(analysis_api, "MAX_OCR_IMAGE_BYTES", 4)

    content = b"ERROR failure detected in service\n" * 10

    result = analysis_api.upload_file(
        file=_upload("app.log", content),
        db=Mock(),
        current_user=SimpleNamespace(id=4),
    )

    assert result.id == 32
    rows = create_analysis.call_args.kwargs["artifacts"]
    assert rows[0]["size_bytes"] == len(content)
