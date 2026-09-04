from pathlib import Path
from PIL import Image, UnidentifiedImageError
from app.core.processing_config import MAX_OCR_IMAGE_BYTES, MAX_OCR_IMAGE_PIXELS, MEBIBYTE

_ocr = None

_ALLOWED_IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
}

_ALLOWED_PIL_FORMATS = {
    "PNG",
    "JPEG",
    "WEBP",
}

class OcrImageError(Exception):
    pass

class OcrImageTooLargeError(OcrImageError):
    pass

class InvalidOcrImageError(OcrImageError):
    pass

class OcrProcessingError(Exception):
    pass

def _get_ocr_engine():
    global _ocr

    if _ocr is None:
        from rapidocr_onnxruntime import RapidOCR

        _ocr = RapidOCR()
    return _ocr

def validate_ocr_image(file_path: str | Path) -> None:
    path = Path(file_path)

    if not path.is_file():
        raise InvalidOcrImageError("Image file not found")

    if path.suffix.lower() not in _ALLOWED_IMAGE_EXTENSIONS:
        raise InvalidOcrImageError("Unsupported image format")

    size = path.stat().st_size
    if size > MAX_OCR_IMAGE_BYTES:
        raise OcrImageTooLargeError(
            f"Image exceeds the {MAX_OCR_IMAGE_BYTES // MEBIBYTE} MiB image size limit"
        )

    try:
        with Image.open(path) as probe:
            width, height = probe.size
            decoded_format = (probe.format or "").upper()

            if decoded_format not in _ALLOWED_PIL_FORMATS:
                raise InvalidOcrImageError("Unsupported image format")

            probe.verify()
    except Image.DecompressionBombError as error:
        raise OcrImageTooLargeError(
            "Image exceeds the maximum decoded pixel count"
        ) from error
    except InvalidOcrImageError:
        raise
    except (UnidentifiedImageError, OSError, ValueError) as error:
        raise InvalidOcrImageError("Image could not be read") from error

    pixel_count = width * height
    if pixel_count > MAX_OCR_IMAGE_PIXELS:
        raise OcrImageTooLargeError(
            "Image exceeds the maximum decoded pixel count"
        )

def _run_ocr(file_path: str) -> list[tuple[str, float | None]]:
    path = Path(file_path)

    if path.suffix.lower() not in _ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("Unsupported image format")

    validate_ocr_image(path)

    try:
        results, _ = _get_ocr_engine()(str(path))

        if not results:
            return []

        lines: list[tuple[str, float | None]] = []
        for result in results:
            text = result[1].strip()
            if not text:
                continue
            confidence = result[2] if len(result) > 2 else None
            lines.append((text, float(confidence) if confidence is not None else None))

        return lines
    except Exception as error:
        raise OcrProcessingError("Image OCR could not be completed") from error

def extract_text_from_image_with_confidence(file_path: str) -> tuple[str, float | None]:
    lines = _run_ocr(file_path)
    text = "\n".join(line_text for line_text, _confidence in lines)
    confidences = [confidence for _text, confidence in lines if confidence is not None]
    confidence = (
        sum(confidences) / len(confidences)
        if confidences
        else None
    )
    return text, confidence
