from pathlib import Path
from PIL import Image, UnidentifiedImageError
from rapidocr_onnxruntime import RapidOCR
from app.core.processing_config import MAX_OCR_IMAGE_BYTES, MAX_OCR_IMAGE_PIXELS, MEBIBYTE
_ocr = RapidOCR()

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
    """Base class for shared pre-OCR image validation failures - never
    raised by RapidOCR itself, only by validate_ocr_image() below."""


class OcrImageTooLargeError(OcrImageError):
    """File size or decoded pixel count exceeds the configured OCR
    resource bounds (MAX_OCR_IMAGE_BYTES / MAX_OCR_IMAGE_PIXELS)."""


class InvalidOcrImageError(OcrImageError):
    """Not a real, decodable image in a supported format."""


class OcrProcessingError(Exception):
    """RapidOCR failed after the image passed Devflo's validation gates."""


def validate_ocr_image(file_path: str | Path) -> None:
    """Shared resource-limit + integrity gate that must run before ANY
    RapidOCR call (see _run_ocr below, the sole caller-independent entry
    point) - RapidOCR itself must never be the first place Devflo discovers
    an oversized or absurdly-dense image. Runs no OCR itself; returns
    normally only when the image is safe to hand to RapidOCR."""
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
    """Single shared entry point into the RapidOCR engine: each result is
    RapidOCR's own (box, text, confidence) triple. Both public functions
    below read from this same call - there is no second/parallel OCR path.
    validate_ocr_image() runs here too (defense-in-depth) even though
    callers upstream (app/api/analysis.py, app/api/image.py) already
    validate before staging/dispatch - so there is no RapidOCR path,
    present or future, that can bypass the resource-limit gate."""
    path = Path(file_path)

    if path.suffix.lower() not in _ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("Unsupported image format")

    validate_ocr_image(path)

    try:
        results, _ = _ocr(str(path))
    except Exception as error:
        raise OcrProcessingError("Image OCR could not be completed") from error

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


def extract_text_from_image(file_path: str) -> str:
    return "\n".join(text for text, _confidence in _run_ocr(file_path))


def extract_text_from_image_with_confidence(file_path: str) -> tuple[str, float | None]:
    """Same extraction as extract_text_from_image(), plus the real RapidOCR
    confidence - never invented/defaulted. Multiple OCR-detected lines are
    combined into one diagnostic record further down the existing pipeline
    (diagnostic_adapters._stream_image_events already treats one image as
    one event, unchanged here), so their per-line confidences are reduced
    to the arithmetic mean: one noisy line shouldn't make an otherwise
    reliable image look globally unreliable. Returns (text, None) when
    RapidOCR found no line with a usable confidence score."""
    lines = _run_ocr(file_path)
    text = "\n".join(line_text for line_text, _confidence in lines)
    confidences = [confidence for _text, confidence in lines if confidence is not None]
    confidence = (
        sum(confidences) / len(confidences)
        if confidences
        else None
    )
    return text, confidence
