from pathlib import Path
from rapidocr_onnxruntime import RapidOCR
_ocr = RapidOCR()

_ALLOWED_IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
}


def _run_ocr(file_path: str) -> list[tuple[str, float | None]]:
    """Single shared entry point into the RapidOCR engine: each result is
    RapidOCR's own (box, text, confidence) triple. Both public functions
    below read from this same call - there is no second/parallel OCR path."""
    path = Path(file_path)

    if path.suffix.lower() not in _ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("Unsupported image format")

    results, _ = _ocr(str(path))

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
