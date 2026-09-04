import re
from app.core.processing_config import SIMPLE_FALLBACK_MAX_TEXT_BYTES
from app.services.diagnostic_parser import (
    EXCEPTION_PATTERN,
    HTTP_STATUS_PATTERN,
    REQUEST_ID_PATTERN,
    TRACE_ID_PATTERN,
)

_MIN_FALLBACK_CHARACTERS = 8
_CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

def _looks_like_binary_or_control_garbage(text: str) -> bool:
    if not text:
        return True
    control_chars = len(_CONTROL_CHAR_PATTERN.findall(text))
    return control_chars > max(4, len(text) // 20)

def _bounded_text(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")

def capture_text_fallback_context(raw_text: str) -> dict | None:
    stripped = raw_text.strip()

    if len(stripped) < _MIN_FALLBACK_CHARACTERS:
        return None
    if _looks_like_binary_or_control_garbage(stripped):
        return None

    return {"kind": "text", "text": _bounded_text(stripped, SIMPLE_FALLBACK_MAX_TEXT_BYTES)}

def _looks_technical_or_diagnostic(text: str) -> bool:
    from app.services.diagnostic_adapters import _contains_stack_frame

    return bool(
        _contains_stack_frame(text)
        or EXCEPTION_PATTERN.search(text)
        or HTTP_STATUS_PATTERN.search(text)
        or TRACE_ID_PATTERN.search(text)
        or REQUEST_ID_PATTERN.search(text)
    )

def capture_ocr_fallback_context(text: str, ocr_confidence: float | None) -> dict | None:
    stripped = text.strip()

    if len(stripped) < _MIN_FALLBACK_CHARACTERS:
        return None
    if _looks_like_binary_or_control_garbage(stripped):
        return None
    if not _looks_technical_or_diagnostic(stripped):
        return None

    return {
        "kind": "ocr",
        "text": _bounded_text(stripped, SIMPLE_FALLBACK_MAX_TEXT_BYTES),
        "ocr_confidence": ocr_confidence,
    }
