"""Sections 9-11: small, bounded unstructured-diagnostic-text fallback,
captured during an artifact's ORIGINAL (and only) ingestion pass - never a
second read of a text artifact, never a second RapidOCR call. Used only
when the WHOLE analysis otherwise retains zero structured Evidence (see
_finalize_analysis_task) - this is not a parallel evidence model, just a
small captured context that would otherwise be lost once ingestion moves
on to the next artifact.
"""
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
    """Permissive gate for a small, intentionally-uploaded TEXT artifact:
    the user's deliberate choice to upload a text diagnostic file is
    already a strong intent signal on its own, so this only screens out
    empty/binary/control-garbage content - it deliberately does NOT
    require ERROR/Traceback/Exception tokens the way structured Evidence
    retention does (see is_evidence_worthy in event_filter.py)."""
    stripped = raw_text.strip()

    if len(stripped) < _MIN_FALLBACK_CHARACTERS:
        return None
    if _looks_like_binary_or_control_garbage(stripped):
        return None

    return {"kind": "text", "text": _bounded_text(stripped, SIMPLE_FALLBACK_MAX_TEXT_BYTES)}


def _looks_technical_or_diagnostic(text: str) -> bool:
    """Reuses existing parsing signals only (never a new keyword
    encyclopedia): a recognized stack frame (Python/Java/Node, or the
    generic path:line[:column] shape - see diagnostic_parser.py), a real
    exception/failure type, an HTTP status code, or a trace/request
    identifier."""
    from app.services.diagnostic_adapters import _contains_stack_frame

    return bool(
        _contains_stack_frame(text)
        or EXCEPTION_PATTERN.search(text)
        or HTTP_STATUS_PATTERN.search(text)
        or TRACE_ID_PATTERN.search(text)
        or REQUEST_ID_PATTERN.search(text)
    )


def capture_ocr_fallback_context(text: str, ocr_confidence: float | None) -> dict | None:
    """Stronger gate than the plain-text one: an arbitrary photograph can
    contain non-trivial readable text (EXIT / ROOM 3 / WELCOME) without
    being developer diagnostic context at all, so "non-empty text" alone
    is never sufficient here - at least one broadly technical/diagnostic
    characteristic must also be present, deterministically and cheaply
    (never by asking Gemini to decide whether Gemini should be called)."""
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
