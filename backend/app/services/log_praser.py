"""Canonical Devflo event types and the compatible generic log entry point.

The filename is intentionally retained as ``log_praser.py`` for import
compatibility.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

_COMPAT_PATTERN_NAMES = (
    "TIMESTAMP_PATTERN",
    "LOG_LEVEL_PATTERN",
    "TRACE_ID_PATTERN",
    "REQUEST_ID_PATTERN",
    "SERVICE_PATTERN",
    "MODULE_PATTERN",
    "HTTP_STATUS_PATTERN",
    "EXCEPTION_PATTERN",
)

__all__ = [  # noqa: F822 - legacy names are resolved by module __getattr__.
    "EXCEPTION_PATTERN",
    "HTTP_STATUS_PATTERN",
    "LOG_LEVEL_PATTERN",
    "MODULE_PATTERN",
    "REQUEST_ID_PATTERN",
    "SERVICE_PATTERN",
    "TIMESTAMP_PATTERN",
    "TRACE_ID_PATTERN",
    "ParsedEvent",
    "StackFrame",
    "estimate_parsed_event_size_bytes",
    "parse_log_line",
]


@dataclass
class StackFrame:
    file: str | None = None
    line: int | None = None
    function: str | None = None


@dataclass
class ParsedEvent:
    line_number: int
    raw_line: str
    timestamp: datetime | str | None = None
    level: str | None = None
    trace_id: str | None = None
    request_id: str | None = None
    service: str | None = None
    module: str | None = None
    exception_type: str | None = None
    exception_message: str | None = None
    fingerprint: str | None = None
    stack_frames: list[StackFrame] = field(default_factory=list)
    endpoint: str | None = None
    http_status: int | None = None
    source_file: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    host: str | None = None
    container: str | None = None
    pod: str | None = None
    source_format: str | None = None
    artifact_id: int | None = None
    source_matches: list[dict[str, Any]] = field(default_factory=list)


def estimate_parsed_event_size_bytes(event: ParsedEvent) -> int:
    """Conservatively estimate canonical data retained until a batch flushes."""

    values = (
        event.raw_line,
        event.level,
        event.trace_id,
        event.request_id,
        event.service,
        event.module,
        event.exception_type,
        event.exception_message,
        event.fingerprint,
        event.endpoint,
        event.source_file,
        event.span_id,
        event.parent_span_id,
        event.host,
        event.container,
        event.pod,
        event.source_format,
    )
    size = sum(
        len(value.encode("utf-8", errors="replace"))
        for value in values
        if value is not None
    )
    size += sum(
        len(value.encode("utf-8", errors="replace"))
        for frame in event.stack_frames
        for value in (frame.file, frame.function)
        if value is not None
    )
    return max(size, 1)


def parse_log_line(line: str, line_number: int) -> ParsedEvent:
    """Parse an ordinary diagnostic line into the canonical event shape."""

    # Imported lazily so this compatibility module remains the owner of the
    # canonical dataclasses without creating a module import cycle.
    from .diagnostic_parser import TIMESTAMP_PATTERN, normalize_text_event

    event = normalize_text_event(
        raw_text=line,
        line_number=line_number,
    )
    timestamp_match = TIMESTAMP_PATTERN.search(line)
    event.timestamp = timestamp_match.group(0) if timestamp_match else None
    return event


def __getattr__(name: str) -> Any:
    """Lazily preserve the original parser-pattern import surface."""

    if name not in _COMPAT_PATTERN_NAMES:
        raise AttributeError(name)

    from . import diagnostic_parser

    value = getattr(diagnostic_parser, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_COMPAT_PATTERN_NAMES))
