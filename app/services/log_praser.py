import re
from dataclasses import dataclass, field
from typing import Optional

TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"
)
LOG_LEVEL_PATTERN = re.compile(
    r"\b(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\b",
    re.IGNORECASE
)
TRACE_ID_PATTERN = re.compile(
    r"\b(?:trace_id|trace-id|traceid)[=: ]+([A-Za-z0-9_-]+)",
    re.IGNORECASE
)
REQUEST_ID_PATTERN = re.compile(
    r"b(?:request_id|request-id|requestid)[=: ]+([A-Za-Z0-9_-]+)",
    re.IGNORECASE
)
@dataclass
class StackFrame:
    file: str | None = None
    line: int | None = None
    fucntion: str | None = None

@dataclass
class ParsedEvent:
    line_number: int
    raw_line: str
    timestamp: Optional[str] = None
    level: Optional[str] = None
    trace_id: Optional[str] = None
    request_id: Optional[str] = None
    service: Optional[str] = None
    modeule: Optional[str] = None
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    stack_frames: list[StackFrame] = field(default_factory=list)
    endpoint: Optional[str] = None
    http_server: Optional[int] = None
    source_file: Optional[str] = None

def parse_log_line(line: str,line_number: int) -> ParsedEvent:
    timestamp_match = TIMESTAMP_PATTERN.search(line)
    level_match = LOG_LEVEL_PATTERN.search(line)
    trace_id_match = TRACE_ID_PATTERN.search(line)
    request_id_match = REQUEST_ID_PATTERN.search(line)

    return ParsedEvent(
        line_number=line_number,
        raw_line=line,
        timestamp=timestamp_match.group(0) if timestamp_match else None,
        level=level_match.group(1).upper() if level_match else None,
        trace_id=trace_id_match.group(1) if trace_id_match else None,
        request_id=request_id_match.group(1) if request_id_match else None
    )
