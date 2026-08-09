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
    r"\b(?:request_id|request-id|requestid)[=: ]+([A-Za-z0-9_-]+)",
    re.IGNORECASE
)
SERVICE_PATTERN = re.compile(
    r"\b(?:service|service_name|app)[=: ]+([A-Za-z0-9_.-]+)",
    re.IGNORECASE,
)

MODULE_PATTERN = re.compile(
    r"\b(?:module|logger)[=: ]+([A-Za-z0-9_.-]+)",
    re.IGNORECASE,
)

HTTP_STATUS_PATTERN = re.compile(
    r"\b(?:status|status_code|http_status)[=: ]+(\d{3})\b",
    re.IGNORECASE,
)

EXCEPTION_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*(?:Error|Exception))\b(?::\s*(.*))?"
)

@dataclass
class StackFrame:
    file: str | None = None
    line: int | None = None
    function: str | None = None

@dataclass
class ParsedEvent:
    line_number: int
    raw_line: str
    timestamp: Optional[str] = None
    level: Optional[str] = None
    trace_id: Optional[str] = None
    request_id: Optional[str] = None
    service: Optional[str] = None
    module: Optional[str] = None
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    fingerprint: Optional[str] = None
    stack_frames: list[StackFrame] = field(default_factory=list)
    endpoint: Optional[str] = None
    http_status: Optional[int] = None
    source_file: Optional[str] = None

def parse_log_line(line: str,line_number: int) -> ParsedEvent:
    timestamp_match = TIMESTAMP_PATTERN.search(line)
    level_match = LOG_LEVEL_PATTERN.search(line)
    trace_id_match = TRACE_ID_PATTERN.search(line)
    request_id_match = REQUEST_ID_PATTERN.search(line)
    service_match = SERVICE_PATTERN.search(line)
    module_match = MODULE_PATTERN.search(line)
    http_status_match = HTTP_STATUS_PATTERN.search(line)
    exception_match = EXCEPTION_PATTERN.search(line)

    return ParsedEvent(
        line_number=line_number,
        raw_line=line,
        timestamp=timestamp_match.group(0) if timestamp_match else None,
        level=level_match.group(1).upper() if level_match else None,
        trace_id=trace_id_match.group(1) if trace_id_match else None,
        request_id=request_id_match.group(1) if request_id_match else None,
        service=service_match.group(1) if service_match else None,
        module=module_match.group(1) if module_match else None,
        exception_type=exception_match.group(1) if exception_match else None,
        exception_message=exception_match.group(2) if exception_match else None,
        http_status=int(http_status_match.group(1)) if http_status_match else None
    )
