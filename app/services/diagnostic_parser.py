import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .log_praser import ParsedEvent, StackFrame

TIMESTAMP_PATTERN = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?(?:Z|[+-]\d{2}:?\d{2})?\b"
)
LOG_LEVEL_PATTERN = re.compile(
    r"(?<![A-Za-z])"
    r"(TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|SEVERE|FATAL|"
    r"CRITICAL|ALERT|EMERG(?:ENCY)?)"
    r"(?![A-Za-z])",
    re.IGNORECASE,
)
TRACE_ID_PATTERN = re.compile(
    r"\b(?:trace[_-]?id|traceid)[\s=:" + "\"'" + r"]+([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)
SPAN_ID_PATTERN = re.compile(
    r"\b(?:span[_-]?id|spanid)[\s=:" + "\"'" + r"]+([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)
PARENT_SPAN_ID_PATTERN = re.compile(
    r"\b(?:parent[_-]?span[_-]?id|parentspanid)"
    r"[\s=:" + "\"'" + r"]+([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)
REQUEST_ID_PATTERN = re.compile(
    r"\b(?:request[_-]?id|requestid|correlation[_-]?id|"
    r"x-request-id)[\s=:" + "\"'" + r"]+([A-Za-z0-9_.:/-]+)",
    re.IGNORECASE,
)
SERVICE_PATTERN = re.compile(
    r"\b(?:service(?:[_-]?name)?|app|component)"
    r"[\s=:" + "\"'" + r"]+([A-Za-z0-9_.-]+)",
    re.IGNORECASE,
)
MODULE_PATTERN = re.compile(
    r"\b(?:module|logger)[\s=:" + "\"'" + r"]+([A-Za-z0-9_.:/-]+)",
    re.IGNORECASE,
)
HOST_PATTERN = re.compile(
    r"\b(?:host(?:name)?|node)[\s=:" + "\"'" + r"]+([A-Za-z0-9_.:-]+)",
    re.IGNORECASE,
)
CONTAINER_PATTERN = re.compile(
    r"\b(?:container(?:[_-]?(?:id|name))?)"
    r"[\s=:" + "\"'" + r"]+([A-Za-z0-9_.:/-]+)",
    re.IGNORECASE,
)
POD_PATTERN = re.compile(
    r"\b(?:pod(?:[_-]?name)?)[\s=:" + "\"'" + r"]+([A-Za-z0-9_.-]+)",
    re.IGNORECASE,
)
HTTP_STATUS_PATTERN = re.compile(
    r"\b(?:status|status_code|http_status)[\s=:" + "\"'" + r"]+(\d{3})\b",
    re.IGNORECASE,
)
ENDPOINT_PATTERN = re.compile(
    r"(?:\b(?:endpoint|route|path|url)[\s=:" + "\"'" + r"]+)"
    r"(https?://[^\s\"']+|/[^\s\"']*)",
    re.IGNORECASE,
)
HTTP_REQUEST_PATTERN = re.compile(
    r"\b(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(\S+)",
    re.IGNORECASE,
)
DIAGNOSTIC_FIELD_PATTERN = re.compile(
    r"\b(?P<key>[A-Za-z][A-Za-z0-9_-]*)\b[\"']?\s*(?:=|:)\s*[\"']?"
    r"(?P<value>[A-Za-z0-9_./:@?&={}-]+)",
)
SPACE_DIAGNOSTIC_FIELD_PATTERN = re.compile(
    r"\b(?P<key>parent[_-]?span[_-]?id|trace[_-]?id|span[_-]?id|"
    r"request[_-]?id|correlation[_-]?id|x-request-id|service(?:[_-]?name)?|"
    r"app|component|module|logger|host(?:name)?|node|"
    r"container(?:[_-]?(?:id|name))?|pod(?:[_-]?name)?|"
    r"status(?:[_-]?code)?|http[_-]?status|endpoint|route|path|url)"
    r"\s+(?P<value>[A-Za-z0-9_./:@?&={}-]+)",
    re.IGNORECASE,
)
EXCEPTION_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception|Failure))\b"
    r"(?::\s*(.*))?",
)
PYTHON_FRAME_PATTERN = re.compile(
    r'^\s*File "([^"]+)", line (\d+), in (.+)$',
    re.MULTILINE,
)
JAVA_FRAME_PATTERN = re.compile(
    r"^\s*at\s+(?:(.+?)\()?([^():]+):(\d+)\)?$",
    re.MULTILINE,
)
NODE_FRAME_PATTERN = re.compile(
    r"^\s*at\s+(?:(.+?)\s+\()?([^():]+):(\d+):\d+\)?$",
    re.MULTILINE,
)

LEVEL_ALIASES = {
    "WARN": "WARNING",
    "WARNING": "WARNING",
    "ERR": "ERROR",
    "ERROR": "ERROR",
    "SEVERE": "CRITICAL",
    "FATAL": "CRITICAL",
    "CRITICAL": "CRITICAL",
    "ALERT": "CRITICAL",
    "EMERG": "CRITICAL",
    "EMERGENCY": "CRITICAL",
    "NOTICE": "INFO",
}

FIELD_ALIASES = {
    "traceid": "trace_id",
    "spanid": "span_id",
    "parentspanid": "parent_span_id",
    "requestid": "request_id",
    "correlationid": "request_id",
    "xrequestid": "request_id",
    "service": "service",
    "servicename": "service",
    "app": "service",
    "component": "service",
    "module": "module",
    "logger": "module",
    "host": "host",
    "hostname": "host",
    "node": "host",
    "container": "container",
    "containerid": "container",
    "containername": "container",
    "pod": "pod",
    "podname": "pod",
    "status": "http_status",
    "statuscode": "http_status",
    "httpstatus": "http_status",
    "endpoint": "endpoint",
    "route": "endpoint",
    "path": "endpoint",
    "url": "endpoint",
}


def normalize_level(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, (int, float)) or (
        isinstance(value, str) and value.strip().isdigit()
    ):
        number = _safe_int(value)
        if number is None:
            return None
        if number >= 50:
            return "CRITICAL"
        if number >= 40:
            return "ERROR"
        if number >= 30:
            return "WARNING"
        if number >= 20:
            return "INFO"
        return "DEBUG"

    normalized = str(value).strip().upper()
    return LEVEL_ALIASES.get(normalized, normalized or None)


def normalize_otel_severity(value: Any) -> str | None:
    if value is None:
        return None

    is_numeric = isinstance(value, (int, float)) or (
        isinstance(value, str) and value.strip().isdigit()
    )
    if not is_numeric:
        return normalize_level(value)

    number = _safe_int(value)
    if number is None or number <= 0:
        return None

    if number >= 21:
        return "CRITICAL"
    if number >= 17:
        return "ERROR"
    if number >= 13:
        return "WARNING"
    if number >= 9:
        return "INFO"
    if number >= 5:
        return "DEBUG"
    return "TRACE"


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) or str(value).strip().isdigit():
        number = _safe_int(value)
        if number is None:
            return None
        try:
            magnitude = abs(number)
            if magnitude >= 100_000_000_000_000_000:
                seconds = number / 1_000_000_000
            elif magnitude >= 100_000_000_000_000:
                seconds = number / 1_000_000
            elif magnitude >= 100_000_000_000:
                seconds = number / 1_000
            else:
                seconds = number

            parsed = datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value).strip().strip("[]")

        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = None

        if parsed is None:
            for date_format in (
                "%d/%b/%Y:%H:%M:%S %z",
                "%b %d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S,%f",
                "%Y-%m-%d %H:%M:%S",
                "%a %b %d %H:%M:%S.%f %Y",
                "%a %b %d %H:%M:%S %Y",
            ):
                try:
                    # Evidence timestamps intentionally use naive UTC because
                    # the existing SQL column is DateTime without timezone.
                    parsed = datetime.strptime(text, date_format)  # noqa: DTZ007
                    if date_format == "%b %d %H:%M:%S":
                        parsed = parsed.replace(year=datetime.now(UTC).year)
                    break
                except ValueError:
                    continue

        if parsed is None:
            return None

    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)

    return parsed


def level_from_http_status(status: int | None) -> str | None:
    if status is None:
        return None
    if status >= 500:
        return "ERROR"
    if status >= 400:
        return "WARNING"
    return "INFO"


def _match_group(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    return match.group(1) if match else None


def _parse_stack_frames(raw_text: str) -> list[StackFrame]:
    frames = [
        StackFrame(file=file, line=int(line), function=function)
        for file, line, function in PYTHON_FRAME_PATTERN.findall(raw_text)
    ]

    for pattern in (NODE_FRAME_PATTERN, JAVA_FRAME_PATTERN):
        for function, file, line in pattern.findall(raw_text):
            frames.append(
                StackFrame(
                    file=file,
                    line=int(line),
                    function=function or None,
                )
            )

    return frames


def normalize_text_event(
    raw_text: str,
    line_number: int,
    *,
    source_file: str | None = None,
    source_format: str | None = None,
    defaults: Mapping[str, Any] | None = None,
) -> ParsedEvent:
    defaults = defaults or {}
    timestamp_match = TIMESTAMP_PATTERN.search(raw_text)
    level_match = LOG_LEVEL_PATTERN.search(raw_text)
    fields = _extract_diagnostic_fields(raw_text)

    if "\n" in raw_text:
        exception_matches = list(EXCEPTION_PATTERN.finditer(raw_text))
        exception_match = exception_matches[-1] if exception_matches else None
    else:
        exception_match = EXCEPTION_PATTERN.search(raw_text)

    endpoint = fields.get("endpoint")

    if endpoint is None and any(
        marker in raw_text
        for marker in ("GET ", "POST ", "PUT ", "PATCH ", "DELETE ", "HEAD ")
    ):
        endpoint = _match_group(HTTP_REQUEST_PATTERN, raw_text)

    status = _safe_int(defaults.get("http_status"))
    if status is None:
        status = _safe_int(fields.get("http_status"))
    level = normalize_level(
        defaults.get("level") or (level_match.group(1) if level_match else None)
    )

    lower_text = raw_text.lower()
    if level is None and (
        exception_match is not None
        or any(
            marker in lower_text
            for marker in ("traceback", "panic:", "segmentation fault", "fatal:")
        )
    ):
        level = "ERROR"
    elif level is None and "slow query" in lower_text:
        level = "WARNING"
    elif level is None:
        level = level_from_http_status(status)

    exception_type = _as_text(defaults.get("exception_type"))
    exception_message = _as_text(defaults.get("exception_message"))

    if exception_match is not None:
        exception_type = exception_type or exception_match.group(1).split(".")[-1]
        exception_message = exception_message or exception_match.group(2)

    stack_frames = []
    if "\n" in raw_text or raw_text.lstrip().startswith(("at ", "File ")):
        stack_frames = _parse_stack_frames(raw_text)

    return ParsedEvent(
        line_number=line_number,
        raw_line=raw_text,
        timestamp=parse_timestamp(
            defaults.get("timestamp")
            or (timestamp_match.group(0) if timestamp_match else None)
        ),
        level=level,
        trace_id=_as_text(defaults.get("trace_id")) or fields.get("trace_id"),
        request_id=_as_text(defaults.get("request_id")) or fields.get("request_id"),
        service=_as_text(defaults.get("service")) or fields.get("service"),
        module=_as_text(defaults.get("module")) or fields.get("module"),
        exception_type=exception_type,
        exception_message=exception_message,
        stack_frames=stack_frames,
        endpoint=_as_text(defaults.get("endpoint")) or endpoint,
        http_status=status,
        source_file=source_file,
        span_id=_as_text(defaults.get("span_id")) or fields.get("span_id"),
        parent_span_id=_as_text(defaults.get("parent_span_id"))
        or fields.get("parent_span_id"),
        host=_as_text(defaults.get("host")) or fields.get("host"),
        container=_as_text(defaults.get("container")) or fields.get("container"),
        pod=_as_text(defaults.get("pod")) or fields.get("pod"),
        source_format=source_format,
    )


def normalize_structured_event(
    data: Mapping[str, Any],
    line_number: int,
    *,
    source_file: str | None = None,
    source_format: str | None = None,
    inherited: Mapping[str, Any] | None = None,
) -> ParsedEvent:
    """Map common structured-log aliases through the shared text normalizer."""

    inherited = inherited or {}
    exception_type = _first_value(
        data,
        (
            ("exception_type",),
            ("exception", "type"),
            ("error", "type"),
            ("error", "name"),
        ),
    )
    exception_message = _first_value(
        data,
        (
            ("exception_message",),
            ("exceptionMessage",),
            ("exception", "message"),
            ("error", "message"),
            ("error",),
            ("exception",),
        ),
    )
    message_value = _first_value(
        data,
        (
            ("message",),
            ("msg",),
            ("log",),
            ("@message",),
            ("body",),
            ("event", "message"),
            ("error", "message"),
            ("exception", "message"),
        ),
    )
    message = _value_text(message_value)

    if message is None:
        message = (
            _value_text(exception_message)
            or _value_text(_first_value(data, (("name",),)))
            or "structured event"
        )

    status = _safe_int(
        _first_value(
            data,
            (
                ("http_status",),
                ("status_code",),
                ("statusCode",),
                ("http", "status_code"),
                ("http", "response", "status_code"),
                ("response", "status"),
                ("response", "statusCode"),
                ("elb_status_code",),
                ("target_status_code",),
                ("status",),
            ),
        )
    )
    level_value = _first_value(
        data,
        (
            ("level",),
            ("severity",),
            ("severityText",),
            ("severity_text",),
            ("logLevel",),
            ("levelname",),
        ),
    )

    if level_value is None:
        status_text = _first_value(data, (("status",), ("state",)))
        if isinstance(status_text, str) and status_text.lower() in {
            "warn",
            "warning",
            "error",
            "failed",
            "failure",
            "fatal",
            "critical",
        }:
            level_value = (
                "ERROR" if status_text.lower() in {"failed", "failure"} else status_text
            )

    if level_value is None and _first_value(data, (("stream",),)) == "stderr":
        level_value = "ERROR"
    if level_value is None and (
        _as_text(exception_type) is not None or _as_text(exception_message) is not None
    ):
        level_value = "ERROR"

    defaults = {
        "timestamp": _first_value(
            data,
            (
                ("timestamp",),
                ("@timestamp",),
                ("time",),
                ("datetime",),
                ("ts",),
                ("eventTime",),
                ("observed_timestamp",),
                ("requestTimeEpoch",),
                ("request_time_epoch",),
                ("requestTime",),
                ("request_time",),
                ("requestContext", "requestTimeEpoch"),
                ("requestContext", "requestTime"),
            ),
        ),
        "level": level_value,
        "trace_id": _first_value(
            data,
            (("trace_id",), ("traceId",), ("trace", "id")),
        ),
        "span_id": _first_value(
            data,
            (("span_id",), ("spanId",), ("span", "id")),
        ),
        "parent_span_id": _first_value(
            data,
            (
                ("parent_span_id",),
                ("parentSpanId",),
                ("span", "parent_id"),
            ),
        ),
        "request_id": _first_value(
            data,
            (
                ("request_id",),
                ("requestId",),
                ("correlation_id",),
                ("correlationId",),
                ("x-request-id",),
                ("awsRequestId",),
                ("requestContext", "requestId"),
            ),
        ),
        "service": _first_value(
            data,
            (
                ("service", "name"),
                ("service_name",),
                ("service",),
                ("app",),
                ("component",),
                ("kubernetes", "labels", "app"),
                ("kubernetes", "container_name"),
            ),
        )
        or inherited.get("service"),
        "module": _first_value(
            data,
            (("module",), ("logger",), ("logger_name",), ("scope", "name")),
        )
        or inherited.get("module"),
        "host": _first_value(
            data,
            (
                ("host", "name"),
                ("hostname",),
                ("host",),
                ("serverIPAddress",),
                ("kubernetes", "host"),
            ),
        )
        or inherited.get("host"),
        "container": _first_value(
            data,
            (
                ("container", "id"),
                ("container", "name"),
                ("container_id",),
                ("container_name",),
                ("container",),
                ("kubernetes", "container_name"),
            ),
        )
        or inherited.get("container"),
        "pod": _first_value(
            data,
            (
                ("pod",),
                ("pod_name",),
                ("kubernetes", "pod_name"),
            ),
        )
        or inherited.get("pod"),
        "endpoint": _first_value(
            data,
            (
                ("url", "path"),
                ("endpoint",),
                ("route",),
                ("path",),
                ("url",),
                ("http", "route"),
                ("request", "url"),
                ("request", "path"),
                ("requestContext", "resourcePath"),
                ("resourcePath",),
                ("resource_path",),
                ("rawPath",),
                ("routeKey",),
                ("requestContext", "http", "path"),
            ),
        ),
        "http_status": status,
        "exception_type": exception_type,
        "exception_message": exception_message,
    }

    for key, value in inherited.items():
        if defaults.get(key) is None:
            defaults[key] = value

    event = normalize_text_event(
        raw_text=message,
        line_number=line_number,
        source_file=source_file,
        source_format=source_format,
        defaults=defaults,
    )

    if event.level is None:
        event.level = level_from_http_status(status)

    return event


def _first_value(
    data: Mapping[str, Any],
    paths: tuple[tuple[str, ...], ...],
) -> Any:
    for path in paths:
        current: Any = data

        for part in path:
            if not isinstance(current, Mapping):
                current = None
                break

            if part in current:
                current = current[part]
                continue

            normalized_part = _normalized_key(part)
            matching_key = next(
                (
                    key
                    for key in current
                    if _normalized_key(str(key)) == normalized_part
                ),
                None,
            )

            if matching_key is None:
                current = None
                break

            current = current[matching_key]

        if current is not None:
            return current

    return None


def _extract_diagnostic_fields(raw_text: str) -> dict[str, str]:
    fields: dict[str, str] = {}

    for match in DIAGNOSTIC_FIELD_PATTERN.finditer(raw_text):
        _store_diagnostic_field(fields, match)

    lowered = raw_text.lower()
    if any(
        marker in lowered
        for marker in (
            "trace",
            "span",
            "request",
            "correlation",
            "service ",
            "module ",
            "logger ",
            "host ",
            "container ",
            "pod ",
            "status ",
            "endpoint ",
        )
    ):
        for match in SPACE_DIAGNOSTIC_FIELD_PATTERN.finditer(raw_text):
            _store_diagnostic_field(fields, match)

    return fields


def _store_diagnostic_field(
    fields: dict[str, str],
    match: re.Match[str],
) -> None:
    normalized_key = match.group("key").lower().replace("_", "").replace("-", "")
    canonical_key = FIELD_ALIASES.get(normalized_key)
    if canonical_key is not None and canonical_key not in fields:
        fields[canonical_key] = match.group("value").rstrip(".,;)")


def _normalized_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _value_text(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key in (
            "stringValue",
            "intValue",
            "doubleValue",
            "boolValue",
            "bytesValue",
        ):
            if key in value:
                return _as_text(value[key])
        return None
    return _as_text(value)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (OverflowError, TypeError, ValueError):
        return None
