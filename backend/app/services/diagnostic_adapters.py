import json
import logging
import re
import shlex
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any
import ijson
from app.core.processing_config import ARTIFACT_DETECTION_SAMPLE_BYTES, JSON_STREAM_BUFFER_BYTES, MAX_DIAGNOSTIC_RECORD_BYTES
from app.utils.bounded_json import BoundedJsonStream
from app.utils.file_reader import stream_text_lines
from .artifact_detector import ArtifactFormat
from .diagnostic_parser import EXCEPTION_PATTERN, fast_path_prefixed_event, level_from_http_status, normalize_level, normalize_structured_event, normalize_text_event, parse_timestamp, structured_event_may_be_important
from .event_filter import IMPORTANT_LEVELS
from .log_praser import ParsedEvent
logger = logging.getLogger(__name__)

if ijson.backend != "yajl2_c":
    # ijson silently falls back to its pure-Python backend (order of
    # magnitude slower for JSON/OTel/BROWSER artifacts) whenever the yajl2 C
    # extension wasn't built at install time - e.g. libyajl2 dev headers
    # missing from the image `pip install` ran in. That failure is otherwise
    # invisible until someone notices JSON ingestion is unexpectedly slow.
    logger.warning(
        "ijson is using the %r backend instead of the C-accelerated "
        "yajl2_c backend; JSON/OpenTelemetry/BROWSER artifact ingestion "
        "will be substantially slower. Install libyajl2 development "
        "headers before `pip install ijson` to fix this.",
        ijson.backend,
    )

@dataclass(slots=True)
class ArtifactEvent:
    event: ParsedEvent | None
    end_offset: int
    artifact_line_number: int
    global_end_line_number: int
    batch_size_bytes: int

@dataclass(slots=True)
class _PendingTextRecord:
    lines: list[str]
    start_global_line: int
    end_global_line: int
    artifact_line_number: int
    end_offset: int
    size_bytes: int
    multiline_kind: str | None

@dataclass(slots=True)
class _BoundedStructuredCapture:
    prefix: str
    depth: int = 1
    values: dict[str, Any] = field(default_factory=dict)
    priorities: dict[str, int] = field(default_factory=dict)
    captured_bytes: int = 0

    def consume(self, prefix: str, event: str, value: Any) -> None:
        if event not in {'string', 'number', 'boolean', 'null'}:
            return
        relative = prefix[len(self.prefix):].lstrip('.')
        canonical_key = _structured_canonical_key(relative)
        if canonical_key is None:
            return
        priority = _structured_alias_priority(canonical_key, relative)
        existing_priority = self.priorities.get(canonical_key)
        if existing_priority is not None and existing_priority <= priority:
            return
        value_size = len(str(value).encode('utf-8', errors='replace'))
        existing_value = self.values.get(canonical_key)
        existing_size = len(str(existing_value).encode('utf-8', errors='replace')) if existing_priority is not None else 0
        retained_size = self.captured_bytes - existing_size + value_size
        if retained_size > MAX_DIAGNOSTIC_RECORD_BYTES:
            return
        self.captured_bytes = retained_size
        self.values[canonical_key] = value
        self.priorities[canonical_key] = priority

    def result(self) -> dict[str, Any]:
        if 'message' not in self.values:
            method = self.values.pop('_method', None)
            endpoint = self.values.get('endpoint')
            status = self.values.get('http_status', self.values.get('status'))
            exception_message = self.values.get('exception_message')
            if exception_message is not None:
                self.values['message'] = exception_message
            elif method or endpoint or status:
                self.values['message'] = ' '.join((str(item) for item in (method, endpoint, status) if item is not None))
            else:
                self.values['message'] = 'structured event'
        else:
            self.values.pop('_method', None)
        return self.values

    @property
    def retained_size_bytes(self) -> int:
        return max(self.captured_bytes, 1)

def stream_artifact_events(*, file_path: str, artifact_format: ArtifactFormat, source_file: str, start_offset: int=0, start_artifact_line: int=0, global_line_number: int=0) -> Iterator[ArtifactEvent]:
    if artifact_format == ArtifactFormat.OPENTELEMETRY:
        from .otel_adapter import stream_otlp_events
        yield from stream_otlp_events(file_path=file_path, source_file=source_file, skip_records=start_artifact_line, global_line_number=global_line_number)
        return
    if artifact_format == ArtifactFormat.CLOUD_GATEWAY:
        cloudfront_fields = _read_cloudfront_fields(file_path)
        if cloudfront_fields is not None:
            yield from _stream_cloudfront_events(file_path=file_path, source_file=source_file, fields=cloudfront_fields, start_offset=start_offset, start_artifact_line=start_artifact_line, global_line_number=global_line_number)
            return
    if artifact_format in {ArtifactFormat.JSON, ArtifactFormat.BROWSER} or (artifact_format in {ArtifactFormat.CONTAINER, ArtifactFormat.CLOUD_GATEWAY, ArtifactFormat.SERVERLESS} and _artifact_starts_with_json(file_path)):
        yield from _stream_json_events(file_path=file_path, artifact_format=artifact_format, source_file=source_file, start_offset=start_offset, start_artifact_line=start_artifact_line, global_line_number=global_line_number)
        return
    yield from _stream_text_events(file_path=file_path, artifact_format=artifact_format, source_file=source_file, start_offset=start_offset, start_artifact_line=start_artifact_line, global_line_number=global_line_number)

def _stream_cloudfront_events(*, file_path: str, source_file: str, fields: list[str], start_offset: int, start_artifact_line: int, global_line_number: int) -> Iterator[ArtifactEvent]:
    local_line = start_artifact_line
    global_line = global_line_number
    previous_offset = start_offset
    for line, end_offset in stream_text_lines(file_path, start_offset=start_offset):
        record_size = end_offset - previous_offset
        previous_offset = end_offset
        local_line += 1
        global_line += 1
        stripped = line.removeprefix('\ufeff').strip()
        if not stripped or stripped.startswith('#'):
            updated_fields = _cloudfront_fields_from_line(stripped)
            if updated_fields is not None:
                fields = updated_fields
            continue
        values = line.split('\t')
        if len(values) == 1:
            values = line.split()
        data = {key.lower(): value for key, value in zip(fields, values, strict=False)}
        event = _normalize_cloudfront_event(line, global_line, source_file, data) if _cloudfront_row_may_be_important(data) else None
        yield ArtifactEvent(event=event, end_offset=end_offset, artifact_line_number=local_line, global_end_line_number=global_line, batch_size_bytes=max(record_size, 1))

def _read_cloudfront_fields(file_path: str) -> list[str] | None:
    with open(file_path, 'rb') as stream:
        sample = stream.read(ARTIFACT_DETECTION_SAMPLE_BYTES)
    for line in sample.decode('utf-8-sig', errors='replace').splitlines():
        fields = _cloudfront_fields_from_line(line)
        if fields is not None:
            return fields
    return None

def _cloudfront_fields_from_line(line: str) -> list[str] | None:
    if not line.lstrip('\ufeff').lower().startswith('#fields:'):
        return None
    fields = line.split(':', 1)[1].split()
    lowered = {field.lower() for field in fields}
    if not {'date', 'time', 'sc-status', 'cs-uri-stem'} <= lowered or not lowered.intersection({'x-edge-location', 'x-edge-request-id', 'x-edge-result-type'}):
        return None
    return fields

def _stream_text_events(*, file_path: str, artifact_format: ArtifactFormat, source_file: str, start_offset: int, start_artifact_line: int, global_line_number: int) -> Iterator[ArtifactEvent]:
    local_line = start_artifact_line
    global_line = global_line_number
    previous_offset = start_offset
    pending: _PendingTextRecord | None = None
    for line, end_offset in stream_text_lines(file_path, start_offset=start_offset):
        local_line += 1
        global_line += 1
        line_size = end_offset - previous_offset
        previous_offset = end_offset
        cri_match = CRI_RE.search(line) if artifact_format == ArtifactFormat.CONTAINER else None
        if cri_match is not None:
            if pending is not None and pending.multiline_kind == 'cri_partial':
                pending_match = CRI_RE.search(pending.lines[0])
                same_stream = pending_match is not None and pending_match.group('stream') == cri_match.group('stream')
                fits_record_bound = pending.size_bytes + line_size <= MAX_DIAGNOSTIC_RECORD_BYTES
                if same_stream and fits_record_bound:
                    pending.lines.append(cri_match.group('body'))
                    pending.end_global_line = global_line
                    pending.artifact_line_number = local_line
                    pending.end_offset = end_offset
                    pending.size_bytes += line_size
                    if cri_match.group('flag') == 'P':
                        continue
                    yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
                    pending = None
                    continue
                yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
                pending = None
            elif pending is not None:
                yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
                pending = None
            cri_record = _PendingTextRecord(lines=[line], start_global_line=global_line, end_global_line=global_line, artifact_line_number=local_line, end_offset=end_offset, size_bytes=line_size, multiline_kind='cri_partial' if cri_match.group('flag') == 'P' else None)
            if cri_match.group('flag') == 'P':
                pending = cri_record
            else:
                yield _build_text_artifact_event(cri_record, artifact_format=artifact_format, source_file=source_file)
            continue
        if pending is not None and pending.multiline_kind == 'cri_partial':
            yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
            pending = None
        if pending is not None and _is_multiline_continuation(pending, line):
            if pending.size_bytes + line_size <= MAX_DIAGNOSTIC_RECORD_BYTES:
                pending.lines.append(line)
                pending.end_global_line = global_line
                pending.artifact_line_number = local_line
                pending.end_offset = end_offset
                pending.size_bytes += line_size
                continue
            yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
            pending = None
        if pending is not None:
            yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
        pending = _PendingTextRecord(lines=[line], start_global_line=global_line, end_global_line=global_line, artifact_line_number=local_line, end_offset=end_offset, size_bytes=line_size, multiline_kind=_multiline_kind(artifact_format, line))
    if pending is not None:
        yield _build_text_artifact_event(pending, artifact_format=artifact_format, source_file=source_file)
_GENERIC_IMPORTANT_MARKERS = (
    "warn",
    "error",
    "err",
    "severe",
    "fatal",
    "critical",
    "alert",
    "emerg",
    "exception",
    "failure",
    "traceback",
    "panic:",
    "segmentation fault",
    "slow query",
)
# "status" alone used to be in _GENERIC_IMPORTANT_MARKERS, but every HTTP
# status code contains that word, so ordinary status=200 records paid for a
# full normalize just to be discarded. level_from_http_status() only treats
# >=400 as important, so gate on that same threshold cheaply up front instead
# of on the word "status" itself.
_GENERIC_IMPORTANT_STATUS_RE = re.compile(
    r'(?:http[_-]?)?status(?:[_-]?code)?["\']?[^0-9A-Za-z]{0,3}[45]\d{2}\b',
    re.IGNORECASE,
)
def _generic_text_may_be_important(raw_text: str) -> bool:
    lowered = raw_text.lower()
    return any(marker in lowered for marker in _GENERIC_IMPORTANT_MARKERS) or (
        'status' in lowered and _GENERIC_IMPORTANT_STATUS_RE.search(raw_text) is not None
    )

def _ci_cd_may_be_important(raw_text: str) -> bool:
    lowered = raw_text.lower()
    return (
        '##[error]' in lowered
        or 'build failed' in lowered
        or 'deployment failed' in lowered
        or '##[warning]' in lowered
    )

# Every non-GENERIC text format used to run the full normalize_text_event()
# classify+extract machinery on 100% of records unconditionally, with no
# early retention decision at all - GENERIC was the only format that got to
# skip parsing a record proven unimportant. Phase-1 profiling on
# representative 10 MiB fixtures per format showed exactly why the other
# formats were 2-6x slower than GENERIC at equal size even after the
# tokenizer work: SERVERLESS was ~3% important, CLOUD_GATEWAY ~7%,
# CONTAINER ~17% (and CONTAINER was the single slowest format measured,
# ~12.6s), CI_CD ~21%, BROWSER ~22%, WEB_SERVER ~25%, STACK_TRACE ~25%,
# MESSAGE_BROKER ~29%, JSON ~33%, SYSLOG ~47%. Each gate below reuses the
# SAME regex/threshold the format's own normalizer already uses to decide
# level - it does not invent a new notion of "important", it just checks it
# earlier and cheaper. DATABASE is intentionally excluded: its current
# semantics mark every "# Time:" slow-query block WARNING unconditionally
# (see _build_text_artifact_event below), so 100% of its records are already
# important and a gate would filter nothing.
def _may_be_important(artifact_format: ArtifactFormat, raw_text: str) -> bool:
    if artifact_format == ArtifactFormat.GENERIC:
        return _generic_text_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.WEB_SERVER:
        return _web_server_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.SYSLOG:
        return _syslog_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.CONTAINER:
        return _container_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.CLOUD_GATEWAY:
        return _cloud_gateway_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.MESSAGE_BROKER:
        # defaults only ever sets 'service' (see _normalize_message_broker_event
        # below) - level is always decided from the raw text, exactly like
        # GENERIC, so the same gate semantics apply unchanged.
        return _generic_text_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.SERVERLESS:
        return _serverless_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.CI_CD:
        return _ci_cd_may_be_important(raw_text)
    if artifact_format == ArtifactFormat.STACK_TRACE:
        return _stack_trace_may_be_important(raw_text)
    return True

def _build_text_artifact_event(record: _PendingTextRecord, *, artifact_format: ArtifactFormat, source_file: str) -> ArtifactEvent:
    raw_text = ''.join(record.lines) if record.multiline_kind == 'cri_partial' else '\n'.join(record.lines)
    if not _may_be_important(artifact_format, raw_text):
        return ArtifactEvent(
        event=None,
        end_offset=record.end_offset,
        artifact_line_number=record.artifact_line_number,
        global_end_line_number=record.end_global_line,
        batch_size_bytes=max(record.size_bytes, 1),
    )
    if artifact_format == ArtifactFormat.WEB_SERVER:
        event = _normalize_web_event(raw_text, record.start_global_line, source_file)
    elif artifact_format == ArtifactFormat.SYSLOG:
        event = _normalize_syslog_event(raw_text, record.start_global_line, source_file)
    elif artifact_format == ArtifactFormat.CONTAINER:
        event = _normalize_container_text_event(raw_text, record.start_global_line, source_file)
    elif artifact_format == ArtifactFormat.CLOUD_GATEWAY:
        event = _normalize_cloud_gateway_event(raw_text, record.start_global_line, source_file)
    elif artifact_format == ArtifactFormat.MESSAGE_BROKER:
        event = _normalize_message_broker_event(raw_text, record.start_global_line, source_file)
    elif artifact_format == ArtifactFormat.SERVERLESS:
        event = _normalize_serverless_text_event(raw_text, record.start_global_line, source_file)
    else:
        event = None
        if artifact_format == ArtifactFormat.GENERIC:
            event = fast_path_prefixed_event(raw_text, record.start_global_line, source_file=source_file, source_format=artifact_format.value)
        if event is None:
            defaults: dict[str, Any] = {}
            if artifact_format == ArtifactFormat.CI_CD:
                lowered = raw_text.lower()
                if '##[error]' in lowered or 'build failed' in lowered or 'deployment failed' in lowered:
                    defaults['level'] = 'ERROR'
                elif '##[warning]' in lowered:
                    defaults['level'] = 'WARNING'
            elif artifact_format == ArtifactFormat.DATABASE:
                lowered = raw_text.lower()
                if 'slow query' in lowered or 'query_time:' in lowered:
                    defaults['level'] = 'WARNING'
            elif artifact_format == ArtifactFormat.STACK_TRACE and record.multiline_kind == 'crash_report':
                defaults['level'] = 'ERROR'
            event = normalize_text_event(raw_text, record.start_global_line, source_file=source_file, source_format=artifact_format.value, defaults=defaults)
    return ArtifactEvent(event=event, end_offset=record.end_offset, artifact_line_number=record.artifact_line_number, global_end_line_number=record.end_global_line, batch_size_bytes=max(record.size_bytes, 1))

def _stream_json_events(*, file_path: str, artifact_format: ArtifactFormat, source_file: str, start_offset: int, start_artifact_line: int, global_line_number: int) -> Iterator[ArtifactEvent]:
    if _is_json_lines(file_path, artifact_format):
        yield from _stream_json_lines(file_path=file_path, artifact_format=artifact_format, source_file=source_file, start_offset=start_offset, start_artifact_line=start_artifact_line, global_line_number=global_line_number)
        return
    yield from _stream_json_document(file_path=file_path, artifact_format=artifact_format, source_file=source_file, skip_records=start_artifact_line, start_offset=start_offset, global_line_number=global_line_number)

def _stream_json_lines(*, file_path: str, artifact_format: ArtifactFormat, source_file: str, start_offset: int, start_artifact_line: int, global_line_number: int) -> Iterator[ArtifactEvent]:
    local_line = start_artifact_line
    global_line = global_line_number
    previous_offset = start_offset
    for line, end_offset in stream_text_lines(file_path, start_offset=start_offset):
        size_bytes = end_offset - previous_offset
        previous_offset = end_offset
        local_line += 1
        global_line += 1
        if start_offset == 0 and local_line == 1:
            line = line.removeprefix('\ufeff')
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            event = normalize_text_event(line, global_line, source_file=source_file, source_format=artifact_format.value) if _generic_text_may_be_important(line) else None
        else:
            data = value if isinstance(value, Mapping) else {'body': value}
            event = normalize_structured_event(data, global_line, source_file=source_file, source_format=artifact_format.value) if structured_event_may_be_important(data) else None
        yield ArtifactEvent(event=event, end_offset=end_offset, artifact_line_number=local_line, global_end_line_number=global_line, batch_size_bytes=max(size_bytes, 1))

def _stream_json_document(*, file_path: str, artifact_format: ArtifactFormat, source_file: str, skip_records: int, start_offset: int, global_line_number: int) -> Iterator[ArtifactEvent]:
    target_prefix = _json_item_prefix(file_path, artifact_format)
    local_record = 0
    global_line = global_line_number
    emitted = False
    try:
        with open(file_path, 'rb') as stream:
            _seek_past_utf8_bom(stream)
            capture: _BoundedStructuredCapture | None = None
            for prefix, event, value in ijson.parse(BoundedJsonStream(stream), buf_size=JSON_STREAM_BUFFER_BYTES, use_float=True):
                if capture is not None:
                    if event in {'start_map', 'start_array'}:
                        capture.depth += 1
                    capture.consume(prefix, event, value)
                    if event in {'end_map', 'end_array'}:
                        capture.depth -= 1
                    if capture.depth != 0:
                        continue
                    data = capture.result()
                    retained_size_bytes = capture.retained_size_bytes
                    capture = None
                elif prefix == target_prefix and event in {'start_map', 'start_array'}:
                    capture = _BoundedStructuredCapture(prefix=prefix)
                    continue
                elif prefix == target_prefix and event in {'string', 'number', 'boolean', 'null'}:
                    data = {'body': value}
                    retained_size_bytes = len(str(value).encode('utf-8', errors='replace'))
                else:
                    continue
                local_record += 1
                if local_record <= skip_records:
                    continue
                global_line += 1
                event = normalize_structured_event(data, global_line, source_file=source_file, source_format=artifact_format.value) if structured_event_may_be_important(data) else None
                emitted = True
                yield ArtifactEvent(event=event, end_offset=0, artifact_line_number=local_record, global_end_line_number=global_line, batch_size_bytes=max(retained_size_bytes, 1))
    except ijson.JSONError:
        has_structured_checkpoint = skip_records > 0 and start_offset == 0
        if emitted or has_structured_checkpoint:
            raise
        logger.warning('Malformed JSON document %s; using generic fallback', file_path)
        yield from _stream_text_events(file_path=file_path, artifact_format=ArtifactFormat.GENERIC, source_file=source_file, start_offset=start_offset, start_artifact_line=skip_records, global_line_number=global_line_number)

def _json_item_prefix(file_path: str, artifact_format: ArtifactFormat) -> str:
    if artifact_format == ArtifactFormat.BROWSER:
        return 'log.entries.item'
    with open(file_path, 'rb') as stream:
        prefix = _json_prefix(stream.read(ARTIFACT_DETECTION_SAMPLE_BYTES))
    if prefix.startswith(b'['):
        return 'item'
    sample = prefix.decode('utf-8-sig', errors='replace')
    collection_match = re.search('"(?P<key>logEvents|records|Records|logs)"\\s*:\\s*\\[', sample)
    if collection_match:
        return f"{collection_match.group('key')}.item"
    return ''

def _is_json_lines(file_path: str, artifact_format: ArtifactFormat) -> bool:
    first_nonempty = ''
    for line, _ in stream_text_lines(file_path):
        candidate = line.removeprefix('\ufeff').strip()
        if candidate:
            first_nonempty = candidate
            break
    if not first_nonempty or first_nonempty.startswith('['):
        return False
    try:
        first_value = json.loads(first_nonempty)
    except json.JSONDecodeError:
        # The bounded reader force-splits at MAX_DIAGNOSTIC_RECORD_BYTES when
        # no newline shows up in time, so an unparseable "first line" here is
        # ambiguous: it could be one truncated fragment of a much larger
        # compact single JSON document (real HAR/CloudWatch exports commonly
        # have zero newlines), or a genuinely oversized JSON-lines record.
        # Guessing JSON-lines used to silently shred the former into garbage
        # text records. Falling through to document mode instead handles
        # both correctly: a real document parses as intended, and a single
        # oversized JSON-lines record is still just one top-level object, so
        # it's captured whole under ijson's '' top-level prefix.
        return False
    if not isinstance(first_value, Mapping):
        return False
    if artifact_format == ArtifactFormat.BROWSER:
        return False
    return not any((isinstance(first_value.get(key), list) for key in ('logEvents', 'records', 'Records', 'logs')))

def _artifact_starts_with_json(file_path: str) -> bool:
    with open(file_path, 'rb') as stream:
        return _json_prefix(stream.read(ARTIFACT_DETECTION_SAMPLE_BYTES)).startswith((b'{', b'['))

def _json_prefix(value: bytes) -> bytes:
    if value.startswith(b'\xef\xbb\xbf'):
        value = value[3:]
    return value.lstrip()

def _seek_past_utf8_bom(stream) -> None:
    prefix = stream.read(3)
    if prefix != b'\xef\xbb\xbf':
        stream.seek(0)

def _structured_canonical_key(relative: str) -> str | None:
    normalized = re.sub('[^a-z0-9]', '', relative.lower())
    leaf = re.sub('[^a-z0-9]', '', relative.rsplit('.', 1)[-1].lower())
    if normalized.endswith(('errormessage', 'exceptionmessage')) or leaf in {'error', 'exception'}:
        return 'exception_message'
    if leaf in {'message', 'msg', 'log', 'body'}:
        return 'message'
    if leaf in {'timestamp', 'time', 'datetime', 'ts', 'eventtime', 'observedtimestamp', 'starteddatetime', 'requesttimeepoch', 'requesttime'}:
        return 'timestamp'
    if leaf in {'level', 'severity', 'severitytext', 'loglevel', 'levelname'}:
        return 'level'
    if leaf in {'traceid'}:
        return 'trace_id'
    if leaf in {'parentspanid'}:
        return 'parent_span_id'
    if leaf in {'spanid'}:
        return 'span_id'
    if leaf in {'requestid', 'correlationid', 'xrequestid', 'awsrequestid'}:
        return 'request_id'
    if normalized.endswith('servicename') or leaf in {'service', 'app', 'component'}:
        return 'service'
    if leaf in {'module', 'logger', 'loggername'}:
        return 'module'
    if normalized.endswith('hostname') or leaf in {'hostname', 'serveripaddress'}:
        return 'host'
    if normalized.endswith(('containerid', 'containername')):
        return 'container'
    if normalized.endswith('podname') or leaf == 'pod':
        return 'pod'
    if normalized.endswith(('errortype', 'exceptiontype')):
        return 'exception_type'
    if normalized.endswith(('requesturl', 'requestpath', 'resourcepath', 'urlpath', 'httproute')) or leaf in {'endpoint', 'route', 'routekey', 'path', 'rawpath', 'url'}:
        return 'endpoint'
    if normalized.endswith(('responsestatus', 'responsestatuscode', 'httpstatus', 'httpstatuscode', 'elbstatuscode', 'targetstatuscode')) or leaf in {'statuscode'}:
        return 'http_status'
    if leaf in {'status', 'state'}:
        return 'status'
    if normalized.endswith('requestmethod') or leaf in {'method', 'httpmethod'}:
        return '_method'
    return None
_STRUCTURED_ALIAS_ORDER = {'message': ('message', 'msg', 'log', '@message', 'body', 'event.message'), 'timestamp': ('timestamp', '@timestamp', 'time', 'datetime', 'ts', 'eventtime', 'observed_timestamp', 'requesttimeepoch', 'request_time_epoch', 'requesttime', 'request_time', 'requestcontext.requesttimeepoch', 'requestcontext.requesttime', 'starteddatetime'), 'level': ('level', 'severity', 'severitytext', 'severity_text', 'loglevel', 'levelname'), 'trace_id': ('trace_id', 'traceid', 'trace.id'), 'span_id': ('span_id', 'spanid', 'span.id'), 'parent_span_id': ('parent_span_id', 'parentspanid', 'span.parent_id'), 'request_id': ('request_id', 'requestid', 'correlation_id', 'correlationid', 'x-request-id', 'awsrequestid', 'requestcontext.requestid'), 'service': ('service.name', 'service_name', 'servicename', 'service', 'app', 'component', 'kubernetes.labels.app', 'kubernetes.container_name'), 'module': ('module', 'logger', 'logger_name', 'loggername', 'scope.name'), 'host': ('host.name', 'hostname', 'host', 'serveripaddress', 'kubernetes.host'), 'container': ('container.id', 'container.name', 'container_id', 'containerid', 'container_name', 'containername', 'container', 'kubernetes.container_name'), 'pod': ('pod', 'pod_name', 'podname', 'kubernetes.pod_name'), 'exception_type': ('exception_type', 'exceptiontype', 'exception.type', 'error.type', 'error.name'), 'exception_message': ('exception_message', 'exceptionmessage', 'exception.message', 'error.message', 'error', 'exception'), 'endpoint': ('url.path', 'endpoint', 'route', 'path', 'url', 'http.route', 'request.url', 'request.path', 'requestcontext.resourcepath', 'resourcepath', 'resource_path', 'rawpath', 'routekey', 'requestcontext.http.path'), 'http_status': ('http_status', 'status_code', 'httpstatus', 'statuscode', 'http.status_code', 'http.statuscode', 'http.response.status_code', 'http.response.statuscode', 'response.status', 'response.statuscode', 'elb_status_code', 'elbstatuscode', 'target_status_code', 'targetstatuscode'), 'status': ('status', 'state'), '_method': ('requestmethod', 'method', 'httpmethod')}

def _structured_alias_priority(canonical_key: str, relative: str) -> int:
    normalized_path = '.'.join((re.sub('[^a-z0-9_@-]', '', part.lower()) for part in relative.split('.')))
    aliases = _STRUCTURED_ALIAS_ORDER.get(canonical_key, ())
    try:
        return aliases.index(normalized_path)
    except ValueError:
        return len(aliases)

def _multiline_kind(artifact_format: ArtifactFormat, line: str) -> str | None:
    lowered = line.lower()
    if 'traceback (most recent call last)' in lowered:
        return 'python_stack'
    if 'exception in thread' in lowered or 'caused by:' in lowered:
        return 'java_stack'
    if 'panic:' in lowered:
        return 'panic_stack'
    if artifact_format == ArtifactFormat.STACK_TRACE and EXCEPTION_PATTERN.match(line.strip()):
        return 'language_stack'
    if artifact_format == ArtifactFormat.DATABASE and line.lstrip().startswith(('# Time:', 'Time:')):
        return 'database_block'
    if artifact_format == ArtifactFormat.STACK_TRACE and any((marker in lowered for marker in ('crash report', 'core dumped', 'fatal signal'))):
        return 'crash_report'
    return None

def _is_multiline_continuation(record: _PendingTextRecord, line: str) -> bool:
    if record.multiline_kind is None:
        return False
    if record.multiline_kind == 'database_block':
        return not line.lstrip().startswith(('# Time:', 'Time:'))
    if record.multiline_kind == 'crash_report':
        return True
    stripped = line.strip()
    if record.multiline_kind == 'python_stack':
        return bool(not stripped or line.startswith((' ', '\t')) or stripped.startswith('File ') or (stripped == 'During handling of the above exception, another exception occurred:') or EXCEPTION_PATTERN.match(stripped))
    if record.multiline_kind == 'java_stack':
        return bool(not stripped or line.startswith((' ', '\t')) or stripped.startswith(('at ', 'Caused by:', 'Suppressed:')))
    if record.multiline_kind == 'language_stack':
        return bool(not stripped or line.startswith((' ', '\t')) or stripped.startswith(('at ', 'File ', 'Caused by:', 'Suppressed:')))
    return bool(not stripped or line.startswith((' ', '\t')) or stripped.startswith(('at ', 'File ', 'Caused by:', 'Suppressed:')) or (stripped == 'During handling of the above exception, another exception occurred:') or EXCEPTION_PATTERN.search(stripped))

def _stack_trace_may_be_important(raw_text: str) -> bool:
    lowered = raw_text.lower()
    if 'crash report' in lowered or 'core dumped' in lowered:
        # _multiline_kind() forces level='ERROR' for these blocks
        # unconditionally, regardless of whether any other marker is
        # present - the generic marker set alone isn't a safe proxy here.
        return True
    return _generic_text_may_be_important(raw_text)

WEB_ACCESS_RE = re.compile('^(?P<host>\\S+)\\s+\\S+\\s+\\S+\\s+\\[(?P<time>[^\\]]+)\\]\\s+"(?P<method>\\S+)\\s+(?P<endpoint>\\S+)(?:\\s+HTTP/[^\\"]+)?"\\s+(?P<status>\\d{3})\\s+')
WEB_ERROR_RE = re.compile('^(?P<time>\\d{4}/\\d{2}/\\d{2}\\s+\\d{2}:\\d{2}:\\d{2})\\s+\\[(?P<level>[^\\]]+)\\]', re.IGNORECASE)
APACHE_ERROR_RE = re.compile('^\\[(?P<time>[A-Z][a-z]{2}\\s+[A-Z][a-z]{2}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?\\s+\\d{4})\\]\\s+\\[[^\\]]*:(?P<level>[^\\]]+)\\]', re.IGNORECASE)

def _web_server_may_be_important(raw_text: str) -> bool:
    access_match = WEB_ACCESS_RE.search(raw_text)
    if access_match:
        return level_from_http_status(int(access_match.group('status'))) in IMPORTANT_LEVELS
    error_match = WEB_ERROR_RE.search(raw_text) or APACHE_ERROR_RE.search(raw_text)
    if error_match:
        return normalize_level(error_match.group('level')) in IMPORTANT_LEVELS
    return _generic_text_may_be_important(raw_text)

def _normalize_web_event(raw_text: str, line_number: int, source_file: str) -> ParsedEvent:
    access_match = WEB_ACCESS_RE.search(raw_text)
    defaults: dict[str, Any] = {}
    if access_match:
        status = int(access_match.group('status'))
        defaults.update(timestamp=access_match.group('time'), host=access_match.group('host'), endpoint=access_match.group('endpoint'), http_status=status, level=level_from_http_status(status))
    else:
        error_match = WEB_ERROR_RE.search(raw_text) or APACHE_ERROR_RE.search(raw_text)
        if error_match:
            defaults['timestamp'] = parse_timestamp(error_match.group('time').replace('/', '-', 2))
            defaults['level'] = normalize_level(error_match.group('level'))
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.WEB_SERVER.value, defaults=defaults)
SYSLOG_5424_RE = re.compile('^<(?P<pri>\\d{1,3})>\\d+\\s+(?P<time>\\S+)\\s+(?P<host>\\S+)\\s+(?P<app>\\S+)\\s+(?P<proc>\\S+)\\s+(?P<msgid>\\S+)\\s+(?P<body>.*)$')
SYSLOG_3164_RE = re.compile('^<(?P<pri>\\d{1,3})>(?P<time>[A-Z][a-z]{2}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2})\\s+(?P<host>\\S+)\\s+(?P<app>[^:\\[]+)(?:\\[\\d+\\])?:\\s*(?P<body>.*)$')

def _syslog_may_be_important(raw_text: str) -> bool:
    match = SYSLOG_5424_RE.search(raw_text) or SYSLOG_3164_RE.search(raw_text)
    if match:
        return _syslog_level(int(match.group('pri')) % 8) in IMPORTANT_LEVELS
    return _generic_text_may_be_important(raw_text)

def _normalize_syslog_event(raw_text: str, line_number: int, source_file: str) -> ParsedEvent:
    match = SYSLOG_5424_RE.search(raw_text) or SYSLOG_3164_RE.search(raw_text)
    defaults: dict[str, Any] = {}
    if match:
        defaults.update(timestamp=match.group('time'), level=_syslog_level(int(match.group('pri')) % 8), host=match.group('host'), service=match.group('app'))
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.SYSLOG.value, defaults=defaults)

def _syslog_level(severity: int) -> str:
    if severity <= 2:
        return 'CRITICAL'
    if severity == 3:
        return 'ERROR'
    if severity == 4:
        return 'WARNING'
    if severity <= 6:
        return 'INFO'
    return 'DEBUG'
CRI_RE = re.compile('^(?P<time>\\S+)\\s+(?P<stream>stdout|stderr)\\s+(?P<flag>[FP])\\s+(?P<body>.*)$', re.DOTALL)

def _container_may_be_important(raw_text: str) -> bool:
    # CRI's own stream marker is always literally " stderr " between the
    # timestamp and flag columns when present. _normalize_container_text_event
    # only inherits level='ERROR' from the stream when the body doesn't
    # already carry its own explicit level, so stderr isn't a guaranteed
    # ERROR outcome by itself - just always worth fully parsing to find out.
    if ' stderr ' in raw_text:
        return True
    return _generic_text_may_be_important(raw_text)

def _normalize_container_text_event(raw_text: str, line_number: int, source_file: str) -> ParsedEvent:
    match = CRI_RE.search(raw_text)
    if not match:
        return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.CONTAINER.value)
    inherited = {'timestamp': match.group('time'), 'level': 'ERROR' if match.group('stream') == 'stderr' else None}
    body = match.group('body')
    try:
        value = json.loads(body)
    except json.JSONDecodeError:
        return normalize_text_event(body, line_number, source_file=source_file, source_format=ArtifactFormat.CONTAINER.value, defaults=inherited)
    data = value if isinstance(value, Mapping) else {'body': value}
    return normalize_structured_event(data, line_number, source_file=source_file, source_format=ArtifactFormat.CONTAINER.value, inherited=inherited)

def _cloud_gateway_may_be_important(raw_text: str) -> bool:
    try:
        fields = shlex.split(raw_text)
    except ValueError:
        fields = []
    if len(fields) > 12 and fields[0].lower() in {'http', 'https', 'h2', 'grpc', 'grpcs'}:
        try:
            status = int(fields[8])
        except (ValueError, IndexError):
            return True  # uncertain shape - don't guess, fully parse
        return level_from_http_status(status) in IMPORTANT_LEVELS
    return _generic_text_may_be_important(raw_text)

def _normalize_cloud_gateway_event(raw_text: str, line_number: int, source_file: str) -> ParsedEvent:
    defaults: dict[str, Any] = {}
    try:
        fields = shlex.split(raw_text)
    except ValueError:
        fields = []
    if len(fields) > 12 and fields[0].lower() in {'http', 'https', 'h2', 'grpc', 'grpcs'}:
        defaults['timestamp'] = fields[1]
        try:
            status = int(fields[8])
        except (ValueError, IndexError):
            status = None
        defaults['http_status'] = status
        defaults['level'] = level_from_http_status(status)
        request_parts = fields[12].split()
        if len(request_parts) >= 2:
            defaults['endpoint'] = request_parts[1]
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.CLOUD_GATEWAY.value, defaults=defaults)

def _cloudfront_row_may_be_important(data: Mapping[str, str]) -> bool:
    status_text = _present_cloudfront_value(data.get('sc-status'))
    if status_text is None:
        return True  # no usable status column - uncertain, fully parse
    try:
        status = int(status_text)
    except ValueError:
        return True
    return level_from_http_status(status) in IMPORTANT_LEVELS

def _normalize_cloudfront_event(raw_text: str, line_number: int, source_file: str, data: Mapping[str, str]) -> ParsedEvent:
    date = _present_cloudfront_value(data.get('date'))
    time = _present_cloudfront_value(data.get('time'))
    timestamp = f'{date}T{time}Z' if date and time else None
    status_text = _present_cloudfront_value(data.get('sc-status'))
    try:
        status = int(status_text) if status_text is not None else None
    except ValueError:
        status = None
    endpoint = _present_cloudfront_value(data.get('cs-uri-stem'))
    query = _present_cloudfront_value(data.get('cs-uri-query'))
    if endpoint and query:
        endpoint = f'{endpoint}?{query}'
    host = _present_cloudfront_value(data.get('cs(host)'))
    if host is None:
        host = _present_cloudfront_value(data.get('x-host-header'))
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.CLOUD_GATEWAY.value, defaults={'timestamp': timestamp, 'level': level_from_http_status(status), 'request_id': _present_cloudfront_value(data.get('x-edge-request-id')), 'service': 'cloudfront', 'host': host, 'endpoint': endpoint, 'http_status': status})

def _present_cloudfront_value(value: str | None) -> str | None:
    if value is None or value == '-':
        return None
    return value
EXPLICIT_SERVICE_RE = re.compile('\\b(?:service(?:[_-]?name)?|app|component)[\\s=:' + '"\'' + ']+', re.IGNORECASE)
LAMBDA_LIFECYCLE_RE = re.compile('^(?:START|END|REPORT)\\s+RequestId:\\s*(?P<request_id>\\S+)', re.IGNORECASE)
LAMBDA_APPLICATION_RE = re.compile('^(?P<time>\\d{4}-\\d{2}-\\d{2}T\\S+)\\s+(?P<request_id>[A-Za-z0-9-]+)\\s+(?P<level>TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|SEVERE|FATAL|CRITICAL|ALERT|EMERG(?:ENCY)?)\\b', re.IGNORECASE)

def _normalize_message_broker_event(raw_text: str, line_number: int, source_file: str) -> ParsedEvent:
    defaults: dict[str, Any] = {}
    if EXPLICIT_SERVICE_RE.search(raw_text) is None:
        lowered = raw_text.lower()
        if 'rabbitmq' in lowered or 'amqp' in lowered:
            defaults['service'] = 'rabbitmq'
        elif 'kafka' in lowered:
            defaults['service'] = 'kafka'
        else:
            defaults['service'] = 'message-broker'
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.MESSAGE_BROKER.value, defaults=defaults)

def _serverless_may_be_important(raw_text: str) -> bool:
    if LAMBDA_LIFECYCLE_RE.search(raw_text):
        return False  # START/END/REPORT lines are always level='INFO'
    application = LAMBDA_APPLICATION_RE.search(raw_text)
    if application:
        return normalize_level(application.group('level')) in IMPORTANT_LEVELS
    return _generic_text_may_be_important(raw_text)

def _normalize_serverless_text_event(raw_text: str, line_number: int, source_file: str) -> ParsedEvent:
    defaults: dict[str, Any] = {}
    lifecycle = LAMBDA_LIFECYCLE_RE.search(raw_text)
    application = LAMBDA_APPLICATION_RE.search(raw_text)
    if lifecycle:
        defaults.update(request_id=lifecycle.group('request_id'), level='INFO')
    elif application:
        defaults.update(timestamp=application.group('time'), request_id=application.group('request_id'), level=application.group('level'))
    if EXPLICIT_SERVICE_RE.search(raw_text) is None:
        defaults['service'] = 'aws-lambda'
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.SERVERLESS.value, defaults=defaults)
