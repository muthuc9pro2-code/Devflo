import re
from collections.abc import Mapping
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any
from .event_filter import IMPORTANT_LEVELS
from .log_praser import ParsedEvent, StackFrame
TIMESTAMP_PATTERN = re.compile('\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?(?:Z|[+-]\\d{2}:?\\d{2})?\\b')
LOG_LEVEL_PATTERN = re.compile('(?<![A-Za-z])(TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|SEVERE|FATAL|CRITICAL|ALERT|EMERG(?:ENCY)?)(?![A-Za-z])', re.IGNORECASE)
TRACE_ID_PATTERN = re.compile('\\b(?:trace[_-]?id|traceid)[\\s=:' + '"\'' + ']+([A-Za-z0-9_-]+)', re.IGNORECASE)
SPAN_ID_PATTERN = re.compile('\\b(?:span[_-]?id|spanid)[\\s=:' + '"\'' + ']+([A-Za-z0-9_-]+)', re.IGNORECASE)
PARENT_SPAN_ID_PATTERN = re.compile('\\b(?:parent[_-]?span[_-]?id|parentspanid)[\\s=:' + '"\'' + ']+([A-Za-z0-9_-]+)', re.IGNORECASE)
REQUEST_ID_PATTERN = re.compile('\\b(?:request[_-]?id|requestid|correlation[_-]?id|x-request-id)[\\s=:' + '"\'' + ']+([A-Za-z0-9_.:/-]+)', re.IGNORECASE)
SERVICE_PATTERN = re.compile('\\b(?:service(?:[_-]?name)?|app|component)[\\s=:' + '"\'' + ']+([A-Za-z0-9_.-]+)', re.IGNORECASE)
MODULE_PATTERN = re.compile('\\b(?:module|logger)[\\s=:' + '"\'' + ']+([A-Za-z0-9_.:/-]+)', re.IGNORECASE)
HOST_PATTERN = re.compile('\\b(?:host(?:name)?|node)[\\s=:' + '"\'' + ']+([A-Za-z0-9_.:-]+)', re.IGNORECASE)
CONTAINER_PATTERN = re.compile('\\b(?:container(?:[_-]?(?:id|name))?)[\\s=:' + '"\'' + ']+([A-Za-z0-9_.:/-]+)', re.IGNORECASE)
POD_PATTERN = re.compile('\\b(?:pod(?:[_-]?name)?)[\\s=:' + '"\'' + ']+([A-Za-z0-9_.-]+)', re.IGNORECASE)
HTTP_STATUS_PATTERN = re.compile('\\b(?:status|status_code|http_status)[\\s=:' + '"\'' + ']+(\\d{3})\\b', re.IGNORECASE)
ENDPOINT_PATTERN = re.compile('(?:\\b(?:endpoint|route|path|url)[\\s=:' + '"\'' + ']+)(https?://[^\\s\\"\']+|/[^\\s\\"\']*)', re.IGNORECASE)
HTTP_REQUEST_PATTERN = re.compile('\\b(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\\s+(\\S+)', re.IGNORECASE)
DIAGNOSTIC_FIELD_PATTERN = re.compile('\\b(?P<key>[A-Za-z][A-Za-z0-9_-]*)\\b[\\"\']?\\s*(?:=|:)\\s*[\\"\']?(?P<value>[A-Za-z0-9_./:@?&={}-]+)')
SPACE_DIAGNOSTIC_FIELD_PATTERN = re.compile('\\b(?P<key>parent[_-]?span[_-]?id|trace[_-]?id|span[_-]?id|request[_-]?id|correlation[_-]?id|x-request-id|service(?:[_-]?name)?|app|component|module|logger|host(?:name)?|node|container(?:[_-]?(?:id|name))?|pod(?:[_-]?name)?|status(?:[_-]?code)?|http[_-]?status|endpoint|route|path|url)\\s+(?P<value>[A-Za-z0-9_./:@?&={}-]+)', re.IGNORECASE)
EXCEPTION_PATTERN = re.compile('\\b([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception|Failure))\\b(?::\\s*(.*))?')
PYTHON_FRAME_PATTERN = re.compile('^\\s*File "([^"]+)", line (\\d+), in (.+)$', re.MULTILINE)
JAVA_FRAME_PATTERN = re.compile('^\\s*at\\s+(?:(.+?)\\()?([^():]+):(\\d+)\\)?$', re.MULTILINE)
NODE_FRAME_PATTERN = re.compile('^\\s*at\\s+(?:(.+?)\\s+\\()?([^():]+):(\\d+):\\d+\\)?$', re.MULTILINE)
# Conservative, language-agnostic "path/to/file.ext:LINE[:COLUMN]" or
# .NET-style "file.ext:line LINE" fallback - covers common diagnostics from
# Go, Rust, Ruby, .NET, C/C++ and other runtimes that don't use any of the
# three explicit conventions above, without building a parser per language.
# Deliberately loose (e.g. "host.example.com:8080" can match syntactically)
# - this only ever produces a StackFrame CANDIDATE; source_index.py's
# _match_frame() still has to resolve the path unambiguously against the
# real indexed source tree before Devflo claims an actual source match, so
# a loose regex here can never fabricate one on its own.
GENERIC_SOURCE_LOCATION_PATTERN = re.compile(
    r'(?P<path>[\w./\\-]+\.[A-Za-z][A-Za-z0-9]{0,9})'
    r':(?:line\s+)?(?P<line>\d+)(?::(?P<column>\d+))?',
    re.IGNORECASE,
)
# Fast substring-scale pre-check for _classify_text's _STACK feature gate -
# "does this text even contain a '.ext:digit'-shaped substring anywhere",
# cheaper than running the full named-group pattern above on every record.
_LOOSE_SOURCE_LOCATION_HINT = re.compile(r'\.[A-Za-z]\w{0,9}:\d')
LEVEL_ALIASES = {'WARN': 'WARNING', 'WARNING': 'WARNING', 'ERR': 'ERROR', 'ERROR': 'ERROR', 'SEVERE': 'CRITICAL', 'FATAL': 'CRITICAL', 'CRITICAL': 'CRITICAL', 'ALERT': 'CRITICAL', 'EMERG': 'CRITICAL', 'EMERGENCY': 'CRITICAL', 'NOTICE': 'INFO'}
FIELD_ALIASES = {'traceid': 'trace_id', 'spanid': 'span_id', 'parentspanid': 'parent_span_id', 'requestid': 'request_id', 'correlationid': 'request_id', 'xrequestid': 'request_id', 'service': 'service', 'servicename': 'service', 'app': 'service', 'component': 'service', 'module': 'module', 'logger': 'module', 'host': 'host', 'hostname': 'host', 'node': 'host', 'container': 'container', 'containerid': 'container', 'containername': 'container', 'pod': 'pod', 'podname': 'pod', 'status': 'http_status', 'statuscode': 'http_status', 'httpstatus': 'http_status', 'endpoint': 'endpoint', 'route': 'endpoint', 'path': 'endpoint', 'url': 'endpoint'}
_TEXT_FIELDS = ('trace_id', 'request_id', 'service', 'module', 'endpoint', 'span_id', 'parent_span_id', 'host', 'container', 'pod')
_FIELD_MARKERS = ('trace', 'span', 'request', 'correlation', 'service', 'app', 'component', 'module', 'logger', 'host', 'node', 'container', 'pod', 'status', 'endpoint', 'route', 'path', 'url')
_SPACE_FIELD_MARKERS = ('trace', 'span', 'request', 'correlation', 'service ', 'module ', 'logger ', 'host ', 'container ', 'pod ', 'status ', 'endpoint ')
_LEVEL_MARKERS = ('trace', 'debug', 'info', 'notice', 'warn', 'error', 'err', 'severe', 'fatal', 'critical', 'alert', 'emerg')
_FIELDS, _SPACE_FIELDS, _LEVEL, _EXCEPTION, _HTTP, _FATAL, _TIMESTAMP, _STACK, _SLOW = (1 << bit for bit in range(9))
_NON_ALNUM_PATTERN = re.compile('[^a-z0-9]')
_ALL_CANONICAL_FIELD_KEYS = frozenset(FIELD_ALIASES.values())
# Folds a matched key to the same form FIELD_ALIASES is keyed on (lowercase,
# '_'/'-' stripped) in a single str.translate() pass instead of
# .lower().replace('_', '').replace('-', '') (three full-string passes).
_KEY_FOLD_TABLE = {ord(c): ord(c.lower()) for c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'}
_KEY_FOLD_TABLE[ord('_')] = None
_KEY_FOLD_TABLE[ord('-')] = None

def normalize_level(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        number = _safe_int(value)
        if number is None:
            return None
        if number >= 50:
            return 'CRITICAL'
        if number >= 40:
            return 'ERROR'
        if number >= 30:
            return 'WARNING'
        if number >= 20:
            return 'INFO'
        return 'DEBUG'
    normalized = str(value).strip().upper()
    return LEVEL_ALIASES.get(normalized, normalized or None)

def normalize_otel_severity(value: Any) -> str | None:
    if value is None:
        return None
    is_numeric = isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit())
    if not is_numeric:
        return normalize_level(value)
    number = _safe_int(value)
    if number is None or number <= 0:
        return None
    if number >= 21:
        return 'CRITICAL'
    if number >= 17:
        return 'ERROR'
    if number >= 13:
        return 'WARNING'
    if number >= 9:
        return 'INFO'
    if number >= 5:
        return 'DEBUG'
    return 'TRACE'

def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) or str(value).strip().isdigit():
        number = _safe_int(value)
        if number is None:
            return None
        try:
            magnitude = abs(number)
            if magnitude >= 100000000000000000:
                seconds = number / 1000000000
            elif magnitude >= 100000000000000:
                seconds = number / 1000000
            elif magnitude >= 100000000000:
                seconds = number / 1000
            else:
                seconds = number
            parsed = datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value).strip().strip('[]')
        if text.endswith('Z'):
            text = f'{text[:-1]}+00:00'
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = None
        if parsed is None:
            for date_format in ('%d/%b/%Y:%H:%M:%S %z', '%b %d %H:%M:%S', '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%a %b %d %H:%M:%S.%f %Y', '%a %b %d %H:%M:%S %Y'):
                try:
                    parsed = datetime.strptime(text, date_format)
                    if date_format == '%b %d %H:%M:%S':
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
        return 'ERROR'
    if status >= 400:
        return 'WARNING'
    return 'INFO'

def _match_group(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    return match.group(1) if match else None

def _parse_stack_frames(raw_text: str) -> list[StackFrame]:
    frames = []
    if 'File "' in raw_text:
        for match in PYTHON_FRAME_PATTERN.finditer(raw_text):
            # A well-formed Python traceback frame line ("File ..., line N,
            # in FUNC") is always immediately followed by its source-code
            # snippet line - that is how every real traceback (Python's own
            # formatter, always) writes one. A frame with nothing at all
            # after it in the record only happens when the record itself
            # was cut short before that snippet line ever arrived (e.g. the
            # underlying artifact's content ends mid-traceback) - the
            # function name captured at that point cannot be trusted as
            # complete, so it is dropped rather than persisted truncated.
            # NODE_FRAME_PATTERN/JAVA_FRAME_PATTERN frames below are exempt:
            # their one-line-per-frame convention has no such "next line"
            # requirement, so being the record's last line is normal there.
            if not raw_text[match.end():].strip():
                continue
            file, line, function = match.group(1), match.group(2), match.group(3)
            frames.append(StackFrame(file=file, line=int(line), function=function))
    if ' at ' in raw_text or raw_text.lstrip().startswith('at '):
        for pattern in (NODE_FRAME_PATTERN, JAVA_FRAME_PATTERN):
            for function, file, line in pattern.findall(raw_text):
                frames.append(StackFrame(file=file, line=int(line), function=function or None))
    if not frames:
        # Explicit Python/JVM/Node parsers above take precedence; only
        # fall back to the generic path:line[:column] shape when none of
        # them found anything. function is always None here - a bare
        # path:line carries no function name, and inventing one would be
        # exactly the fabrication this must not do.
        for match in GENERIC_SOURCE_LOCATION_PATTERN.finditer(raw_text):
            frames.append(
                StackFrame(
                    file=match.group('path'),
                    line=int(match.group('line')),
                    function=None,
                )
            )
    return frames

def _classify_text(raw_text: str) -> tuple[str, int]:
    lowered = raw_text.lower()
    features = (
        (_FIELDS if any(marker in lowered for marker in _FIELD_MARKERS) else 0)
        | (_SPACE_FIELDS if any(marker in lowered for marker in _SPACE_FIELD_MARKERS) else 0)
        | (_LEVEL if any(marker in lowered for marker in _LEVEL_MARKERS) else 0)
        | (_EXCEPTION if any(marker in lowered for marker in ('error', 'exception', 'failure')) else 0)
        | (_HTTP if any(marker in lowered for marker in ('get ', 'post ', 'put ', 'patch ', 'delete ', 'head ', 'options ')) else 0)
        | (_FATAL if any(marker in lowered for marker in ('traceback', 'panic:', 'segmentation fault', 'fatal:')) else 0)
    )
    newline_index = raw_text.find('\n')
    head = raw_text if newline_index == -1 else raw_text[:newline_index]

    if '-' in head and ':' in head:
        features |= _TIMESTAMP
    if (
        '\n' in raw_text
        or raw_text.lstrip().startswith(('at ', 'File '))
        # Cheap pre-check for a single-line generic "file.ext:N" shape
        # (Go/Rust/Ruby/.NET/C/C++ etc.) - only a fast substring-scale
        # regex, not the full named-group GENERIC_SOURCE_LOCATION_PATTERN,
        # so records with no ".x:digit" shape at all skip stack-frame
        # parsing exactly as cheaply as before.
        or _LOOSE_SOURCE_LOCATION_HINT.search(raw_text)
    ):
        features |= _STACK
    if 'slow query' in lowered: features |= _SLOW
    return lowered, features

_SPACED_OPERATOR_RE = re.compile('[=:]\\s|\\s[=:]')
_HTTP_VERBS = frozenset({'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'})

def fast_path_prefixed_event(raw_text: str, line_number: int, *, source_file: str | None=None, source_format: str | None=None) -> ParsedEvent | None:
    """True single-pass tokenizer for single-line 'TIMESTAMP LEVEL key=value ...' records.

    Recognizes the timestamp/level prefix structurally (two short anchored
    matches instead of the two full-text TIMESTAMP_PATTERN/LOG_LEVEL_PATTERN
    searches normalize_text_event() pays for), then walks the remainder
    ONCE - split into whitespace-delimited tokens - extracting every field,
    the exception marker, and the HTTP-verb-derived endpoint in that same
    traversal. That replaces _classify_text() (a full-line .lower() plus
    ~50 substring checks) and _extract_diagnostic_fields()'s two whole-line
    regex passes entirely for this shape.

    Each candidate token is still handed to the real
    DIAGNOSTIC_FIELD_PATTERN/SPACE_DIAGNOSTIC_FIELD_PATTERN plus the shared
    _store_diagnostic_field() helper - just scoped to that one short token
    instead of the whole line - so the character-class-level extraction
    semantics stay byte-for-byte identical to the full parser; only the
    traversal that finds candidates changes.

    Only ever called where defaults would be empty, so this only needs to
    reproduce normalize_text_event(raw_text, line_number, defaults={}).

    Returns None whenever equivalence isn't cheaply guaranteed; callers must
    fall back to normalize_text_event in that case.
    """
    if '\n' in raw_text:
        return None

    ts_token, sep, remainder = raw_text.partition(' ')
    if not sep:
        return None
    level_token, sep, rest = remainder.partition(' ')
    if not sep:
        return None

    ts_match = TIMESTAMP_PATTERN.match(ts_token)
    if ts_match is None or ts_match.end() != len(ts_token):
        return None

    level_match = LOG_LEVEL_PATTERN.match(level_token)
    if level_match is None or level_match.end() != len(level_token):
        return None

    if _SPACED_OPERATOR_RE.search(rest) is not None:
        # A '='/':' with whitespace on either side could let a field match
        # span what we're about to treat as a token boundary (e.g.
        # "key: value" or "key =value") - the per-token scan below can't see
        # across that boundary, so hand the whole record to the full parser
        # instead of risking a field that's there but goes unfound.
        return None

    tokens = rest.split()
    n_tokens = len(tokens)
    fields: dict[str, str] = {}
    endpoint_from_http = None
    has_exception_marker = False
    consumed_as_value = -1
    # _SPACE_FIELD_MARKERS (the real parser's gate for even attempting
    # SPACE_DIAGNOSTIC_FIELD_PATTERN) doesn't cover every alias
    # SPACE_DIAGNOSTIC_FIELD_PATTERN's own alternation supports - e.g. bare
    # "route"/"path"/"url"/"app"/"component"/"node", or "servicename"/
    # "containerid"-style compounds, never satisfy it on their own. Folding
    # a token to a FIELD_ALIASES hit is a superset of that gate, so it's
    # only a safe pre-filter once also checked against the real gate -
    # computed at most once per record, only if a candidate token shows up.
    space_fields_enabled: bool | None = None

    for i, token in enumerate(tokens):
        if not has_exception_marker:
            lowered_token = token.lower()
            if 'error' in lowered_token or 'exception' in lowered_token or 'failure' in lowered_token:
                has_exception_marker = True

        if i == consumed_as_value:
            continue

        if '=' in token or ':' in token:
            for match in DIAGNOSTIC_FIELD_PATTERN.finditer(token):
                _store_diagnostic_field(fields, match, None)
            continue

        # Bare "key value" form (no '='/':'): only worth an actual regex
        # attempt when this token could plausibly BE one of the recognized
        # field names at all - folding is a strict superset of what
        # SPACE_DIAGNOSTIC_FIELD_PATTERN's alternation accepts, so this
        # never skips an attempt the real pattern would have wanted.
        normalized_key = token.translate(_KEY_FOLD_TABLE)
        if normalized_key in FIELD_ALIASES and i + 1 < n_tokens:
            if space_fields_enabled is None:
                # level_token itself can coincidentally contain a marker
                # (e.g. level "TRACE" contains the bare 'trace' marker) even
                # though ts_token structurally never can, so it has to be
                # included in what gets scanned here too.
                lowered_scan = f'{level_token} {rest}'.lower()
                space_fields_enabled = any(
                    marker in lowered_scan for marker in _SPACE_FIELD_MARKERS
                )
            if space_fields_enabled:
                combined = SPACE_DIAGNOSTIC_FIELD_PATTERN.match(f'{token} {tokens[i + 1]}')
                if combined is not None:
                    _store_diagnostic_field(fields, combined, None)
                    consumed_as_value = i + 1
                    continue

        if endpoint_from_http is None and i + 1 < n_tokens and token.upper() in _HTTP_VERBS:
            endpoint_from_http = tokens[i + 1]

    exception_type = None
    exception_message = None
    exception_match = EXCEPTION_PATTERN.search(raw_text) if has_exception_marker else None
    if exception_match is not None:
        exception_type = exception_match.group(1).split('.')[-1]
        exception_message = exception_match.group(2)

    endpoint = fields.get('endpoint')
    if endpoint is None:
        endpoint = endpoint_from_http

    return ParsedEvent(
        line_number=line_number,
        raw_line=raw_text,
        timestamp=parse_timestamp(ts_token),
        level=normalize_level(level_token),
        trace_id=fields.get('trace_id'),
        request_id=fields.get('request_id'),
        service=fields.get('service'),
        module=fields.get('module'),
        exception_type=exception_type,
        exception_message=exception_message,
        stack_frames=[],
        endpoint=endpoint,
        http_status=_safe_int(fields.get('http_status')),
        source_file=source_file,
        span_id=fields.get('span_id'),
        parent_span_id=fields.get('parent_span_id'),
        host=fields.get('host'),
        container=fields.get('container'),
        pod=fields.get('pod'),
        source_format=source_format,
    )

def _normalize_fully_defaulted_event(raw_text: str, line_number: int, *, source_file: str | None, source_format: str | None, text_defaults: dict[str, str | None], status: int | None, resolved_level: str, default_timestamp: Any, exception_type: str | None, exception_message: str | None) -> ParsedEvent:
    if exception_type is None or exception_message is None:
        lowered_text = raw_text.lower()
        if 'error' in lowered_text or 'exception' in lowered_text or 'failure' in lowered_text:
            if '\n' in raw_text:
                exception_match = None
                for exception_match in EXCEPTION_PATTERN.finditer(raw_text):
                    pass
            else:
                exception_match = EXCEPTION_PATTERN.search(raw_text)
            if exception_match is not None:
                exception_type = exception_type or exception_match.group(1).split('.')[-1]
                exception_message = exception_message or exception_match.group(2)

    stack_frames = []
    if (
        '\n' in raw_text
        or raw_text.lstrip().startswith(('at ', 'File '))
        or _LOOSE_SOURCE_LOCATION_HINT.search(raw_text)
    ):
        stack_frames = _parse_stack_frames(raw_text)

    return ParsedEvent(
        line_number=line_number,
        raw_line=raw_text,
        timestamp=parse_timestamp(default_timestamp),
        level=resolved_level,
        trace_id=text_defaults.get('trace_id'),
        request_id=text_defaults.get('request_id'),
        service=text_defaults.get('service'),
        module=text_defaults.get('module'),
        exception_type=exception_type,
        exception_message=exception_message,
        stack_frames=stack_frames,
        endpoint=text_defaults.get('endpoint'),
        http_status=status,
        source_file=source_file,
        span_id=text_defaults.get('span_id'),
        parent_span_id=text_defaults.get('parent_span_id'),
        host=text_defaults.get('host'),
        container=text_defaults.get('container'),
        pod=text_defaults.get('pod'),
        source_format=source_format,
    )

def normalize_text_event(raw_text: str, line_number: int, *, source_file: str | None=None, source_format: str | None=None, defaults: Mapping[str, Any] | None=None) -> ParsedEvent:
    defaults = defaults or {}
    text_defaults = {key: _as_text(defaults.get(key)) for key in _TEXT_FIELDS} if defaults else {}
    status = _safe_int(defaults.get('http_status'))
    missing = {key for key, value in text_defaults.items() if value is None} if defaults else None
    if missing is not None and status is None:
        missing.add('http_status')

    if defaults and missing is not None and not missing:
        resolved_level = normalize_level(defaults.get('level')) if defaults.get('level') else None
        default_timestamp = defaults.get('timestamp')
        if resolved_level is not None and default_timestamp:
            # Every _TEXT_FIELDS/http_status value defaults already supplies,
            # plus a settled timestamp and level: nothing
            # _extract_diagnostic_fields() could still find is wanted, the
            # timestamp/level searches are moot (defaults already won them),
            # and the exception-match guard below is exactly
            # `exception_type is None or exception_message is None` (its
            # third clause, `not defaults.get('level')`, is already false) -
            # so classify_text()'s full lower()+6-marker-group scan and both
            # DIAGNOSTIC_FIELD_PATTERN passes are all guaranteed no-ops here.
            # Only stack-frame detection doesn't depend on defaults at all,
            # and is cheap to check directly without classify_text().
            return _normalize_fully_defaulted_event(
                raw_text,
                line_number,
                source_file=source_file,
                source_format=source_format,
                text_defaults=text_defaults,
                status=status,
                resolved_level=resolved_level,
                default_timestamp=default_timestamp,
                exception_type=_as_text(defaults.get('exception_type')),
                exception_message=_as_text(defaults.get('exception_message')),
            )

    lower_text, features = _classify_text(raw_text)
    fields = _extract_diagnostic_fields(raw_text, lower_text, missing, features)
    timestamp_match = TIMESTAMP_PATTERN.search(raw_text) if not defaults.get('timestamp') and features & _TIMESTAMP else None
    level_match = LOG_LEVEL_PATTERN.search(raw_text) if not defaults.get('level') and features & _LEVEL else None
    exception_type = _as_text(defaults.get('exception_type'))
    exception_message = _as_text(defaults.get('exception_message'))
    exception_match = None
    if features & _EXCEPTION and (exception_type is None or exception_message is None or not defaults.get('level')):
        if '\n' in raw_text:
            for exception_match in EXCEPTION_PATTERN.finditer(raw_text):
                pass
        else:
            exception_match = EXCEPTION_PATTERN.search(raw_text)
    endpoint = fields.get('endpoint')
    if text_defaults.get('endpoint') is None and endpoint is None and features & _HTTP:
        endpoint = _match_group(HTTP_REQUEST_PATTERN, raw_text)
    if status is None:
        status = _safe_int(fields.get('http_status'))
    level = normalize_level(defaults.get('level') or (level_match.group(1) if level_match else None))
    if level is None and (exception_match is not None or features & _FATAL):
        level = 'ERROR'
    elif level is None and features & _SLOW:
        level = 'WARNING'
    elif level is None:
        level = level_from_http_status(status)
    if exception_match is not None:
        exception_type = exception_type or exception_match.group(1).split('.')[-1]
        exception_message = exception_message or exception_match.group(2)
    stack_frames = []
    if features & _STACK:
        stack_frames = _parse_stack_frames(raw_text)
    return ParsedEvent(line_number=line_number, raw_line=raw_text, timestamp=parse_timestamp(defaults.get('timestamp') or (timestamp_match.group(0) if timestamp_match else None)), level=level, trace_id=text_defaults.get('trace_id') or fields.get('trace_id'), request_id=text_defaults.get('request_id') or fields.get('request_id'), service=text_defaults.get('service') or fields.get('service'), module=text_defaults.get('module') or fields.get('module'), exception_type=exception_type, exception_message=exception_message, stack_frames=stack_frames, endpoint=text_defaults.get('endpoint') or endpoint, http_status=status, source_file=source_file, span_id=text_defaults.get('span_id') or fields.get('span_id'), parent_span_id=text_defaults.get('parent_span_id') or fields.get('parent_span_id'), host=text_defaults.get('host') or fields.get('host'), container=text_defaults.get('container') or fields.get('container'), pod=text_defaults.get('pod') or fields.get('pod'), source_format=source_format)

def normalize_structured_event(data: Mapping[str, Any], line_number: int, *, source_file: str | None=None, source_format: str | None=None, inherited: Mapping[str, Any] | None=None, diagnostic_attributes: dict[str, Any] | None=None) -> ParsedEvent:
    inherited = inherited or {}
    key_cache: dict[int, dict[str, Any]] = {}
    def first(*paths: tuple[str, ...]) -> Any:
        return _first_value(data, paths, key_cache)
    exception_type = first(('exception_type',), ('exception', 'type'), ('error', 'type'), ('error', 'name'))
    exception_message = first(('exception_message',), ('exceptionMessage',), ('exception', 'message'), ('error', 'message'), ('error',), ('exception',))
    message_value = first(('message',), ('msg',), ('log',), ('@message',), ('body',), ('event', 'message'), ('error', 'message'), ('exception', 'message'))
    message = _value_text(message_value)
    if message is None:
        message = _value_text(exception_message) or _value_text(first(('name',))) or 'structured event'
    status = _safe_int(first(('http_status',), ('status_code',), ('statusCode',), ('http', 'status_code'), ('http', 'response', 'status_code'), ('response', 'status'), ('response', 'statusCode'), ('elb_status_code',), ('target_status_code',), ('status',)))
    level_value = first(('level',), ('severity',), ('severityText',), ('severity_text',), ('logLevel',), ('levelname',))
    if level_value is None:
        status_text = first(('status',), ('state',))
        if isinstance(status_text, str) and status_text.lower() in {'warn', 'warning', 'error', 'failed', 'failure', 'fatal', 'critical'}:
            level_value = 'ERROR' if status_text.lower() in {'failed', 'failure'} else status_text
    if level_value is None and first(('stream',)) == 'stderr':
        level_value = 'ERROR'
    if level_value is None and (_as_text(exception_type) is not None or _as_text(exception_message) is not None):
        level_value = 'ERROR'
    defaults = {'timestamp': first(('timestamp',), ('@timestamp',), ('time',), ('datetime',), ('ts',), ('eventTime',), ('observed_timestamp',), ('requestTimeEpoch',), ('request_time_epoch',), ('requestTime',), ('request_time',), ('requestContext', 'requestTimeEpoch'), ('requestContext', 'requestTime')), 'level': level_value, 'trace_id': first(('trace_id',), ('traceId',), ('trace', 'id')), 'span_id': first(('span_id',), ('spanId',), ('span', 'id')), 'parent_span_id': first(('parent_span_id',), ('parentSpanId',), ('span', 'parent_id')), 'request_id': first(('request_id',), ('requestId',), ('correlation_id',), ('correlationId',), ('x-request-id',), ('awsRequestId',), ('requestContext', 'requestId')), 'service': first(('service', 'name'), ('service_name',), ('service',), ('app',), ('component',), ('kubernetes', 'labels', 'app'), ('kubernetes', 'container_name')) or inherited.get('service'), 'module': first(('module',), ('logger',), ('logger_name',), ('scope', 'name')) or inherited.get('module'), 'host': first(('host', 'name'), ('hostname',), ('host',), ('serverIPAddress',), ('kubernetes', 'host')) or inherited.get('host'), 'container': first(('container', 'id'), ('container', 'name'), ('container_id',), ('container_name',), ('container',), ('kubernetes', 'container_name')) or inherited.get('container'), 'pod': first(('pod',), ('pod_name',), ('kubernetes', 'pod_name')) or inherited.get('pod'), 'endpoint': first(('url', 'path'), ('endpoint',), ('route',), ('path',), ('url',), ('http', 'route'), ('request', 'url'), ('request', 'path'), ('requestContext', 'resourcePath'), ('resourcePath',), ('resource_path',), ('rawPath',), ('routeKey',), ('requestContext', 'http', 'path')), 'http_status': status, 'exception_type': exception_type, 'exception_message': exception_message}
    for key, value in inherited.items():
        if defaults.get(key) is None:
            defaults[key] = value
    event = normalize_text_event(raw_text=message, line_number=line_number, source_file=source_file, source_format=source_format, defaults=defaults)
    if event.level is None:
        event.level = level_from_http_status(status)
    event.diagnostic_attributes = diagnostic_attributes
    return event

def structured_event_may_be_important(data: Mapping[str, Any], *, inherited: Mapping[str, Any] | None=None) -> bool:
    """Cheap pre-check mirroring normalize_structured_event()'s level
    resolution, so a definitely-unimportant structured record (e.g. an
    ordinary status=200 access-log-style JSON entry) doesn't have to pay for
    building its full ~15-alias defaults dict and running normalize_text_event
    on its message just to be discarded a moment later.

    Only returns False when the record is PROVABLY unimportant: none of the
    real signals below fired, AND an explicit level/severity field (when
    present) resolves to something non-important. A producer's own level
    label is not trusted blindly - a record explicitly labeled "INFO" that
    still carries a real exception/error type, an "stderr" stream, an
    error-shaped status/state string, or an HTTP 4xx/5xx status is exactly
    the "producer mislabeled a real failure" case is_evidence_worthy()
    (event_filter.py) exists to catch, so those signals are checked before
    ever trusting an unimportant explicit level (previously: an explicit
    level short-circuited every other check, so
    {"level":"INFO","exception":{"type":"ConnectionError", ...}} was
    treated as provably unimportant and never even reached the full
    parser). Any other shape - no explicit level, no usable status, or an
    exception already implying ERROR - returns True and the record gets
    fully parsed, exactly like today.
    """
    inherited = inherited or {}
    key_cache: dict[int, dict[str, Any]] = {}

    def first(*paths: tuple[str, ...]) -> Any:
        return _first_value(data, paths, key_cache)

    level_value = first(('level',), ('severity',), ('severityText',), ('severity_text',), ('logLevel',), ('levelname',))
    if level_value is not None and normalize_level(level_value) in IMPORTANT_LEVELS:
        return True

    exception_type = first(('exception_type',), ('exception', 'type'), ('error', 'type'), ('error', 'name'))
    exception_message = first(('exception_message',), ('exceptionMessage',), ('exception', 'message'), ('error', 'message'), ('error',), ('exception',))
    if _as_text(exception_type) is not None or _as_text(exception_message) is not None:
        return True

    if first(('stream',)) == 'stderr':
        return True

    status_text = first(('status',), ('state',))
    if isinstance(status_text, str) and status_text.lower() in {'warn', 'warning', 'error', 'failed', 'failure', 'fatal', 'critical'}:
        return True

    status = _safe_int(first(('http_status',), ('status_code',), ('statusCode',), ('http', 'status_code'), ('http', 'response', 'status_code'), ('response', 'status'), ('response', 'statusCode'), ('elb_status_code',), ('target_status_code',), ('status',)))
    if status is not None and level_from_http_status(status) in IMPORTANT_LEVELS:
        return True

    if level_value is not None:
        # A real, explicit level that is not itself important, and none of
        # the real signals above fired either - trust the producer's level.
        return False

    if status is None:
        return True  # nothing resolved at all - uncertain, fully parse

    message_value = first(('message',), ('msg',), ('log',), ('@message',), ('body',), ('event', 'message'), ('error', 'message'), ('exception', 'message'))
    message_text = (_value_text(message_value) or '').lower()
    return any(marker in message_text for marker in _LEVEL_MARKERS)

def _first_value(data: Mapping[str, Any], paths: tuple[tuple[str, ...], ...], key_cache: dict[int, dict[str, Any]] | None=None) -> Any:
    key_cache = key_cache if key_cache is not None else {}
    for path in paths:
        current: Any = data
        for part in path:
            if not isinstance(current, Mapping):
                current = None
                break
            if part in current:
                current = current[part]
                continue
            cache_key = id(current)
            normalized_keys = key_cache.get(cache_key)
            if normalized_keys is None:
                normalized_keys = {}
                for key in current:
                    normalized_keys.setdefault(_normalized_key(str(key)), key)
                key_cache[cache_key] = normalized_keys
            matching_key = normalized_keys.get(_normalized_key(part))
            if matching_key is None:
                current = None
                break
            current = current[matching_key]
        if current is not None:
            return current
    return None

def _extract_diagnostic_fields(raw_text: str, lowered: str | None=None, wanted: set[str] | None=None, features: int | None=None) -> dict[str, str]:
    fields: dict[str, str] = {}
    if features is None:
        _, features = _classify_text(raw_text)
    if wanted is not None and not wanted:
        return fields
    if ('=' in raw_text or ':' in raw_text) and features & _FIELDS:
        for match in DIAGNOSTIC_FIELD_PATTERN.finditer(raw_text):
            _store_diagnostic_field(fields, match, wanted)
    # SPACE_DIAGNOSTIC_FIELD_PATTERN only ever contributes canonical keys the
    # '='/':' pass above didn't already find (_store_diagnostic_field never
    # overwrites an existing key), so once every key we could still want is
    # already present there is nothing left for the second regex pass to add.
    if features & _SPACE_FIELDS:
        still_wanted = _ALL_CANONICAL_FIELD_KEYS if wanted is None else wanted
        if not still_wanted <= fields.keys():
            for match in SPACE_DIAGNOSTIC_FIELD_PATTERN.finditer(raw_text):
                _store_diagnostic_field(fields, match, wanted)
    return fields

def _store_diagnostic_field(fields: dict[str, str], match: re.Match[str], wanted=None) -> None:
    normalized_key = match.group('key').translate(_KEY_FOLD_TABLE)
    canonical_key = FIELD_ALIASES.get(normalized_key)
    if canonical_key is not None and (wanted is None or canonical_key in wanted) and canonical_key not in fields:
        fields[canonical_key] = match.group('value').rstrip('.,;)')

@lru_cache(maxsize=2048)
def _normalized_key(value: str) -> str:
    # A pure string function, called for the same small, repeated set of
    # alias path components (e.g. 'level', 'exception_type', 'status')
    # tens of times per structured record - memoized so the regex
    # substitution runs once per distinct key ever seen, not once per
    # lookup. Bounded (not @cache) so an adversarial record with many
    # distinct field names cannot grow this unboundedly across a whole
    # ingestion run.
    return _NON_ALNUM_PATTERN.sub('', value.lower())

def _value_text(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key in ('stringValue', 'intValue', 'doubleValue', 'boolValue', 'bytesValue'):
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
