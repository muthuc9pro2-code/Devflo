
from __future__ import annotations

import dataclasses
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.services.diagnostic_parser as dp

new_normalize_text_event = dp.normalize_text_event


def old_normalize_text_event(raw_text, line_number, *, source_file=None, source_format=None, defaults=None):
    defaults = defaults or {}
    _, features = dp._classify_text(raw_text)
    text_defaults = {key: dp._as_text(defaults.get(key)) for key in dp._TEXT_FIELDS} if defaults else {}
    status = dp._safe_int(defaults.get('http_status'))
    missing = {key for key, value in text_defaults.items() if value is None} if defaults else None
    if missing is not None and status is None:
        missing.add('http_status')
    fields = dp._extract_diagnostic_fields(raw_text, missing, features)
    timestamp_match = dp.TIMESTAMP_PATTERN.search(raw_text) if not defaults.get('timestamp') and features & dp._TIMESTAMP else None
    level_match = dp.LOG_LEVEL_PATTERN.search(raw_text) if not defaults.get('level') and features & dp._LEVEL else None
    exception_type = dp._as_text(defaults.get('exception_type'))
    exception_message = dp._as_text(defaults.get('exception_message'))
    exception_match = None
    if features & dp._EXCEPTION and (exception_type is None or exception_message is None or not defaults.get('level')):
        if '\n' in raw_text:
            for exception_match in dp.EXCEPTION_PATTERN.finditer(raw_text):
                pass
        else:
            exception_match = dp.EXCEPTION_PATTERN.search(raw_text)
    endpoint = fields.get('endpoint')
    if text_defaults.get('endpoint') is None and endpoint is None and features & dp._HTTP:
        endpoint = dp._match_group(dp.HTTP_REQUEST_PATTERN, raw_text)
    if status is None:
        status = dp._safe_int(fields.get('http_status'))
    level = dp.normalize_level(defaults.get('level') or (level_match.group(1) if level_match else None))
    if level is None and (exception_match is not None or features & dp._FATAL):
        level = 'ERROR'
    elif level is None and features & dp._SLOW:
        level = 'WARNING'
    elif level is None:
        level = dp.level_from_http_status(status)
    if exception_match is not None:
        exception_type = exception_type or exception_match.group(1).split('.')[-1]
        exception_message = exception_message or exception_match.group(2)
    stack_frames = []
    if features & dp._STACK:
        stack_frames = dp._parse_stack_frames(raw_text)
    return dp.ParsedEvent(
        line_number=line_number, raw_line=raw_text,
        timestamp=dp.parse_timestamp(defaults.get('timestamp') or (timestamp_match.group(0) if timestamp_match else None)),
        level=level,
        trace_id=text_defaults.get('trace_id') or fields.get('trace_id'),
        request_id=text_defaults.get('request_id') or fields.get('request_id'),
        service=text_defaults.get('service') or fields.get('service'),
        module=text_defaults.get('module') or fields.get('module'),
        exception_type=exception_type, exception_message=exception_message,
        stack_frames=stack_frames,
        endpoint=text_defaults.get('endpoint') or endpoint,
        http_status=status, source_file=source_file,
        span_id=text_defaults.get('span_id') or fields.get('span_id'),
        parent_span_id=text_defaults.get('parent_span_id') or fields.get('parent_span_id'),
        host=text_defaults.get('host') or fields.get('host'),
        container=text_defaults.get('container') or fields.get('container'),
        pod=text_defaults.get('pod') or fields.get('pod'),
        source_format=source_format,
    )


RAW_TEXTS = [
    "GET /orders status=503 RuntimeError: failed",
    "plain informational text with no fields",
    "trace_id=abc request_id=def service=api module=payments",
    "ERROR ConnectionError: refused",
    "exception=TimeoutError message=\"database timeout\"",
    "budget getter target practice error_count=0",
    "Traceback (most recent call last):\n  File \"/x.py\", line 1, in y\nRuntimeError: boom",
    "at checkout (/srv/app.js:10:3)",
    "slow query detected query_time: 500ms",
    "panic: something broke",
    "segmentation fault detected",
    "  multi\nline\ntext\nwith\nerror\nkeyword",
    "",
    "café unicode message with exception=LookupError",
    "x" * 5000 + " exception=RuntimeError",
    "status=200 all good",
    "status=502 gateway error",
    "GET /health HTTP/1.1",
    "key = value spaced operator exception=X",
    "duplicate key=1 key=2",
]

DEFAULT_FIELD_KEYS = list(dp._TEXT_FIELDS)
LEVEL_VALUES = [None, "", "   ", "ERROR", "WARNING", "info", "CRITICAL", "err", "NOTLEVEL", 0, 10, 40]
TIMESTAMP_VALUES = [None, "", "2026-01-01T00:00:00Z", "2026-01-01 00:00:00", 1786529483000]
EXCEPTION_TYPE_VALUES = [None, "ConnectionError", ""]
EXCEPTION_MESSAGE_VALUES = [None, "boom", ""]
HTTP_STATUS_VALUES = [None, 200, 404, 502, "503"]


def _as_dict(event) -> dict:
    data = dataclasses.asdict(event)
    data.pop("fingerprint", None)
    return data


def main() -> int:
    rng = random.Random(9182736)
    mismatches = 0
    checked = 0

    def check(raw_text, defaults, label):
        nonlocal mismatches, checked
        checked += 1
        old = old_normalize_text_event(raw_text, checked, source_file="f.log", source_format="test", defaults=defaults)
        new = new_normalize_text_event(raw_text, checked, source_file="f.log", source_format="test", defaults=defaults)
        old_d, new_d = _as_dict(old), _as_dict(new)
        if old_d != new_d:
            mismatches += 1
            print(f"MISMATCH [{label}] defaults={defaults!r} raw_text={raw_text!r}")
            for key in old_d:
                if old_d[key] != new_d.get(key):
                    print(f"    {key}: old={old_d[key]!r} new={new_d.get(key)!r}")

    base_full_defaults = {key: f"val-{key}" for key in DEFAULT_FIELD_KEYS}
    for raw_text in RAW_TEXTS:
        for level in LEVEL_VALUES:
            for timestamp in TIMESTAMP_VALUES:
                for exc_type in EXCEPTION_TYPE_VALUES:
                    for exc_message in EXCEPTION_MESSAGE_VALUES:
                        for http_status in HTTP_STATUS_VALUES:
                            defaults = dict(base_full_defaults)
                            defaults["level"] = level
                            defaults["timestamp"] = timestamp
                            defaults["exception_type"] = exc_type
                            defaults["exception_message"] = exc_message
                            defaults["http_status"] = http_status
                            check(raw_text, defaults, "full_fields")

    for _ in range(3000):
        raw_text = rng.choice(RAW_TEXTS)
        defaults = {key: f"val-{key}" for key in DEFAULT_FIELD_KEYS}
        n_drop = rng.randint(0, len(DEFAULT_FIELD_KEYS))
        for key in rng.sample(DEFAULT_FIELD_KEYS, n_drop):
            defaults[key] = None
        defaults["level"] = rng.choice(LEVEL_VALUES)
        defaults["timestamp"] = rng.choice(TIMESTAMP_VALUES)
        defaults["exception_type"] = rng.choice(EXCEPTION_TYPE_VALUES)
        defaults["exception_message"] = rng.choice(EXCEPTION_MESSAGE_VALUES)
        defaults["http_status"] = rng.choice(HTTP_STATUS_VALUES)
        check(raw_text, defaults, "partial_fields")

    for raw_text in RAW_TEXTS:
        check(raw_text, {}, "no_defaults")
        check(raw_text, None, "none_defaults")

    print(f"\nchecked={checked} mismatches={mismatches}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
