"""Exhaustive equivalence check: fast_path_prefixed_event vs normalize_text_event.

Runs BOTH parsers over every GENERIC-eligible line of the frozen 10 MiB
fixture (and a battery of hand-picked edge cases) and asserts field-for-field
identical ParsedEvent output whenever the fast path chooses to handle a
record instead of returning None. Not a pytest test - a one-off correctness
gate for the fast-path optimization, run manually:

    .venv/bin/python scripts/verify_fastpath_equivalence.py
"""

from __future__ import annotations

import dataclasses
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.diagnostic_parser import (  # noqa: E402
    fast_path_prefixed_event,
    normalize_text_event,
)
from app.utils.file_reader import stream_text_lines  # noqa: E402

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "tests/fixtures/bench/devflo_10mib.log"

EDGE_CASES = [
    # --- ERR vs ERROR / mixed casing / all supported severities ---
    "2026-01-01T00:00:00Z ERR something failed badly",
    "2026-01-01T00:00:00Z err lowercase err level",
    "2026-01-01T00:00:00Z Err MixedCase err level",
    "2026-01-01T00:00:00Z ERROR uppercase error",
    "2026-01-01T00:00:00Z WARN short warn form",
    "2026-01-01T00:00:00Z WARNING long warning form",
    "2026-01-01T00:00:00Z CRITICAL critical event",
    "2026-01-01T00:00:00Z FATAL fatal event",
    "2026-01-01T00:00:00Z SEVERE severe event",
    "2026-01-01T00:00:00Z ALERT alert event",
    "2026-01-01T00:00:00Z EMERG emergency short form",
    "2026-01-01T00:00:00Z EMERGENCY emergency long form",
    "2026-01-01T00:00:00Z NOTICE notice event",
    "2026-01-01T00:00:00Z TRACE trace event",
    "2026-01-01T00:00:00Z DEBUG debug event",
    "2026-01-01T00:00:00Z INFO info event",
    "2026-01-01T00:00:00Z info lowercase info",
    "2026-01-01T00:00:00Z WaRnInG alternating case warning",
    # --- misleading substrings ---
    "2026-01-01T00:00:00Z INFO error_count=0 no_failure=true exception_free=yes",
    "2026-01-01T00:00:00Z INFO terror_alert=false preferred=true deferred=true",
    "2026-01-01T00:00:00Z INFO budget getter target practice",  # 'get ' substring inside words
    "2026-01-01T00:00:00Z INFO errorcode=success",
    # --- status=200 / status=500 and separator/key variants ---
    "2026-01-01T00:00:00Z INFO service=api status=200",
    "2026-01-01T00:00:00Z INFO service=api status=500",
    "2026-01-01T00:00:00Z INFO service=api status=404",
    "2026-01-01T00:00:00Z INFO service=api status:200",
    "2026-01-01T00:00:00Z INFO service=api status_code=200",
    "2026-01-01T00:00:00Z INFO service=api http_status=502",
    # --- quoted / escaped values ---
    '2026-01-01T00:00:00Z ERROR service=api message="connection refused to host 10.0.0.1:5432"',
    '2026-01-01T00:00:00Z ERROR message="line with \\"nested\\" quotes"',
    "2026-01-01T00:00:00Z ERROR message='single quoted value here'",
    '2026-01-01T00:00:00Z ERROR path="C:\\\\Users\\\\test\\\\file.txt" trace_id=abc',
    # --- empty / truncated / malformed ---
    "2026-01-01T00:00:00Z INFO",
    "2026-01-01T00:00:00Z INFO ",
    "2026-01-01T00:00:00Z ERROR service=",
    "2026-01-01T00:00:00Z ERROR =novalue",
    "2026-01-01T00:00:00Z ERROR service==doubled",
    "2026-01-01T00:00:00Z ERROR trace_id",  # bare key, no value follows at all
    "2026-01-01T00:00:00Z ERROR trace_id=",  # bare key with dangling operator
    "2026-01-01T00:00:00Z ERROR ===",
    "2026-01-01T00:00:00Z ERROR service:",  # spaced-adjacent operator (no space) but empty value
    "2026-01-01T00:00:00Z ERROR service: value",  # spaced operator -> must fall back
    "2026-01-01T00:00:00Z ERROR service :value",  # spaced operator other side
    "2026-01-01T00:00:00Z ERROR service = value",  # fully spaced '='
    # --- duplicate keys / aliases ---
    "2026-01-01T00:00:00Z ERROR service=first service=second",
    "2026-01-01T00:00:00Z ERROR app=first service=second component=third",
    "2026-01-01T00:00:00Z ERROR trace_id=first traceId=second TRACE_ID=third",
    "2026-01-01T00:00:00Z ERROR request_id=first correlation_id=second x-request-id=third",
    "2026-01-01T00:00:00Z ERROR container_id=abc containerName=def",
    "2026-01-01T00:00:00Z ERROR podname=p1 pod=p2",
    "2026-01-01T00:00:00Z ERROR hostname=h1 host=h2 node=h3",
    # --- trace/span/request/correlation ids, various spellings, glued and bare ---
    "2026-01-01T00:00:00Z DEBUG trace_id=abc span_id=def parent_span_id=ghi request_id=req-1",
    "2026-01-01T00:00:00Z DEBUG traceId=abc spanId=def parentSpanId=ghi requestId=req-1",
    "2026-01-01T00:00:00Z DEBUG trace-id=abc span-id=def parent-span-id=ghi",
    "2026-01-01T00:00:00Z DEBUG service prod",
    "2026-01-01T00:00:00Z DEBUG trace abc",  # bare 'trace' word, not a recognized field alone
    "2026-01-01T00:00:00Z DEBUG module prod",
    "2026-01-01T00:00:00Z DEBUG logger prod",
    "2026-01-01T00:00:00Z DEBUG endpoint /health",
    "2026-01-01T00:00:00Z DEBUG route /v1/users",
    "2026-01-01T00:00:00Z DEBUG x-request-id=abc123",
    "2026-01-01T00:00:00Z DEBUG xrequestid=abc123",  # glued form the real regex doesn't accept bare
    "2026-01-01T00:00:00Z DEBUG x_request_id=abc123",
    # --- HTTP verbs / endpoints ---
    "2026-01-01T00:00:00Z INFO service=api GET /health status=200 duration_ms=12",
    "2026-01-01T00:00:00Z INFO service=api POST /v1/orders status=201",
    "2026-01-01T00:00:00Z INFO GET /a/b/c",
    "2026-01-01T00:00:00Z INFO service=api endpoint=/explicit GET /ignored",
    "2026-01-01T00:00:00Z INFO GET",  # verb with nothing after it
    # --- exception detection ---
    "2026-01-01T00:00:00Z ERROR exception=ConnectionError message=\"connection failed\"",
    "2026-01-01T00:00:00Z ERROR MyModule.Sub.ConnectionError: timed out",
    "2026-01-01T00:00:00Z ERROR raised RuntimeError without colon suffix",
    "2026-01-01T00:00:00Z ERROR NullPointerException",
    "2026-01-01T00:00:00Z ERROR ValidationFailure: bad input",
    "2026-01-01T00:00:00Z INFO nothing_exceptional_here=true",
    # --- misleading marker words that must NOT create false positives on level ---
    "2026-01-01T00:00:00Z INFO preferred config deferred referred occurred",
    "2026-01-01T00:00:00Z INFO terrific terrain terrier",
    # --- unicode ---
    "2026-01-01T00:00:00Z ERROR service=café message=\"héllo wörld\" user=北京",
    "2026-01-01T00:00:00Z INFO emoji=🚀 service=api",
    "2026-01-01T00:00:00Z ERROR naïve=true résumé=café",
    # --- very long / huge fields ---
    "2026-01-01T00:00:00Z ERROR service=api message=\"" + ("x" * 5000) + "\"",
    "2026-01-01T00:00:00Z ERROR " + " ".join(f"field{i}=value{i}" for i in range(200)),
    "2026-01-01T00:00:00Z ERROR bigvalue=" + ("a" * 3000),
    # --- timestamps: timezone variants ---
    "2026-01-01T00:00:00Z ERROR utc marker",
    "2026-01-01T00:00:00+05:30 ERROR positive offset",
    "2026-01-01T00:00:00-08:00 ERROR negative offset",
    "2026-01-01T00:00:00+0530 ERROR offset without colon",
    "2026-01-01T00:00:00.123456789Z ERROR nanosecond precision",
    "2026-01-01 00:00:00 ERROR space separated date time",
    # --- stack traces / multiline (must always fall back) ---
    "2026-01-01T00:00:00Z ERROR Traceback (most recent call last):\n  File \"x.py\", line 1, in <module>",
    "2026-01-01T00:00:00Z ERROR line one\nline two continuation",
    # --- structurally-not-fast-path ---
    "not a timestamp at all HERE",
    "2026-01-01T00:00:00Z",  # no level token at all
    "2026-01-01T00:00:00Z NOTLEVEL rest of line",  # level-shaped position, not a real level
    "justtext",
    "",
    "2026-01-01T00:00:00Zxyz BADLEVEL rest",  # timestamp token with trailing garbage
    "2026-01-01T00:00:00Z INFO abc-service=prod hyphen-glued key",
    "2026-01-01T00:00:00Z FATAL panic: something broke",
    "2026-01-01T00:00:00Z ALERT segmentation fault detected",
    "2026-01-01T00:00:00Z WARN slow query detected query_time: 500ms",
]


def _generate_fuzz_cases(rng: random.Random, count: int) -> list[str]:
    timestamps = [
        "2026-03-14T09:22:07Z",
        "2026-03-14T09:22:07.123Z",
        "2026-03-14T09:22:07+05:30",
        "2026-03-14T09:22:07-0800",
        "2026-03-14 09:22:07",
    ]
    levels = [
        "ERR", "ERROR", "err", "Error", "WARN", "WARNING", "warn",
        "CRITICAL", "FATAL", "SEVERE", "ALERT", "EMERG", "EMERGENCY",
        "NOTICE", "TRACE", "DEBUG", "INFO", "info",
    ]
    field_names = [
        "trace_id", "traceId", "trace-id", "span_id", "spanId",
        "parent_span_id", "request_id", "requestId", "correlation_id",
        "x-request-id", "service", "servicename", "app", "component",
        "module", "logger", "host", "hostname", "node", "container",
        "containerId", "pod", "podname", "status", "status_code",
        "http_status", "endpoint", "route", "path", "url",
        "session_id", "user_id", "unknownfield", "error_count", "duration_ms",
    ]
    values = [
        "abc123", "tr-000001", "10.0.0.1:5432", "/v1/users/42", "200",
        "500", "café", "🚀", "a-b-c_d.e", "true", "0", "-42",
        "x" * 50, "value,with;punct)here.", '"quoted"', "'quoted'",
    ]
    separators = ["=", ":"]
    exception_words = [
        "ConnectionError", "TimeoutError", "NullPointerException",
        "ValidationFailure", "error_count=3", "MyModule.Sub.Error",
    ]
    http_bits = ["GET /health", "POST /v1/orders", "DELETE /x/y"]

    cases = []
    for _ in range(count):
        ts = rng.choice(timestamps)
        level = rng.choice(levels)
        n_fields = rng.randint(0, 5)
        parts = [ts, level]
        if rng.random() < 0.15:
            parts.extend(rng.choice(http_bits).split(" "))
        if rng.random() < 0.2:
            parts.append(rng.choice(exception_words))
        for _ in range(n_fields):
            key = rng.choice(field_names)
            value = rng.choice(values)
            sep = rng.choice(separators)
            spaced = rng.random() < 0.1
            if spaced:
                parts.append(f"{key}")
                parts.append(sep)
                parts.append(value)
            else:
                parts.append(f"{key}{sep}{value}")
        cases.append(" ".join(parts))
    return cases


def _as_dict(event) -> dict:
    if event is None:
        return {"__none__": True}
    data = dataclasses.asdict(event)
    data.pop("fingerprint", None)
    return data


def main() -> int:
    mismatches = 0
    fast_handled = 0
    fast_declined = 0

    def check(raw_text: str, line_number: int, label: str) -> bool:
        nonlocal mismatches, fast_handled, fast_declined
        fast = fast_path_prefixed_event(
            raw_text, line_number, source_file="bench.log", source_format="generic"
        )
        if fast is None:
            fast_declined += 1
            return True
        fast_handled += 1
        slow = normalize_text_event(
            raw_text, line_number, source_file="bench.log", source_format="generic", defaults={}
        )
        fast_d, slow_d = _as_dict(fast), _as_dict(slow)
        if fast_d != slow_d:
            mismatches += 1
            print(f"MISMATCH [{label}] line {line_number}: {raw_text!r}")
            for key in fast_d:
                if fast_d[key] != slow_d.get(key):
                    print(f"    {key}: fast={fast_d[key]!r} slow={slow_d.get(key)!r}")
            return False
        return True

    for i, text in enumerate(EDGE_CASES):
        check(text, i + 1, "edge_case")

    rng = random.Random(20260814)
    fuzz_cases = _generate_fuzz_cases(rng, 20000)
    for i, text in enumerate(fuzz_cases):
        check(text, i + 1, "fuzz")

    for line_number, (line, _end) in enumerate(stream_text_lines(str(FIXTURE_PATH)), start=1):
        check(line, line_number, "fixture")

    print(
        f"\nfast_handled={fast_handled} fast_declined={fast_declined} "
        f"mismatches={mismatches}"
    )
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
