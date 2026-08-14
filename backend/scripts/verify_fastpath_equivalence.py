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
    "2026-01-01T00:00:00Z ERR something failed badly",
    "2026-01-01T00:00:00Z ERROR preferred config applied deferred=true",
    "2026-01-01T00:00:00.123456789+05:30 CRITICAL service=db exception=TimeoutError message=\"db timeout\"",
    "2026-01-01 00:00:00 WARNING no explicit fields at all",
    "2026-01-01T00:00:00Z INFO service=api GET /health status=200 duration_ms=12",
    "2026-01-01T00:00:00Z INFO service=api status=503 duration_ms=12",
    "2026-01-01T00:00:00Z INFO service=api status_code=404",
    "2026-01-01T00:00:00Z INFO service=api http_status: 500",
    "2026-01-01T00:00:00Z DEBUG trace_id=abc span_id=def parent_span_id=ghi request_id=req-1",
    "2026-01-01T00:00:00Z NOTICE service prod",  # space-separated field (no '=')
    "2026-01-01T00:00:00Z TRACE key = value",  # spaced '=' separator
    "2026-01-01T00:00:00Z FATAL panic: something broke",
    "2026-01-01T00:00:00Z ALERT segmentation fault detected",
    "2026-01-01T00:00:00Z EMERGENCY system down",
    "2026-01-01T00:00:00Z SEVERE disk failure imminent",
    "2026-01-01T00:00:00Z WARN slow query detected query_time: 500ms",
    "2026-01-01T00:00:00Z INFO abc-service=prod hyphen-glued key",
    "2026-01-01T00:00:00Z ERROR exception=MyModule.Sub.ConnectionError message=\"boom\"",
    "not a timestamp at all HERE",
    "2026-01-01T00:00:00Z",  # no level token at all
    "2026-01-01T00:00:00Z NOTLEVEL rest of line",  # level-shaped position, not a real level
    "justtext",
    "",
    "2026-01-01T00:00:00Zxyz BADLEVEL rest",  # timestamp token with trailing garbage
]


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

    for line_number, (line, _end) in enumerate(stream_text_lines(str(FIXTURE_PATH)), start=1):
        check(line, line_number, "fixture")

    print(
        f"\nfast_handled={fast_handled} fast_declined={fast_declined} "
        f"mismatches={mismatches}"
    )
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
