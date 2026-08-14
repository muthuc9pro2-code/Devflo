"""Differential equivalence test for structured_event_may_be_important(),
the retention gate for JSON-lines/JSON-document/BROWSER records.

Same technique as verify_retention_gates.py: run stream_artifact_events()
with the gate ACTIVE vs FORCED OFF (always parse) over every record in the
JSON and BROWSER 10 MiB fixtures, and assert:
  1. Records the gate parsed are byte-for-byte identical either way.
  2. Records the gate skipped genuinely aren't important (no evidence loss).

Also runs a hand-written adversarial battery of structured payload shapes
directly against structured_event_may_be_important() vs
normalize_structured_event(), including the specific risk case this gate's
design had to account for: a status-derived level of INFO where the message
text itself carries a competing level word.

Not a pytest test - a one-off correctness gate, run manually:

    .venv/bin/python scripts/verify_structured_gate.py
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.services.diagnostic_adapters as da  # noqa: E402
from app.services.artifact_detector import ArtifactFormat, detect_artifact  # noqa: E402
from app.services.diagnostic_parser import normalize_structured_event, structured_event_may_be_important  # noqa: E402
from app.services.event_filter import IMPORTANT_LEVELS  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"

FORMAT_FIXTURES = {
    "json": "json_10mib.jsonl",
    "browser": "browser_10mib.har",
}

_real_gate = structured_event_may_be_important


def _always_important(_data, inherited=None):
    return True


def _as_dict(event) -> dict:
    if event is None:
        return {"__none__": True}
    data = dataclasses.asdict(event)
    data.pop("fingerprint", None)
    return data


def _is_genuinely_unimportant(event) -> bool:
    if event is None:
        return True
    has_otel_identity = event.source_format == "opentelemetry" and (
        event.trace_id is not None or event.span_id is not None
    )
    return event.level not in IMPORTANT_LEVELS and not has_otel_identity


def check_format(name: str, fixture_path: Path) -> tuple[int, int]:
    artifact_format = detect_artifact(fixture_path, filename=fixture_path.name)

    da.structured_event_may_be_important = _real_gate
    gated = list(
        da.stream_artifact_events(
            file_path=str(fixture_path), artifact_format=artifact_format, source_file=fixture_path.name
        )
    )

    da.structured_event_may_be_important = _always_important
    try:
        ungated = list(
            da.stream_artifact_events(
                file_path=str(fixture_path), artifact_format=artifact_format, source_file=fixture_path.name
            )
        )
    finally:
        da.structured_event_may_be_important = _real_gate

    assert len(gated) == len(ungated), f"{name}: record count differs ({len(gated)} vs {len(ungated)})"

    mismatches = 0
    evidence_loss = 0
    for i, (gated_record, ungated_record) in enumerate(zip(gated, ungated, strict=True)):
        if gated_record.event is None:
            if not _is_genuinely_unimportant(ungated_record.event):
                evidence_loss += 1
                print(
                    f"EVIDENCE LOSS RISK [{name}] record {i}: gate skipped, ungated "
                    f"level={ungated_record.event.level!r} raw_line={ungated_record.event.raw_line[:120]!r}"
                )
            continue
        gated_d, ungated_d = _as_dict(gated_record.event), _as_dict(ungated_record.event)
        if gated_d != ungated_d:
            mismatches += 1
            print(f"CONTENT MISMATCH [{name}] record {i}:")
            for key in gated_d:
                if gated_d[key] != ungated_d.get(key):
                    print(f"    {key}: gated={gated_d[key]!r} ungated={ungated_d.get(key)!r}")
    return mismatches, evidence_loss


ADVERSARIAL_CASES = [
    {"level": "INFO", "message": "all good"},
    {"level": "info", "message": "all good"},
    {"level": "ERROR", "message": "boom"},
    {"severity": "WARN", "message": "careful"},
    {"status": "200", "message": "ok request"},
    {"status": "failed", "message": "operation failed"},
    {"stream": "stderr", "message": "stderr text"},
    {"stream": "stdout", "message": "stdout text"},
    {"error": {"type": "LookupError", "message": "missing"}},
    {"http_status": 200, "message": "fine"},
    {"http_status": 500, "message": "fine"},
    {"http_status": 404, "message": "fine"},
    {"status_code": 200, "message": "fine but has ERROR word inside"},
    {"status_code": 200, "message": "fine plain text"},
    {"response": {"status": 503}, "message": "gateway"},
    {"elb_status_code": "200", "message": "plain"},
    {"elb_status_code": "200", "message": "contains warn keyword"},
    {},  # nothing at all
    {"message": "just a plain message with no signals"},
    {"message": "plain message mentioning error nonetheless"},
    {"body": "raw body text status 200"},
]


def main() -> int:
    total_mismatches = 0
    total_loss = 0
    for name, fixture_name in FORMAT_FIXTURES.items():
        path = FIXTURE_DIR / fixture_name
        if not path.exists():
            print(f"{name}: SKIP (missing {path})")
            continue
        mismatches, loss = check_format(name, path)
        total_mismatches += mismatches
        total_loss += loss
        print(f"{name}: content_mismatches={mismatches} evidence_loss_risk={loss}")

    adversarial_violations = 0
    for data in ADVERSARIAL_CASES:
        gate_says_important = structured_event_may_be_important(data)
        event = normalize_structured_event(data, 1, source_file="f", source_format="json")
        actually_important = event.level in IMPORTANT_LEVELS
        if not gate_says_important and actually_important:
            adversarial_violations += 1
            print(f"ADVERSARIAL VIOLATION: gate=False but level={event.level!r} for data={data!r}")
        elif gate_says_important and not actually_important:
            print(f"(info, safe) over-conservative: gate=True, level={event.level!r} not important, data={data!r}")

    print(
        f"\nTOTAL content_mismatches={total_mismatches} evidence_loss_risk={total_loss} "
        f"adversarial_total={len(ADVERSARIAL_CASES)} adversarial_violations={adversarial_violations}"
    )
    return 1 if (total_mismatches or total_loss or adversarial_violations) else 0


if __name__ == "__main__":
    raise SystemExit(main())
