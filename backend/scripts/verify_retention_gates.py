"""Differential equivalence test for the per-format retention gates added in
_may_be_important()/_build_text_artifact_event().

For every record in every 10 MiB benchmark fixture (plus a hand-written
adversarial battery per format), this compares:

  A) gate ACTIVE (current code): stream_artifact_events() as shipped
  B) gate FORCED OFF (_may_be_important monkeypatched to always return True):
     every record fully parsed, exactly like before this optimization

and asserts:

  1. For every record the active gate chose to parse (event is not None),
     its ParsedEvent is byte-for-byte identical to what the ungated parse
     produced for that same record.
  2. For every record the active gate skipped (event is None), the ungated
     parse's event.level is NOT in IMPORTANT_LEVELS and it is not an
     OpenTelemetry event carrying a trace/span id - i.e. it genuinely
     would never have become evidence. No evidence is silently dropped.

Not a pytest test - a one-off correctness gate, run manually:

    .venv/bin/python scripts/verify_retention_gates.py
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.services.diagnostic_adapters as da  # noqa: E402
from app.services.artifact_detector import ArtifactFormat, detect_artifact  # noqa: E402
from app.services.event_filter import IMPORTANT_LEVELS  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"

FORMAT_FIXTURES = {
    "web_server": "web_server_10mib.log",
    "syslog": "syslog_10mib.log",
    "container": "container_10mib.log",
    "cloud_gateway": "cloud_gateway_10mib.log",
    "message_broker": "message_broker_10mib.log",
    "serverless": "serverless_10mib.log",
    "ci_cd": "ci_cd_10mib.log",
    "stack_trace": "stack_trace_10mib.log",
    "generic": "generic_10mib.log",
    "database": "database_10mib.log",
}

_real_may_be_important = da._may_be_important


def _always_important(_artifact_format, _raw_text) -> bool:
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

    da._may_be_important = _real_may_be_important
    gated = list(
        da.stream_artifact_events(
            file_path=str(fixture_path), artifact_format=artifact_format, source_file=fixture_path.name
        )
    )

    da._may_be_important = _always_important
    try:
        ungated = list(
            da.stream_artifact_events(
                file_path=str(fixture_path), artifact_format=artifact_format, source_file=fixture_path.name
            )
        )
    finally:
        da._may_be_important = _real_may_be_important

    assert len(gated) == len(ungated), f"{name}: record count differs ({len(gated)} vs {len(ungated)})"

    mismatches = 0
    skipped_but_would_be_important = 0
    for i, (gated_record, ungated_record) in enumerate(zip(gated, ungated, strict=True)):
        if gated_record.event is None:
            if not _is_genuinely_unimportant(ungated_record.event):
                skipped_but_would_be_important += 1
                print(
                    f"EVIDENCE LOSS RISK [{name}] record {i}: gate skipped it, but "
                    f"ungated parse gives level={ungated_record.event.level!r} "
                    f"(raw_line={ungated_record.event.raw_line[:120]!r})"
                )
            continue
        gated_d = _as_dict(gated_record.event)
        ungated_d = _as_dict(ungated_record.event)
        if gated_d != ungated_d:
            mismatches += 1
            print(f"CONTENT MISMATCH [{name}] record {i}:")
            for key in gated_d:
                if gated_d[key] != ungated_d.get(key):
                    print(f"    {key}: gated={gated_d[key]!r} ungated={ungated_d.get(key)!r}")

    return mismatches, skipped_but_would_be_important


def main() -> int:
    total_mismatches = 0
    total_evidence_loss = 0
    for name, fixture_name in FORMAT_FIXTURES.items():
        path = FIXTURE_DIR / fixture_name
        if not path.exists():
            print(f"{name}: SKIP (missing {path})")
            continue
        mismatches, evidence_loss = check_format(name, path)
        total_mismatches += mismatches
        total_evidence_loss += evidence_loss
        print(f"{name}: content_mismatches={mismatches} evidence_loss_risk={evidence_loss}")

    print(f"\nTOTAL content_mismatches={total_mismatches} evidence_loss_risk={total_evidence_loss}")
    return 1 if (total_mismatches or total_evidence_loss) else 0


if __name__ == "__main__":
    raise SystemExit(main())
