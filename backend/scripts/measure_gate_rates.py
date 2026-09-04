from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.services.diagnostic_adapters as da
from app.services.artifact_detector import detect_artifact

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"

GATED_FORMATS = {
    "generic": "generic_10mib.log",
    "web_server": "web_server_10mib.log",
    "syslog": "syslog_10mib.log",
    "container": "container_10mib.log",
    "cloud_gateway": "cloud_gateway_10mib.log",
    "message_broker": "message_broker_10mib.log",
    "serverless": "serverless_10mib.log",
    "ci_cd": "ci_cd_10mib.log",
    "stack_trace": "stack_trace_10mib.log",
}
UNGATED_FORMATS = {
    "database": "database_10mib.log",
    "opentelemetry": "opentelemetry_10mib.json",
}
STRUCTURED_FORMATS = {
    "json": "json_10mib.jsonl",
    "browser": "browser_10mib.har",
}


def measure_text(name: str, fixture_name: str) -> None:
    path = FIXTURE_DIR / fixture_name
    fmt = detect_artifact(path, filename=path.name)
    counts = {"true": 0, "false": 0}
    real = da._may_be_important

    def counting(artifact_format, raw_text):
        result = real(artifact_format, raw_text)
        counts["true" if result else "false"] += 1
        return result

    da._may_be_important = counting
    try:
        records = list(da.stream_artifact_events(file_path=str(path), artifact_format=fmt, source_file=path.name))
    finally:
        da._may_be_important = real

    total = counts["true"] + counts["false"]
    fast_pct = counts["false"] / total * 100 if total else 0.0
    print(f"{name:<14} records={len(records):<8} gate_calls={total:<8} fast_reject={counts['false']:<8} ({fast_pct:.1f}%) sent_to_full_parse={counts['true']:<8} ({100 - fast_pct:.1f}%)")


def measure_structured(name: str, fixture_name: str) -> None:
    path = FIXTURE_DIR / fixture_name
    fmt = detect_artifact(path, filename=path.name)
    counts = {"true": 0, "false": 0}
    real = da.structured_event_may_be_important

    def counting(artifact_format, structured):
        result = real(artifact_format, structured)
        counts["true" if result else "false"] += 1
        return result

    da.structured_event_may_be_important = counting
    try:
        records = list(da.stream_artifact_events(file_path=str(path), artifact_format=fmt, source_file=path.name))
    finally:
        da.structured_event_may_be_important = real

    total = counts["true"] + counts["false"]
    fast_pct = counts["false"] / total * 100 if total else 0.0
    print(f"{name:<14} records={len(records):<8} gate_calls={total:<8} fast_reject={counts['false']:<8} ({fast_pct:.1f}%) sent_to_full_parse={counts['true']:<8} ({100 - fast_pct:.1f}%)")


def measure_ungated(name: str, fixture_name: str) -> None:
    path = FIXTURE_DIR / fixture_name
    fmt = detect_artifact(path, filename=path.name)
    records = list(da.stream_artifact_events(file_path=str(path), artifact_format=fmt, source_file=path.name))
    print(f"{name:<14} records={len(records):<8} no retention gate (100% sent to full parse by design)")


for name, fixture in GATED_FORMATS.items():
    measure_text(name, fixture)
for name, fixture in STRUCTURED_FORMATS.items():
    measure_structured(name, fixture)
for name, fixture in UNGATED_FORMATS.items():
    measure_ungated(name, fixture)
