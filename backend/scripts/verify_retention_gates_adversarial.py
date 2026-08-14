"""Adversarial battery for the per-format retention gates: hand-written
edge cases per format (boundary status codes, misleading substrings, unusual
casing, malformed shapes) checked directly against each format's real
normalizer to prove the gate never says "skip" when the real parse would
have produced an important event.

Not a pytest test - a one-off correctness gate, run manually:

    .venv/bin/python scripts/verify_retention_gates_adversarial.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.services.diagnostic_adapters as da  # noqa: E402
from app.services.artifact_detector import ArtifactFormat  # noqa: E402
from app.services.event_filter import IMPORTANT_LEVELS  # noqa: E402

CASES: dict[ArtifactFormat, list[str]] = {
    ArtifactFormat.WEB_SERVER: [
        '203.0.113.9 - - [12/Aug/2026:10:11:15 +0000] "GET /api/orders HTTP/1.1" 200 173 "-" "curl/8"',
        '203.0.113.9 - - [12/Aug/2026:10:11:15 +0000] "GET /api/orders HTTP/1.1" 399 173 "-" "curl/8"',
        '203.0.113.9 - - [12/Aug/2026:10:11:15 +0000] "GET /api/orders HTTP/1.1" 400 173 "-" "curl/8"',
        '203.0.113.9 - - [12/Aug/2026:10:11:15 +0000] "GET /api/orders HTTP/1.1" 499 173 "-" "curl/8"',
        '203.0.113.9 - - [12/Aug/2026:10:11:15 +0000] "GET /api/orders HTTP/1.1" 500 173 "-" "curl/8"',
        "2026/08/12 10:11:22 [notice] 123#123: normal startup",
        "2026/08/12 10:11:22 [info] 123#123: normal info",
        "2026/08/12 10:11:22 [warn] 123#123: upstream response buffered",
        "[Wed Aug 12 10:11:22.123456 2026] [core:info] [pid 123] worker info",
        "[Wed Aug 12 10:11:22.123456 2026] [core:notice] [pid 123] child init",
        "not a recognized web log shape at all",
        "not recognized but contains error keyword",
        "budget getter target practice",  # 'get ' substring false positive check
    ],
    ArtifactFormat.SYSLOG: [
        "<191>1 2026-08-12T10:11:19Z host1 app 1 - - debug level facility 23 severity 7",
        "<190>1 2026-08-12T10:11:19Z host1 app 1 - - info level facility 23 severity 6",
        "<188>1 2026-08-12T10:11:19Z host1 app 1 - - warning level facility 23 severity 4",
        "<187>1 2026-08-12T10:11:19Z host1 app 1 - - error level facility 23 severity 3",
        "<184>1 2026-08-12T10:11:19Z host1 app 1 - - critical level facility 23 severity 0",
        "<134>Aug 12 10:11:20 worker-1 app[321]: heartbeat ok",
        "<27>Aug 12 10:11:20 worker-1 app[321]: warning depth",
        "not a syslog line at all",
    ],
    ArtifactFormat.CONTAINER: [
        '2026-08-12T10:11:16.123456Z stdout F {"level":"info","message":"ok"}',
        '2026-08-12T10:11:16.123456Z stderr F {"level":"info","message":"ok"}',
        '2026-08-12T10:11:16.123456Z stdout F {"level":"error","message":"boom"}',
        '2026-08-12T10:11:16.123456Z stdout F plain text no json here',
        '2026-08-12T10:11:16.123456Z stdout F plain text with error keyword',
        "not a CRI line at all",
    ],
    ArtifactFormat.CLOUD_GATEWAY: [
        'http 2026-08-12T10:11:22.000000Z app/gateway/1 203.0.113.1:50000 10.0.0.1:8080 0.001 0.002 0.001 200 200 120 57 "GET https://api.example.test/orders HTTP/1.1" "curl/8" - - arn:trace Root=trace-1',
        'http 2026-08-12T10:11:22.000000Z app/gateway/1 203.0.113.1:50000 10.0.0.1:8080 0.001 0.002 0.001 399 399 120 57 "GET https://api.example.test/orders HTTP/1.1" "curl/8" - - arn:trace Root=trace-1',
        'http 2026-08-12T10:11:22.000000Z app/gateway/1 203.0.113.1:50000 10.0.0.1:8080 0.001 0.002 0.001 400 400 120 57 "GET https://api.example.test/orders HTTP/1.1" "curl/8" - - arn:trace Root=trace-1',
        'http 2026-08-12T10:11:22.000000Z app/gateway/1 203.0.113.1:50000 10.0.0.1:8080 0.001 0.002 0.001 502 502 120 57 "GET https://api.example.test/orders HTTP/1.1" "curl/8" - - arn:trace Root=trace-1',
        "not an ALB line at all",
        "api gateway custom text with no status column",
    ],
    ArtifactFormat.MESSAGE_BROKER: [
        "2026-08-12T10:14:00.123Z INFO kafka heartbeat",
        "2026-08-12T10:14:00.123Z WARN rabbitmq connection blocked",
        "2026-08-12T10:14:00.123Z ERROR kafka offset commit failed",
        "not a recognized broker line",
    ],
    ArtifactFormat.SERVERLESS: [
        "START RequestId: abc Version: $LATEST",
        "END RequestId: abc",
        "REPORT RequestId: abc Duration: 1.0 ms",
        "2026-08-12T10:15:00.123Z\tabc\tINFO\tservice=x processed",
        "2026-08-12T10:15:00.123Z\tabc\tWARN\tservice=x retrying",
        "2026-08-12T10:15:00.123Z\tabc\tERROR\tservice=x RuntimeError: resize failed",
        "not a lambda line",
    ],
    ArtifactFormat.CI_CD: [
        "2026-08-12T10:11:18Z ##[section] Starting job",
        "2026-08-12T10:11:18Z ##[warning] retrying step",
        "2026-08-12T10:11:18Z ##[error] deployment failed",
        "2026-08-12T10:11:18Z plain build output line",
        "2026-08-12T10:11:18Z BUILD FAILED for target x",
        "2026-08-12T10:11:18Z Deployment Failed unexpectedly",
    ],
    ArtifactFormat.STACK_TRACE: [
        "2026-08-12 10:11:14 INFO service=worker heartbeat ok",
        "2026-08-12 10:11:14 ERROR service=worker something broke",
        "Crash Report\nProcess: frontend [123]\nsomething with no other markers",
        "Core dumped\nunrelated content with no other markers",
        "plain informational text only",
    ],
}


def normalize_for(artifact_format: ArtifactFormat, raw_text: str):
    if artifact_format == ArtifactFormat.WEB_SERVER:
        return da._normalize_web_event(raw_text, 1, "f")
    if artifact_format == ArtifactFormat.SYSLOG:
        return da._normalize_syslog_event(raw_text, 1, "f")
    if artifact_format == ArtifactFormat.CONTAINER:
        return da._normalize_container_text_event(raw_text, 1, "f")
    if artifact_format == ArtifactFormat.CLOUD_GATEWAY:
        return da._normalize_cloud_gateway_event(raw_text, 1, "f")
    if artifact_format == ArtifactFormat.MESSAGE_BROKER:
        return da._normalize_message_broker_event(raw_text, 1, "f")
    if artifact_format == ArtifactFormat.SERVERLESS:
        return da._normalize_serverless_text_event(raw_text, 1, "f")
    if artifact_format == ArtifactFormat.CI_CD:
        lowered = raw_text.lower()
        defaults = {}
        if "##[error]" in lowered or "build failed" in lowered or "deployment failed" in lowered:
            defaults["level"] = "ERROR"
        elif "##[warning]" in lowered:
            defaults["level"] = "WARNING"
        return da.normalize_text_event(raw_text, 1, source_file="f", source_format="ci_cd", defaults=defaults)
    if artifact_format == ArtifactFormat.STACK_TRACE:
        defaults = {}
        lowered = raw_text.lower()
        if "crash report" in lowered or "core dumped" in lowered:
            defaults["level"] = "ERROR"
        return da.normalize_text_event(raw_text, 1, source_file="f", source_format="stack_trace", defaults=defaults)
    raise ValueError(artifact_format)


def main() -> int:
    total = 0
    violations = 0
    for artifact_format, cases in CASES.items():
        for raw_text in cases:
            total += 1
            gate_says_important = da._may_be_important(artifact_format, raw_text)
            event = normalize_for(artifact_format, raw_text)
            actually_important = event.level in IMPORTANT_LEVELS
            if not gate_says_important and actually_important:
                violations += 1
                print(
                    f"VIOLATION [{artifact_format.value}]: gate=False but "
                    f"actual level={event.level!r} (raw_text={raw_text!r})"
                )
            # Also report (non-fatal) over-conservatism for visibility.
            elif gate_says_important and not actually_important:
                print(
                    f"(info, safe) over-conservative [{artifact_format.value}]: "
                    f"gate=True, actual level={event.level!r} not important "
                    f"(raw_text={raw_text[:80]!r})"
                )

    print(f"\ntotal={total} violations={violations}")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
