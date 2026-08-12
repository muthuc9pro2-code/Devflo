from io import BytesIO
from pathlib import Path

import pytest

from app.services.artifact_detector import (
    ArtifactFormat,
    detect_artifact,
    detect_artifact_sample,
    detect_artifact_stream,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "diagnostics"


@pytest.mark.parametrize(
    ("fixture_name", "expected"),
    (
        ("generic.txt", ArtifactFormat.GENERIC),
        ("json_in_txt.txt", ArtifactFormat.JSON),
        ("stack_trace.txt", ArtifactFormat.STACK_TRACE),
        ("nginx.txt", ArtifactFormat.WEB_SERVER),
        ("docker.jsonl", ArtifactFormat.CONTAINER),
        ("kubernetes.txt", ArtifactFormat.CONTAINER),
        ("ci.txt", ArtifactFormat.CI_CD),
        ("syslog.txt", ArtifactFormat.SYSLOG),
        ("otlp.json", ArtifactFormat.OPENTELEMETRY),
        ("browser.har", ArtifactFormat.BROWSER),
        ("cloud_gateway.txt", ArtifactFormat.CLOUD_GATEWAY),
        ("cloudfront.tsv", ArtifactFormat.CLOUD_GATEWAY),
        ("cloudwatch.json", ArtifactFormat.SERVERLESS),
        ("database.txt", ArtifactFormat.DATABASE),
        ("message_broker.txt", ArtifactFormat.MESSAGE_BROKER),
        ("serverless.txt", ArtifactFormat.SERVERLESS),
    ),
)
def test_content_first_format_detection(fixture_name, expected):
    path = FIXTURES / fixture_name
    assert detect_artifact(path, filename="misleading.txt") == expected


def test_misleading_json_hint_falls_back_to_generic():
    assert (
        detect_artifact_sample(
            b"ordinary unstructured diagnostic text",
            filename="misleading.json",
            mime_type="application/json",
        )
        == ArtifactFormat.GENERIC
    )


@pytest.mark.parametrize(
    ("sample", "expected"),
    (
        (b"2026-08-12 ERROR Query_time: 12.4 slow query", ArtifactFormat.DATABASE),
        (b"API Gateway request failed with status=502", ArtifactFormat.CLOUD_GATEWAY),
        (
            b'{"log":{"entries":[{"startedDateTime":"2026-08-12T00:00:00Z","request":{}}]}}',
            ArtifactFormat.BROWSER,
        ),
        (b"Kafka consumer group rebalance failed", ArtifactFormat.MESSAGE_BROKER),
        (b"START RequestId: lambda-1 Version: $LATEST", ArtifactFormat.SERVERLESS),
        (
            b"#Version: 1.0\n#Fields: date time x-edge-location sc-status cs-uri-stem",
            ArtifactFormat.CLOUD_GATEWAY,
        ),
        (
            b"[Wed Aug 12 10:11:16.123456 2026] [core:error] worker failed",
            ArtifactFormat.WEB_SERVER,
        ),
    ),
)
def test_remaining_v1_families_are_recognized(sample, expected):
    assert detect_artifact_sample(sample) == expected


def test_detector_uses_a_bounded_read_and_restores_position():
    class GuardedStream(BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 32
            return super().read(size)

    stream = GuardedStream(b'{"level":"error","message":"failed"}')
    stream.seek(4)

    result = detect_artifact_stream(stream, sample_bytes=32)

    assert result == ArtifactFormat.GENERIC
    assert stream.tell() == 4
