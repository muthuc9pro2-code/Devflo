import json
from dataclasses import fields
from pathlib import Path

import pytest

from app.services import diagnostic_parser
from app.services.artifact_detector import ArtifactFormat, detect_artifact
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.diagnostic_parser import (
    normalize_level,
    normalize_otel_severity,
    parse_timestamp,
)
from app.services.event_filter import filter_important_events
from app.services.log_praser import ParsedEvent, parse_log_line

FIXTURES = Path(__file__).parents[1] / "fixtures" / "diagnostics"
CANONICAL_FIELDS = {
    "line_number",
    "raw_line",
    "timestamp",
    "level",
    "trace_id",
    "request_id",
    "service",
    "module",
    "exception_type",
    "exception_message",
    "fingerprint",
    "stack_frames",
    "endpoint",
    "http_status",
    "source_file",
    "span_id",
    "parent_span_id",
    "host",
    "container",
    "pod",
    "source_format",
    "artifact_id",
}


def normalized(fixture_name):
    path = FIXTURES / fixture_name
    return normalized_path(path)


def normalized_path(path: Path):
    artifact_format = detect_artifact(path, filename=path.name)
    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )
    for record in records:
        assert isinstance(record.event, ParsedEvent)
        assert CANONICAL_FIELDS <= {item.name for item in fields(record.event)}
    return [record.event for record in records]


def test_generic_plain_application_log():
    event = normalized("generic.txt")[0]
    assert event.level == "ERROR"
    assert event.trace_id == "trace-1"
    assert event.request_id == "req-1"
    assert event.service == "checkout"
    assert event.module == "payments"
    assert event.exception_type == "ValueError"


def test_text_normalization_reuses_one_feature_classification(monkeypatch):
    classify = diagnostic_parser._classify_text
    calls = []

    def counted(raw_text):
        calls.append(raw_text)
        return classify(raw_text)

    monkeypatch.setattr(diagnostic_parser, "_classify_text", counted)
    event = diagnostic_parser.normalize_text_event(
        "2026-08-12T10:00:00Z ERROR service=api trace_id=trace-1 "
        "GET /orders status=503 RuntimeError: failed",
        1,
    )

    assert calls == [event.raw_line]
    assert (event.trace_id, event.endpoint, event.http_status) == (
        "trace-1",
        "/orders",
        503,
    )


def test_generic_parser_preserves_space_separated_identity_fields():
    event = parse_log_line(
        "2026-08-12 10:11:12 ERROR trace_id trace-space "
        "request_id request-space service checkout",
        1,
    )
    assert event.trace_id == "trace-space"
    assert event.request_id == "request-space"
    assert event.service == "checkout"


def test_generic_parser_preserves_legacy_timestamp_string():
    event = parse_log_line("2026-08-12 10:11:12 ERROR failed", 1)

    assert event.timestamp == "2026-08-12 10:11:12"
    assert isinstance(event.timestamp, str)


def test_jsonl_structured_application_log_in_txt():
    event = normalized("json_in_txt.txt")[0]
    assert event.level == "ERROR"
    assert event.trace_id == "trace-2"
    assert event.request_id == "req-2"
    assert event.service == "inventory"
    assert event.exception_type == "LookupError"


def test_multiline_stack_trace_is_one_event_with_frames():
    events = normalized("stack_trace.txt")
    assert len(events) == 1
    assert events[0].level == "ERROR"
    assert events[0].exception_type == "RuntimeError"
    assert events[0].stack_frames[0].file == "/srv/worker.py"
    assert events[0].stack_frames[0].line == 42


def test_node_stack_trace_is_grouped_without_merging_independent_error_logs(tmp_path):
    node_path = tmp_path / "node-stack.txt"
    node_path.write_text(
        "TypeError: cart is undefined\n"
        "    at checkout (/srv/app.js:10:3)\n"
        "    at handler (/srv/router.js:22:7)\n",
        encoding="utf-8",
    )

    node_events = normalized_path(node_path)
    assert len(node_events) == 1
    assert node_events[0].exception_type == "TypeError"
    assert len(node_events[0].stack_frames) == 2

    independent_path = tmp_path / "independent-errors.txt"
    independent_path.write_text(
        "ERROR RuntimeError: first\nERROR RuntimeError: second\n",
        encoding="utf-8",
    )
    independent_records = list(
        stream_artifact_events(
            file_path=str(independent_path),
            artifact_format=ArtifactFormat.STACK_TRACE,
            source_file=independent_path.name,
        )
    )
    assert [record.event.exception_message for record in independent_records] == [
        "first",
        "second",
    ]


def test_nginx_access_log_normalization():
    access_event, apache_error = normalized("nginx.txt")
    assert access_event.level == "ERROR"
    assert access_event.http_status == 502
    assert access_event.endpoint == "/api/orders"
    assert access_event.host == "203.0.113.9"
    assert apache_error.level == "ERROR"
    assert apache_error.exception_type == "RuntimeError"


@pytest.mark.parametrize("fixture_name", ("docker.jsonl", "kubernetes.txt"))
def test_container_log_normalization(fixture_name):
    event = normalized(fixture_name)[0]
    assert event.level == "ERROR"
    assert event.service == "orders"
    assert event.exception_type in {"ConnectionError", "TimeoutError"}


def test_ci_cd_log_normalization():
    event = normalized("ci.txt")[0]
    assert event.level == "ERROR"
    assert event.service == "web"
    assert event.request_id == "deploy-1"


def test_syslog_normalization_for_rfc5424_and_rfc3164():
    events = normalized("syslog.txt")
    assert len(events) == 2
    assert events[0].level == "ERROR"
    assert events[0].host == "edge-1"
    assert events[0].service == "gateway"
    assert events[1].level == "WARNING"
    assert events[1].host == "worker-1"


def test_otlp_logs_and_spans_preserve_explicit_relationships():
    events = normalized("otlp.json")
    assert len(events) == 2

    log_event, span_event = events
    assert log_event.service == "catalog"
    assert log_event.trace_id == "trace-otel"
    assert log_event.span_id == "span-log"
    assert log_event.exception_type == "LookupError"

    assert span_event.trace_id == "trace-otel"
    assert span_event.span_id == "span-child"
    assert span_event.parent_span_id == "span-parent"
    assert span_event.service == "catalog"
    assert span_event.pod == "catalog-abc"
    assert span_event.http_status == 500
    assert span_event.endpoint == "/products/{id}"
    assert span_event.exception_type == "TimeoutError"
    assert filter_important_events(events) == events

    informational_span = ParsedEvent(
        line_number=3,
        raw_line="OpenTelemetry span: cache lookup",
        level="INFO",
        trace_id="trace-info",
        span_id="span-info",
        source_format="opentelemetry",
    )
    ordinary_info = ParsedEvent(
        line_number=4,
        raw_line="ordinary info",
        level="INFO",
    )
    assert filter_important_events([informational_span, ordinary_info]) == [
        informational_span
    ]


def test_browser_har_normalization():
    event = normalized("browser.har")[0]
    assert event.timestamp is not None
    assert event.level == "ERROR"
    assert event.http_status == 503
    assert event.endpoint == "https://example.test/api/cart"


def test_cloud_load_balancer_normalization():
    event = normalized("cloud_gateway.txt")[0]
    assert event.level == "ERROR"
    assert event.http_status == 502
    assert event.endpoint == "https://api.example.test/orders"


def test_cloudfront_fields_tsv_normalization_and_safe_resume():
    path = FIXTURES / "cloudfront.tsv"
    artifact_format = detect_artifact(path, filename="misleading.log")
    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )

    assert len(records) == 2
    first, second = records
    assert first.event.level == "ERROR"
    assert first.event.http_status == 502
    assert first.event.request_id == "edge-request-1"
    assert first.event.service == "cloudfront"
    assert first.event.host == "d111111abcdef8.cloudfront.net"
    assert first.event.endpoint == "/api/orders?order=123"
    assert first.event.timestamp is not None
    assert second.event.level == "INFO"
    assert second.event.endpoint == "/health"

    resumed = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
            start_offset=first.end_offset,
            start_artifact_line=first.artifact_line_number,
            global_line_number=first.global_end_line_number,
        )
    )
    assert [record.event.request_id for record in resumed] == ["edge-request-2"]
    assert resumed[0].artifact_line_number == second.artifact_line_number
    assert resumed[0].global_end_line_number == second.global_end_line_number
    assert resumed[0].end_offset == second.end_offset


def test_message_broker_text_uses_source_defaults_without_losing_fields():
    kafka_event, rabbitmq_event = normalized("message_broker.txt")

    assert kafka_event.source_format == "message_broker"
    assert kafka_event.level == "ERROR"
    assert kafka_event.request_id == "broker-1"
    assert kafka_event.service == "kafka"
    assert rabbitmq_event.level == "WARNING"
    assert rabbitmq_event.service == "rabbitmq"


def test_serverless_plain_text_preserves_lambda_request_relationships():
    events = normalized("serverless.txt")

    assert len(events) == 4
    assert all(event.source_format == "serverless" for event in events)
    assert all(event.request_id == "lambda-request-1" for event in events)
    assert events[0].service == "aws-lambda"
    assert events[0].level == "INFO"

    failure = events[1]
    assert failure.level == "ERROR"
    assert failure.service == "thumbnailer"
    assert failure.exception_type == "RuntimeError"
    assert filter_important_events(events) == [failure]


def test_cloudwatch_document_streams_individual_log_events():
    events = normalized("cloudwatch.json")
    assert len(events) == 2
    assert events[0].level == "ERROR"
    assert events[0].service == "thumbnailer"
    assert events[0].request_id == "lambda-1"
    assert events[1].level == "WARNING"


def test_database_slow_query_block_is_one_bounded_event():
    events = normalized("database.txt")
    assert len(events) == 1
    assert events[0].level == "WARNING"
    assert "SELECT * FROM orders" in events[0].raw_line


def test_compact_structured_wrapper_is_not_mistaken_for_jsonl(tmp_path):
    path = tmp_path / "compact-cloudwatch.json"
    path.write_text(
        '{"logEvents":['
        '{"timestamp":1786529483000,"message":"ERROR first"},'
        '{"timestamp":1786529484000,"message":"WARN second"}'
        "]}",
        encoding="utf-8",
    )
    artifact_format = detect_artifact(path, filename=path.name)
    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )
    assert [record.event.level for record in records] == ["ERROR", "WARNING"]


def test_cri_partial_fragments_are_reassembled_before_normalization(tmp_path):
    path = tmp_path / "cri-partial.log"
    path.write_text(
        "2026-08-12T10:12:00.000000Z stdout P "
        '{"level":"ERROR","message":"Connection\n'
        "2026-08-12T10:12:00.000001Z stdout F "
        'Error: refused","service":"orders"}\n',
        encoding="utf-8",
    )

    artifact_format = detect_artifact(path, filename=path.name)
    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )

    assert artifact_format.value == "container"
    assert len(records) == 1
    assert records[0].artifact_line_number == 2
    assert records[0].global_end_line_number == 2
    assert records[0].end_offset == path.stat().st_size
    assert records[0].event.line_number == 1
    assert records[0].event.raw_line == "ConnectionError: refused"
    assert records[0].event.exception_type == "ConnectionError"
    assert records[0].event.service == "orders"


def test_rfc5424_nil_timestamp_is_detected_and_normalized(tmp_path):
    path = tmp_path / "syslog-nil.log"
    path.write_text(
        "<36>1 - edge-2 scheduler 123 ID47 - queue delayed\n",
        encoding="utf-8",
    )

    events = normalized_path(path)

    assert events[0].timestamp is None
    assert events[0].level == "WARNING"
    assert events[0].host == "edge-2"
    assert events[0].service == "scheduler"


@pytest.mark.parametrize(
    "line",
    (
        "2026/08/12 10:11:22 [warn] 123#123: upstream response buffered",
        (
            "[Wed Aug 12 10:11:22.123456 2026] "
            "[core:warn] [pid 123] worker nearing capacity"
        ),
    ),
)
def test_web_server_warn_lines_are_content_detected(tmp_path, line):
    path = tmp_path / "web-warning.txt"
    path.write_text(f"{line}\n", encoding="utf-8")

    events = normalized_path(path)

    assert events[0].source_format == "web_server"
    assert events[0].level == "WARNING"


def test_log_praser_keeps_legacy_regex_constant_imports():
    from app.services import log_praser

    pattern_names = (
        "TIMESTAMP_PATTERN",
        "LOG_LEVEL_PATTERN",
        "TRACE_ID_PATTERN",
        "REQUEST_ID_PATTERN",
        "SERVICE_PATTERN",
        "MODULE_PATTERN",
        "HTTP_STATUS_PATTERN",
        "EXCEPTION_PATTERN",
    )

    assert all(hasattr(getattr(log_praser, name), "search") for name in pattern_names)


def test_log_praser_keeps_legacy_wildcard_exports():
    namespace: dict[str, object] = {}

    exec("from app.services.log_praser import *", namespace)  # noqa: S102

    assert callable(namespace["parse_log_line"])
    assert hasattr(namespace["TIMESTAMP_PATTERN"], "search")


def test_numeric_levels_are_source_specific_and_conversion_is_defensive():
    assert normalize_level(10) == "DEBUG"
    assert normalize_level(20) == "INFO"
    assert normalize_level(40) == "ERROR"

    assert normalize_otel_severity(1) == "TRACE"
    assert normalize_otel_severity(5) == "DEBUG"
    assert normalize_otel_severity(13) == "WARNING"
    assert normalize_otel_severity(21) == "CRITICAL"
    assert normalize_otel_severity(0) is None

    oversized_number = "9" * 5_000
    assert normalize_level(oversized_number) is None
    assert normalize_otel_severity(oversized_number) is None
    assert parse_timestamp(oversized_number) is None
    assert normalize_level(float("inf")) is None
    assert parse_timestamp(float("inf")) is None


def test_streamed_document_aliases_match_jsonl_priority(tmp_path):
    payload = {
        "msg": "secondary message",
        "message": "primary message",
        "severity": "WARNING",
        "level": "ERROR",
        "error": {"message": "details", "type": "LookupError"},
    }
    jsonl_path = tmp_path / "aliases.jsonl"
    document_path = tmp_path / "aliases.json"
    jsonl_path.write_text(f"{json.dumps(payload)}\n", encoding="utf-8")
    document_path.write_text(json.dumps([payload]), encoding="utf-8")

    jsonl_event = normalized_path(jsonl_path)[0]
    document_event = normalized_path(document_path)[0]

    assert document_event.raw_line == jsonl_event.raw_line == "primary message"
    assert document_event.level == jsonl_event.level == "ERROR"
    assert document_event.exception_type == jsonl_event.exception_type == "LookupError"
    assert (
        document_event.exception_message == jsonl_event.exception_message == "details"
    )


@pytest.mark.parametrize("as_document", (False, True))
def test_structured_exception_without_level_is_promoted(as_document, tmp_path):
    payload = {"error": {"type": "LookupError", "message": "sku missing"}}
    path = tmp_path / ("exception.json" if as_document else "exception.jsonl")
    serialized = json.dumps([payload] if as_document else payload)
    path.write_text(f"{serialized}\n", encoding="utf-8")

    event = normalized_path(path)[0]

    assert event.raw_line == "sku missing"
    assert event.level == "ERROR"
    assert event.exception_type == "LookupError"
    assert event.exception_message == "sku missing"


def test_api_gateway_direct_access_log_aliases(tmp_path):
    path = tmp_path / "api-gateway.json"
    path.write_text(
        json.dumps(
            [
                {
                    "requestId": "gateway-request-1",
                    "requestTimeEpoch": 1_786_529_483_000,
                    "httpMethod": "GET",
                    "resourcePath": "/orders/{id}",
                    "status": 502,
                }
            ]
        ),
        encoding="utf-8",
    )

    event = normalized_path(path)[0]

    assert event.timestamp is not None
    assert event.level == "ERROR"
    assert event.request_id == "gateway-request-1"
    assert event.endpoint == "/orders/{id}"
    assert event.http_status == 502


def test_crash_report_without_textual_log_level_is_error(tmp_path):
    path = tmp_path / "runtime-crash.txt"
    path.write_text(
        "Crash Report\n"
        "Process: frontend [123]\n"
        "Exception Type: EXC_BAD_ACCESS\n"
        "Termination Signal: Segmentation fault\n",
        encoding="utf-8",
    )

    events = normalized_path(path)

    assert len(events) == 1
    assert events[0].source_format == "stack_trace"
    assert events[0].level == "ERROR"


def test_otlp_scope_name_does_not_leak_to_a_scope_without_metadata(tmp_path):
    path = tmp_path / "multiple-otel-scopes.json"
    path.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {
                                "scope": {"name": "first.logs"},
                                "logRecords": [
                                    {
                                        "severityText": "ERROR",
                                        "body": {"stringValue": "first log"},
                                    }
                                ],
                            },
                            {
                                "logRecords": [
                                    {
                                        "severityText": "ERROR",
                                        "body": {"stringValue": "second log"},
                                    }
                                ]
                            },
                        ]
                    }
                ],
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "scope": {"name": "first.spans"},
                                "spans": [
                                    {
                                        "traceId": "trace-1",
                                        "spanId": "span-1",
                                        "name": "first span",
                                    }
                                ],
                            },
                            {
                                "spans": [
                                    {
                                        "traceId": "trace-2",
                                        "spanId": "span-2",
                                        "name": "second span",
                                    }
                                ]
                            },
                        ]
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    events = normalized_path(path)

    assert [event.module for event in events] == [
        "first.logs",
        None,
        "first.spans",
        None,
    ]
