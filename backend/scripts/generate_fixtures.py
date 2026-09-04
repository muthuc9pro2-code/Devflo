from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"

SERVICES = ["api", "orders", "payments", "inventory", "checkout", "worker", "catalog"]
HOSTS = [f"host-{i}" for i in range(8)]
ENDPOINTS = ["/orders", "/health", "/checkout", "/cart", "/products/{id}", "/users/{id}"]
EXCEPTIONS = ["ConnectionError", "TimeoutError", "ValueError", "LookupError", "RuntimeError"]

def _target_bytes(size_mib: float) -> int:
    return int(size_mib * 1024 * 1024)

def gen_web_server(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        host = f"203.0.113.{i % 250}"
        ts = f"12/Aug/2026:10:{(i // 60) % 60:02d}:{i % 60:02d} +0000"
        endpoint = ENDPOINTS[i % len(ENDPOINTS)]
        if i % 11 == 0:
            status = 502
        elif i % 7 == 0:
            status = 404
        else:
            status = 200
        line = f'{host} - - [{ts}] "GET {endpoint} HTTP/1.1" {status} {100 + i % 900} "-" "curl/8"\n'
        if i % 23 == 0:
            line += (
                f"[Wed Aug 12 10:11:{i % 60:02d}.{i % 1000:06d} 2026] "
                f"[core:error] [pid {1000 + i}] RuntimeError: worker {i} failed\n"
            )
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_syslog(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        host = HOSTS[i % len(HOSTS)]
        if i % 3 == 0:
            line = (
                f"<11>1 2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}Z {host} "
                f"gateway {1000 + i} ID{i} - upstream ConnectionError: refused id={i}\n"
            )
        elif i % 5 == 0:
            line = f"<36>Aug 12 10:{(i // 60) % 60:02d}:{i % 60:02d} {host} scheduler[{321 + i}]: queue depth warning id={i}\n"
        else:
            line = f"<134>Aug 12 10:{(i // 60) % 60:02d}:{i % 60:02d} {host} app[{321 + i}]: heartbeat ok id={i}\n"
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_container(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        ts = f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}.{i % 1000000:06d}Z"
        stream = "stderr" if i % 6 == 0 else "stdout"
        service = SERVICES[i % len(SERVICES)]
        if i % 6 == 0:
            body = {
                "level": "error",
                "message": f"{EXCEPTIONS[i % len(EXCEPTIONS)]}: upstream timeout id={i}",
                "service": service,
            }
        else:
            body = {"level": "info", "message": f"request handled id={i}", "service": service}
        line = f"{ts} {stream} F {json.dumps(body)}\n"
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_database(size_mib: float) -> bytes:
    blocks = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        ts = f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}Z"
        query_time = 0.001 * (i % 50) if i % 4 != 0 else 5.0 + (i % 20)
        block = (
            f"# Time: {ts}\n"
            f"# User@Host: app[app] @ db-{i % 4}\n"
            f"# Query_time: {query_time:.6f}  Lock_time: 0.000100 Rows_sent: {i % 5} Rows_examined: {i % 500}\n"
            f"SELECT * FROM orders WHERE id = {i} AND status = 'pending';\n"
        )
        blocks.append(block)
        total += len(block)
    return "".join(blocks).encode()

def gen_ci_cd(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        ts = f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}Z"
        if i % 13 == 0:
            line = f"{ts} ##[error] deployment failed for service={SERVICES[i % len(SERVICES)]} request_id=deploy-{i}\n"
        elif i % 7 == 0:
            line = f"{ts} ##[warning] retrying step {i} for service={SERVICES[i % len(SERVICES)]}\n"
        else:
            line = f"{ts} ##[section] Running step {i} for service={SERVICES[i % len(SERVICES)]}\n"
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_message_broker(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        ts = f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}.{i % 1000:03d}Z"
        if i % 9 == 0:
            line = (
                f"{ts} ERROR [KafkaConsumer clientId=orders-consumer-{i}, groupId=checkout] "
                f"org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - "
                f"Offset commit failed request_id=broker-{i}\n"
            )
        elif i % 5 == 0:
            line = f"{ts} WARN rabbitmq AMQP connection blocked while publishing queue=orders-{i % 8}\n"
        else:
            line = f"{ts} INFO kafka consumer group checkout heartbeat id={i} partition={i % 12}\n"
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_serverless(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        rid = f"lambda-request-{i}"
        ts = f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}.{i % 1000:03d}Z"
        block = [f"START RequestId: {rid} Version: $LATEST\n"]
        if i % 8 == 0:
            block.append(
                f"{ts}\t{rid}\tERROR\tservice=thumbnailer {EXCEPTIONS[i % len(EXCEPTIONS)]}: resize failed id={i}\n"
            )
        else:
            block.append(f"{ts}\t{rid}\tINFO\tservice=thumbnailer processed id={i}\n")
        block.append(f"END RequestId: {rid}\n")
        block.append(
            f"REPORT RequestId: {rid} Duration: {100 + i % 900:.2f} ms "
            f"Billed Duration: {101 + i % 900} ms Memory Size: 256 MB Max Memory Used: {128 + i % 100} MB\n"
        )
        chunk = "".join(block)
        lines.append(chunk)
        total += len(chunk)
    return "".join(lines).encode()

def gen_cloud_gateway(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        ts = f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}.{i % 1000000:06d}Z"
        status = 502 if i % 15 == 0 else 200
        endpoint = ENDPOINTS[i % len(ENDPOINTS)]
        line = (
            f'http {ts} app/gateway/{i % 4} 203.0.113.{i % 250}:50000 10.0.0.{i % 250}:8080 '
            f'0.001 0.002 0.001 {status} {status} 120 57 '
            f'"GET https://api.example.test{endpoint} HTTP/1.1" "curl/8" - - arn:trace Root=trace-{i}\n'
        )
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_stack_trace(size_mib: float) -> bytes:
    blocks = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        service = SERVICES[i % len(SERVICES)]
        if i % 4 == 0:
            block = (
                f"2026-08-12 10:11:{i % 60:02d} ERROR service={service} Traceback (most recent call last):\n"
                f'  File "/srv/{service}.py", line {10 + i % 200}, in run\n'
                f'    raise RuntimeError("queue unavailable {i}")\n'
                f"RuntimeError: queue unavailable {i}\n"
            )
        else:
            block = f"2026-08-12 10:11:{i % 60:02d} INFO service={service} heartbeat ok id={i}\n"
        blocks.append(block)
        total += len(block)
    return "".join(blocks).encode()

def gen_generic(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        ts = f"2026-08-10T10:00:{i % 60:02d}.{i % 1000:03d}"
        cycle = i % 7
        if cycle == 0:
            line = (
                f"{ts} INFO service=api trace_id=tr-{i:06d} request_id=req-{i:06d} "
                f"GET /health status=200 duration_ms=12\n"
            )
        elif cycle == 1:
            line = (
                f'{ts} WARNING service=orders trace_id=tr-{i:06d} request_id=req-{i:06d} '
                f'endpoint=/checkout message="slow downstream response"\n'
            )
        elif cycle == 2:
            line = (
                f'{ts} ERROR service=payments trace_id=tr-{i:06d} request_id=req-{i:06d} '
                f'exception=ConnectionError message="connection failed to host 10.0.0.{i % 250}:5432"\n'
            )
        elif cycle == 3:
            line = (
                f'{ts} CRITICAL service=database trace_id=tr-{i:06d} '
                f'exception=TimeoutError message="database timeout after {1000 + i}ms"\n'
            )
        elif cycle == 4:
            line = f'{ts} ERROR service=inventory request_id=req-{i:06d} exception=ValueError message="invalid product id {i}"\n'
        elif cycle == 5:
            line = f'{ts} ERROR service=worker exception=RuntimeError message="job {i} failed while processing item {i}"\n'
        else:
            line = f"{ts} malformed line without structured identifiers number={i}\n"
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_json_lines(size_mib: float) -> bytes:
    lines = []
    target = _target_bytes(size_mib)
    total = 0
    i = 0
    while total < target:
        i += 1
        level = "ERROR" if i % 6 == 0 else ("WARN" if i % 4 == 0 else "INFO")
        record = {
            "timestamp": f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}.{i % 1000:03d}Z",
            "level": level,
            "service": SERVICES[i % len(SERVICES)],
            "trace_id": f"tr-{i:08d}",
            "request_id": f"req-{i:08d}",
            "message": f"handling request {i} for user {i % 1000}",
        }
        if level == "ERROR":
            record["exception_type"] = EXCEPTIONS[i % len(EXCEPTIONS)]
            record["exception_message"] = f"failure on request {i}"
        line = json.dumps(record) + "\n"
        lines.append(line)
        total += len(line)
    return "".join(lines).encode()

def gen_otlp(size_mib: float) -> bytes:
    target = _target_bytes(size_mib)
    log_records = []
    spans = []
    i = 0
    approx_record_bytes = 420
    count = max(1, target // (approx_record_bytes * 2))
    while i < count:
        i += 1
        severity = "ERROR" if i % 5 == 0 else "INFO"
        log_records.append(
            {
                "timeUnixNano": str(1786529481000000000 + i * 1000000),
                "severityText": severity,
                "body": {"stringValue": f"{EXCEPTIONS[i % len(EXCEPTIONS)]}: item {i} missing"},
                "traceId": f"trace-{i:08x}",
                "spanId": f"span-log-{i:08x}",
                "attributes": [
                    {"key": "exception.type", "value": {"stringValue": EXCEPTIONS[i % len(EXCEPTIONS)]}},
                    {"key": "exception.message", "value": {"stringValue": f"item {i} missing"}},
                ],
            }
        )
        spans.append(
            {
                "traceId": f"trace-{i:08x}",
                "spanId": f"span-child-{i:08x}",
                "parentSpanId": f"span-parent-{i:08x}",
                "name": f"GET /products/{{id}} {i}",
                "startTimeUnixNano": str(1786529482000000000 + i * 1000000),
                "status": {
                    "code": "STATUS_CODE_ERROR" if i % 7 == 0 else "STATUS_CODE_OK",
                    "message": "request failed" if i % 7 == 0 else "ok",
                },
                "attributes": [
                    {"key": "http.route", "value": {"stringValue": "/products/{id}"}},
                    {"key": "http.response.status_code", "value": {"intValue": "500" if i % 7 == 0 else "200"}},
                ],
            }
        )
    document = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "catalog"}},
                        {"key": "host.name", "value": {"stringValue": "catalog-1"}},
                    ]
                },
                "scopeLogs": [{"scope": {"name": "catalog.logger"}, "logRecords": log_records}],
            }
        ],
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "catalog"}},
                        {"key": "k8s.pod.name", "value": {"stringValue": "catalog-abc"}},
                    ]
                },
                "scopeSpans": [{"scope": {"name": "catalog.http"}, "spans": spans}],
            }
        ],
    }
    return json.dumps(document).encode()

def gen_browser_har(size_mib: float) -> bytes:
    target = _target_bytes(size_mib)
    entries = []
    i = 0
    approx_entry_bytes = 220
    count = max(1, target // approx_entry_bytes)
    while i < count:
        i += 1
        status = 503 if i % 11 == 0 else (404 if i % 7 == 0 else 200)
        entries.append(
            {
                "startedDateTime": f"2026-08-12T10:{(i // 60) % 60:02d}:{i % 60:02d}Z",
                "request": {"method": "GET", "url": f"https://example.test/api/cart/{i}"},
                "response": {"status": status},
                "serverIPAddress": f"203.0.113.{i % 250}",
                "time": i % 500,
            }
        )
    document = {"log": {"version": "1.2", "entries": entries}}
    return json.dumps(document).encode()

GENERATORS = {
    "generic": gen_generic,
    "json": gen_json_lines,
    "stack_trace": gen_stack_trace,
    "web_server": gen_web_server,
    "container": gen_container,
    "database": gen_database,
    "cloud_gateway": gen_cloud_gateway,
    "ci_cd": gen_ci_cd,
    "browser": gen_browser_har,
    "message_broker": gen_message_broker,
    "serverless": gen_serverless,
    "syslog": gen_syslog,
    "opentelemetry": gen_otlp,
}

EXTENSIONS = {
    "generic": "log",
    "json": "jsonl",
    "stack_trace": "log",
    "web_server": "log",
    "container": "log",
    "database": "log",
    "cloud_gateway": "log",
    "ci_cd": "log",
    "browser": "har",
    "message_broker": "log",
    "serverless": "log",
    "syslog": "log",
    "opentelemetry": "json",
}

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size-mib", type=float, default=10)
    parser.add_argument("--formats", type=str, default="")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    wanted = set(args.formats.split(",")) if args.formats else set(GENERATORS)

    for name, gen in GENERATORS.items():
        if name not in wanted:
            continue
        data = gen(args.size_mib)
        suffix = EXTENSIONS[name]
        size_label = f"{args.size_mib:g}mib"
        out_path = OUT_DIR / f"{name}_{size_label}.{suffix}"
        out_path.write_bytes(data)
        print(f"{name}: {len(data):,} bytes -> {out_path}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
