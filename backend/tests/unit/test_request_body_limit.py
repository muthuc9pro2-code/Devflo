import json
from pathlib import Path

import pytest

from app.core.processing_config import MAX_ANALYSIS_REQUEST_BODY_BYTES
from app.core.request_body_limit import RequestBodyLimitMiddleware


def _scope(*, path="/analysis/upload", method="POST", content_length=None):
    headers = []
    if content_length is not None:
        headers.append((b"content-length", str(content_length).encode("ascii")))
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }


async def _run(
    *,
    max_body_size,
    chunks,
    content_length=None,
    path="/analysis/upload",
    method="POST",
):
    downstream_completed = False
    receive_calls = 0
    sent = []

    messages = [
        {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }
        for index, chunk in enumerate(chunks)
    ]
    if not messages:
        messages = [{"type": "http.request", "body": b"", "more_body": False}]

    async def receive():
        nonlocal receive_calls
        receive_calls += 1
        return messages.pop(0)

    async def send(message):
        sent.append(message)

    async def downstream(_scope, receive_from_middleware, send_from_middleware):
        nonlocal downstream_completed
        while True:
            message = await receive_from_middleware()
            if not message.get("more_body", False):
                break
        downstream_completed = True
        await send_from_middleware(
            {"type": "http.response.start", "status": 204, "headers": []}
        )
        await send_from_middleware({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        path="/analysis/upload",
        max_body_size=max_body_size,
    )

    await middleware(
        _scope(path=path, method=method, content_length=content_length),
        receive,
        send,
    )

    return downstream_completed, receive_calls, sent


def _response_status(sent):
    return next(
        message["status"] for message in sent if message["type"] == "http.response.start"
    )


def _response_json(sent):
    body = b"".join(
        message.get("body", b"")
        for message in sent
        if message["type"] == "http.response.body"
    )
    return json.loads(body)


@pytest.mark.asyncio
async def test_oversized_content_length_is_rejected_before_body_is_read():
    completed, receive_calls, sent = await _run(
        max_body_size=10,
        chunks=[b"not-read"],
        content_length=11,
    )

    assert completed is False
    assert receive_calls == 0
    assert _response_status(sent) == 413
    assert "configured body limit" in _response_json(sent)["detail"]


@pytest.mark.asyncio
async def test_actual_streamed_bytes_are_capped_without_trusting_content_length():
    completed, receive_calls, sent = await _run(
        max_body_size=10,
        chunks=[b"123456", b"78901"],
        # Deliberately dishonest.
        content_length=5,
    )

    assert completed is False
    assert receive_calls == 2
    assert _response_status(sent) == 413


@pytest.mark.asyncio
async def test_exactly_at_limit_is_allowed():
    completed, _calls, sent = await _run(
        max_body_size=10,
        chunks=[b"1234", b"567890"],
    )

    assert completed is True
    assert _response_status(sent) == 204


@pytest.mark.asyncio
async def test_unrelated_routes_bypass_upload_body_limit():
    completed, _calls, sent = await _run(
        max_body_size=1,
        chunks=[b"much larger than one byte"],
        path="/analysis/history",
        method="GET",
    )

    assert completed is True
    assert _response_status(sent) == 204


def test_caddy_upload_ceiling_matches_backend_raw_body_limit():
    repo_root = Path(__file__).resolve().parents[3]
    caddyfile = (repo_root / "frontend" / "Caddyfile").read_text(encoding="utf-8")

    assert f"max_size {MAX_ANALYSIS_REQUEST_BODY_BYTES}" in caddyfile
