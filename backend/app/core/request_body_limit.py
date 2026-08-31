from collections.abc import Awaitable, Callable
from typing import Any

from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

from app.core.processing_config import ANALYSIS_REQUEST_BODY_LIMIT_DETAIL


class _RequestBodyLimitExceeded(HTTPException):
    def __init__(self) -> None:
        super().__init__(
            status_code=413,
            detail=ANALYSIS_REQUEST_BODY_LIMIT_DETAIL,
        )


class RequestBodyLimitMiddleware:
    """Bound the raw body of one expensive HTTP upload route.

    Content-Length is used for an immediate rejection when available, but the
    ASGI receive stream is also counted so chunked requests or a dishonest
    Content-Length cannot bypass the ceiling.

    The middleware runs before FastAPI's multipart parser, so the parser cannot
    first spool an arbitrarily large upload and only then discover Devflo's
    application-level size limits.
    """

    def __init__(
        self,
        app: Callable[..., Awaitable[Any]],
        *,
        path: str,
        max_body_size: int,
        method: str = "POST",
    ) -> None:
        self.app = app
        self.path = path
        self.max_body_size = max_body_size
        self.method = method.upper()

    async def __call__(self, scope, receive, send) -> None:
        if (
            scope.get("type") != "http"
            or scope.get("method", "").upper() != self.method
            or scope.get("path") != self.path
        ):
            await self.app(scope, receive, send)
            return

        content_length = _content_length(scope)
        if content_length is not None and content_length > self.max_body_size:
            await self._reject(scope, receive, send)
            return

        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_size:
                    raise _RequestBodyLimitExceeded()
            return message

        try:
            await self.app(scope, limited_receive, send)
        except _RequestBodyLimitExceeded:
            await self._reject(scope, receive, send)

    async def _reject(self, scope, receive, send) -> None:
        response = JSONResponse(
            status_code=413,
            content={"detail": ANALYSIS_REQUEST_BODY_LIMIT_DETAIL},
        )
        await response(scope, receive, send)


def _content_length(scope) -> int | None:
    for name, value in scope.get("headers", []):
        if name.lower() != b"content-length":
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None
    return None
