from __future__ import annotations
import json
import logging
from time import monotonic
from redis.exceptions import RedisError
from app.services.analysis_events import (
    analysis_event_channel,
    async_redis_client,
)
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from app.api.dependencies import get_current_verified_user_id_for_stream
from app.db.database import sessionLocal
from app.models.analysis import Analysis
from app.tasks.analysis import compute_current_analysis_state
from starlette.concurrency import run_in_threadpool


logger = logging.getLogger(__name__)

router = APIRouter()

_SSE_HEARTBEAT_SECONDS = 15.0

def _sse_event(event: str, data: dict) -> str:
    return (
        f"event: {event}\n"
        f"data: {json.dumps(data, separators=(',', ':'), default=str)}\n\n"
    )

def _load_analysis_state(
    analysis_id: int,
) -> dict | None:
    with sessionLocal() as db:
        analysis = (
            db.query(Analysis)
            .filter(Analysis.id == analysis_id)
            .first()
        )

        if analysis is None:
            return None

        return compute_current_analysis_state(
            db,
            analysis,
        )

def _analysis_owned_by_user(
    analysis_id: int,
    user_id: int,
) -> bool:
    with sessionLocal() as db:
        return (
            db.query(Analysis.id)
            .filter(
                Analysis.id == analysis_id,
                Analysis.user_id == user_id,
            )
            .first()
            is not None
        )

async def _analysis_event_stream(analysis_id: int):
    channel = analysis_event_channel(analysis_id)
    pubsub = async_redis_client.pubsub()
    subscribed = False

    try:
        try:
            await pubsub.subscribe(channel)
            subscribed = True
        except RedisError:
           
            logger.warning(
                "Redis subscribe failed for analysis_id=%s; serving a "
                "DB-only snapshot",
                analysis_id,
                exc_info=True,
            )
            initial_state = await run_in_threadpool(
                _load_analysis_state,
                analysis_id,
            )

            if initial_state is None:
                return

            yield _sse_event("state", initial_state)
            return

        initial_state = await run_in_threadpool(
            _load_analysis_state,
            analysis_id,
        )

        if initial_state is None:
            return

        yield _sse_event("state", initial_state)

        if initial_state.get("status") in ("completed", "failed", "cancelled"):
            return

        min_progress = initial_state.get("progress")

        last_activity = monotonic()

        while True:
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
            except RedisError:
                logger.warning(
                    "Redis error while streaming analysis_id=%s; "
                    "terminating stream",
                    analysis_id,
                    exc_info=True,
                )
                return

            now = monotonic()

            if message is None:
                if now - last_activity >= _SSE_HEARTBEAT_SECONDS:
                    last_activity = now
                    yield ": keep-alive\n\n"
                continue

            try:
                data = json.loads(message["data"])
                event_name = data["event"]
                event_data = data["data"]
            except (KeyError, TypeError, ValueError):
                logger.warning(
                    "Malformed Redis pub/sub message for analysis_id=%s; "
                    "skipping",
                    analysis_id,
                )
                continue

            last_activity = now

            if event_name == "progress" and min_progress is not None:
                progress = event_data.get("progress")
                if progress is not None:
                    if progress < min_progress:
                        continue
                    min_progress = progress

            if event_name in ("investigation_result", "cancelled"):
                yield _sse_event(event_name, event_data)
                return

            yield _sse_event(event_name, event_data)

    finally:
        if subscribed:
            try:
                await pubsub.unsubscribe(channel)
            except Exception:
                logger.warning(
                    "Redis unsubscribe failed for analysis_id=%s",
                    analysis_id,
                    exc_info=True,
                )

        try:
            await pubsub.aclose()
        except Exception:
            logger.warning(
                "Redis pubsub close failed for analysis_id=%s",
                analysis_id,
                exc_info=True,
            )

@router.get(
    "/analyses/{analysis_id}/events",
    response_class=StreamingResponse,
)
async def stream_analysis_events(
    analysis_id: int,
    current_user_id: Annotated[int, Depends(get_current_verified_user_id_for_stream)],
) -> StreamingResponse:
    is_owned = await run_in_threadpool(
        _analysis_owned_by_user,
        analysis_id,
        current_user_id,
    )

    if not is_owned:
        raise HTTPException(
            status_code=404,
            detail="Analysis not found",
        )

    return StreamingResponse(
        _analysis_event_stream(analysis_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
