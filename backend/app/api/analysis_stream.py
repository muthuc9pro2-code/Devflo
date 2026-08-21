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

# SSE proxies/load balancers (and some browsers) can silently drop a
# connection that stays quiet too long. This is a transport-level
# keep-alive only - not a Devflo progress event, carries no JSON payload,
# and must never be interpreted by the frontend as analysis state.
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
    """No DB Session or Analysis ORM object is passed in - both are only
    ever held briefly, by this generator itself, never for the stream's
    full (potentially minutes-long) lifetime.

    Reconnect correctness (no durable event replay needed): subscribe to
    the live channel BEFORE reading the persisted-state snapshot. Redis
    pub/sub only delivers a message to clients already subscribed at
    publish time - it never queues for a not-yet-subscribed client and
    there is no history to replay. Snapshotting first would leave a window
    between the DB read and pubsub.subscribe() during which a Celery
    worker's publish (including the final investigation_result) would be
    silently and permanently lost, since nothing was listening yet.
    Subscribing first closes that window: the worst case is now a harmless
    duplicate (a buffered live message whose value the snapshot, read
    moments later, already reflects), never a dropped one. Historical
    intermediate progress ticks (22%, 23%, 24%, ...) that predate the
    subscription are still deliberately NOT replayed - only the current
    snapshot plus messages from here on.
    """
    channel = analysis_event_channel(analysis_id)
    pubsub = async_redis_client.pubsub()
    subscribed = False

    try:
        try:
            await pubsub.subscribe(channel)
            subscribed = True
        except RedisError:
            # Redis subscription itself failed (e.g. Redis is down). The
            # frontend already performs durable-state checks/reconnects -
            # this connection just degrades to "one DB snapshot, then
            # done" instead of hanging or raising into the ASGI server.
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

        # The DB Session below is scoped to this `with` block only - it is
        # already closed by the time the generator starts waiting for
        # future Redis messages further down.
        initial_state = await run_in_threadpool(
            _load_analysis_state,
            analysis_id,
        )

        if initial_state is None:
            return

        yield _sse_event("state", initial_state)

        # For a snapshot that is already terminal (completed/failed), there
        # is nothing further this stream can ever meaningfully deliver:
        # investigation_result (for "completed") is already inside the
        # snapshot just yielded, and a "failed" analysis will never
        # publish one. Ending here also means a stray buffered live
        # investigation_result for this same analysis is never relayed a
        # second time - DB state is the source of truth.
        if initial_state.get("status") in ("completed", "failed"):
            return

        # Guard against a stale queued progress tick from the
        # subscribe -> snapshot handoff window: DB state is always
        # committed before the corresponding live event is published (see
        # _publish_ingestion_progress / persist-before-publish in
        # _finalize_analysis_task), so any "progress" tick buffered from
        # before the snapshot read can only be <= what the snapshot
        # already reports. Forwarding it verbatim would visually regress
        # the client backward, so stale ticks below the snapshot's own
        # progress are dropped - a message that reflects real forward
        # progress always passes through untouched.
        min_progress = initial_state.get("progress")

        last_activity = monotonic()

        while True:
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
            except RedisError:
                # Redis disconnected/raised mid-stream. This only ends the
                # SSE connection - it must never mutate Analysis.status or
                # otherwise touch backend computation state. The browser's
                # existing reconnect path takes it from here.
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
                # Malformed/unexpected pub/sub payload must not crash the
                # stream or be silently reinterpreted - skip it and keep
                # waiting for the next message.
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

            if event_name == "investigation_result":
                # The single authoritative final payload - delivered
                # exactly once, then this stream is finished. No
                # heartbeat/progress follows it.
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
    # Short-lived ownership check only - this Session is closed well
    # before the (potentially minutes-long) StreamingResponse begins.
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
