from __future__ import annotations
import asyncio
import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from app.services.analysis_events import (
    analysis_event_channel,
    redis_client,
)
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.api.dependencies import get_current_verified_user
from app.db.database import get_db
from app.models.analysis import Analysis
from app.models.user import User
from app.tasks.analysis import compute_current_analysis_state


router = APIRouter()

async def _analysis_event_stream(analysis_id: int, db: Session, analysis: Analysis):
    # Reconnect correctness (no durable event replay needed): subscribe to
    # the live channel BEFORE reading the persisted-state snapshot. Redis
    # pub/sub only delivers a message to clients already subscribed at
    # publish time - it never queues for a not-yet-subscribed client and
    # there is no history to replay. Snapshotting first (the previous
    # order) left a window between the DB read and pubsub.subscribe()
    # during which a Celery worker's publish (including the final
    # investigation_result) would be silently and permanently lost, since
    # nothing was listening yet. Subscribing first closes that window: the
    # worst case is now a harmless duplicate (a buffered live message whose
    # value the snapshot, read moments later, already reflects), never a
    # dropped one. Historical intermediate progress ticks (22%, 23%,
    # 24%, ...) that predate the subscription are still deliberately NOT
    # replayed - only the current snapshot plus messages from here on.
    pubsub = redis_client.pubsub()
    pubsub.subscribe(analysis_event_channel(analysis_id))

    try:
        initial_state = compute_current_analysis_state(db, analysis)
        yield (
            "event: state\n"
            f"data: {json.dumps(initial_state, separators=(',', ':'), default=str)}\n\n"
        )

        # Guards for messages that were already queued (buffered by Redis
        # for this now-subscribed client) during the subscribe -> snapshot
        # handoff window:
        #
        # DB state is always committed before the corresponding live event
        # is published (see _publish_ingestion_progress / persist-before-
        # publish in _finalize_analysis_task), so any "progress" tick
        # buffered from before the snapshot read can only be <= what the
        # snapshot already reports. Forwarding it verbatim would visually
        # regress the client backward, so stale ticks below the snapshot's
        # own progress are dropped (never forwarded below the high-water
        # mark) - a message that reflects real forward progress always
        # passes through untouched.
        #
        # If the snapshot itself is already status="completed", it already
        # carries the authoritative investigation_result - DB state is the
        # source of truth, so a redundant buffered investigation_result
        # live event (there should not realistically be one, since the
        # task has already finished) must not be relayed a second time.
        status_already_terminal = initial_state.get("status") == "completed"
        min_progress = None if status_already_terminal else initial_state.get("progress")
        result_already_delivered = status_already_terminal

        while True:
            message = pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message is None:
                await asyncio.sleep(0.1)
                continue

            data = json.loads(message["data"])
            event_name = data["event"]
            event_data = data["data"]

            if event_name == "progress" and min_progress is not None:
                progress = event_data.get("progress")
                if progress is not None:
                    if progress < min_progress:
                        continue
                    min_progress = progress

            if event_name == "investigation_result":
                if result_already_delivered:
                    continue
                result_already_delivered = True

            yield (
                f"event: {event_name}\n"
                f"data: {json.dumps(event_data, separators=(',', ':'))}\n\n"
            )

    finally:
        pubsub.unsubscribe(analysis_event_channel(analysis_id))
        pubsub.close()

@router.get(
    "/analyses/{analysis_id}/events",
    response_class=StreamingResponse,
)
async def stream_analysis_events(
    analysis_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_verified_user)],
) -> StreamingResponse:
    analysis = (
        db.query(Analysis)
        .filter(
            Analysis.id == analysis_id,
            Analysis.user_id == current_user.id,
        )
        .first()
    )

    if analysis is None:
        raise HTTPException(
            status_code=404,
            detail="Analysis not found",
        )

    return StreamingResponse(
        _analysis_event_stream(analysis_id, db, analysis),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )