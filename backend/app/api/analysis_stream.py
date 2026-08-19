from __future__ import annotations
import asyncio
import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from app.services.analysis_events import (
    analysis_event_channel,
    redis_client,
)
from typing import Annotated, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.api.dependencies import get_current_verified_user
from app.db.database import get_db
from app.models.analysis import Analysis
from app.models.user import User
from app.tasks.analysis import compute_current_analysis_state


router = APIRouter()

async def _analysis_event_stream(analysis_id: int, initial_state: dict[str, Any]):
    # Reconnect correctness (no durable event replay needed): the very
    # first thing a (re)connecting client receives is a snapshot of
    # already-persisted state, computed once, before subscribing to the
    # live channel - so it can render the current status/progress
    # immediately instead of starting from 0 and waiting for the next live
    # tick. Historical intermediate progress ticks (22%, 23%, 24%, ...) are
    # deliberately NOT replayed; only future pubsub messages follow.
    yield (
        "event: state\n"
        f"data: {json.dumps(initial_state, separators=(',', ':'), default=str)}\n\n"
    )

    pubsub = redis_client.pubsub()
    pubsub.subscribe(analysis_event_channel(analysis_id))

    try:
        while True:
            message = pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message is None:
                await asyncio.sleep(0.1)
                continue

            data = json.loads(message["data"])

            yield (
                f"event: {data['event']}\n"
                f"data: {json.dumps(data['data'], separators=(',', ':'))}\n\n"
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

    initial_state = compute_current_analysis_state(db, analysis)

    return StreamingResponse(
        _analysis_event_stream(analysis_id, initial_state),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )