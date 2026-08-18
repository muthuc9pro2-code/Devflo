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


router = APIRouter()

async def _analysis_event_stream(analysis_id: int):
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

    return StreamingResponse(
        _analysis_event_stream(analysis_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )