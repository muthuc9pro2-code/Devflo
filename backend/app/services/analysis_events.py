from __future__ import annotations
import json
import logging
from typing import Any
from redis import Redis
from redis import asyncio as redis_asyncio
from redis.exceptions import RedisError
from app.core.config import Settings

logger = logging.getLogger(__name__)

_REDIS_URL = Settings.REDIS_EVENTS_URL

redis_client = Redis.from_url(
    _REDIS_URL,
    decode_responses=True,
)

async_redis_client = redis_asyncio.Redis.from_url(
    _REDIS_URL,
    decode_responses=True,
)

def analysis_event_channel(analysis_id: int) -> str:
    return f"analysis:{analysis_id}:events"


def publish_analysis_event(
    analysis_id: int,
    event: str,
    data: dict[str, Any],
) -> None:
    payload = {
        "analysis_id": analysis_id,
        "event": event,
        "data": data,
    }

    try:
        redis_client.publish(
            analysis_event_channel(analysis_id),
            json.dumps(
                payload,
                separators=(",", ":"),
                default=str,
            ),
        )
    except RedisError:
        logger.warning(
            "Redis publish failed | analysis_id=%s | event=%s",
            analysis_id,
            event,
            exc_info=True,
        )


def publish_progress(
    analysis_id: int,
    stage: str,
    message: str,
    progress: int | None = None,
) -> None:
    data: dict[str, Any] = {
        "stage": stage,
        "message": message,
    }

    if progress is not None:
        data["progress"] = progress

    publish_analysis_event(
        analysis_id=analysis_id,
        event="progress",
        data=data,
    )


def publish_investigation_result(
    analysis_id: int,
    payload: dict[str, Any],
) -> None:
    publish_analysis_event(
        analysis_id=analysis_id,
        event="investigation_result",
        data=payload,
    )


def publish_artifact_outcome(
    analysis_id: int,
    payload: dict[str, Any],
) -> None:
    publish_analysis_event(
        analysis_id=analysis_id,
        event="artifact_outcome",
        data=payload,
    )