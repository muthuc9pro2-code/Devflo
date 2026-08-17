from __future__ import annotations
import json
from typing import Any
from redis import Redis
from app.core.config import settings

def _channel(analysis_id: int) -> str:
    return f"analysis:{analysis_id}:events"

def publish_analysis_event(
    redis_client: Redis,
    analysis_id: int,
    event: str,
    data: dict[str, Any],
) -> None:
    payload = {
        "analysis_id": analysis_id,
        "event": event,
        "data": data,
    }

    redis_client.publish(
        _channel(analysis_id),
        json.dumps(
            payload,
            separators=(",", ":"),
            default=str,
        ),
    )

def publish_progress(
    redis_client: Redis,
    analysis_id: int,
    stage: str,
    message: str,
) -> None:
    publish_analysis_event(
        redis_client=redis_client,
        analysis_id=analysis_id,
        event="progress",
        data={
            "stage": stage,
            "message": message,
        },
    )

def publish_correlation_result(
    redis_client: Redis,
    analysis_id: int,
    payload: dict[str, Any],
) -> None:
    publish_analysis_event(
        redis_client=redis_client,
        analysis_id=analysis_id,
        event="correlation_result",
        data=payload,
    )