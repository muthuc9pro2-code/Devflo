from __future__ import annotations

import json
from typing import Any

from redis import Redis


redis_client = Redis.from_url(
    "redis://localhost:6379/0",
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

    redis_client.publish(
        analysis_event_channel(analysis_id),
        json.dumps(
            payload,
            separators=(",", ":"),
            default=str,
        ),
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


def publish_correlation_result(
    analysis_id: int,
    payload: dict[str, Any],
) -> None:
    publish_analysis_event(
        analysis_id=analysis_id,
        event="correlation_result",
        data=payload,
    )


def publish_investigation_result(
    analysis_id: int,
    payload: dict[str, Any],
) -> None:
    """Final investigation result, common to all three investigation
    outcomes (correlated/simple/zero_evidence) - distinguished by the
    payload's own `investigation_path` field. Published in addition to the
    existing `correlation_result` event (still published, unchanged, for
    the correlated path) rather than replacing it, so this is purely
    additive to the existing SSE contract."""
    publish_analysis_event(
        analysis_id=analysis_id,
        event="investigation_result",
        data=payload,
    )