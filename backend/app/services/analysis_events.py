from __future__ import annotations

import json
import logging
from typing import Any

from redis import Redis
from redis import asyncio as redis_asyncio
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

_REDIS_URL = "redis://localhost:6379/0"

# Synchronous client: used by normal request handlers and Celery worker code
# (publishing is a fire-and-forget side effect of already-committed DB state,
# never awaited from an async context).
redis_client = Redis.from_url(
    _REDIS_URL,
    decode_responses=True,
)

# Separate async client: used ONLY by the FastAPI SSE subscriber
# (analysis_stream.py). A sync Redis call from an async generator would
# block the event loop for every other concurrent request this worker is
# serving - this client exists purely to avoid that, not to change what is
# published or how.
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
    """Best-effort live notification only. Devflo's deterministic result is
    always persisted (result_snapshot + status="completed") BEFORE this is
    ever called - see _finalize_analysis_task's persist-before-publish
    ordering. So a Redis/transport failure here must never propagate: it
    would otherwise turn an already-successful, already-persisted
    deterministic result into a failed analysis for a reason that has
    nothing to do with the computation itself. SSE is ephemeral - the
    frontend's durable DB-backed reconnect path is what actually recovers
    from this, not a retry here."""
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
    """The single authoritative full final result, common to all three
    investigation outcomes (correlated/simple/zero_evidence) - distinguished
    by the payload's own `investigation_path` field. Published exactly
    once per analysis. There used to also be a `correlation_result` event
    carrying the identical correlated-path payload a second time (no
    frontend code ever consumed it) - removed rather than kept "for
    compatibility" against a contract nothing actually depended on."""
    publish_analysis_event(
        analysis_id=analysis_id,
        event="investigation_result",
        data=payload,
    )


def publish_artifact_outcome(
    analysis_id: int,
    payload: dict[str, Any],
) -> None:
    """One artifact's outcome (unsupported / duplicate / zero-evidence),
    published as soon as it is deterministically known - so the frontend
    does not have to wait for final correlation to learn that a specific
    file needs no further processing. Purely additive: the final
    investigation_result.artifacts[] built by
    investigation_context.build_artifact_outcome_payload() remains the
    authoritative, complete list; this only surfaces individual entries
    from that same contract earlier."""
    publish_analysis_event(
        analysis_id=analysis_id,
        event="artifact_outcome",
        data=payload,
    )