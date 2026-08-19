"""SSE reconnect correctness: on (re)connect the client must get current
persisted state immediately, then only future live events - no durable
replay of historical progress ticks, ownership/auth preserved, no new
endpoint (the existing GET /analyses/{id}/events is extended in place).
"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from app.api import analysis_stream


# --- ownership/auth is preserved --------------------------------------


def test_stream_endpoint_404s_for_an_analysis_the_user_does_not_own():
    db = Mock()
    db.query.return_value.filter.return_value.first.return_value = None  # ownership filter excludes it

    import asyncio

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            analysis_stream.stream_analysis_events(
                analysis_id=999, db=db, current_user=SimpleNamespace(id=4)
            )
        )

    assert error.value.status_code == 404


def test_stream_endpoint_computes_current_state_for_the_owned_analysis(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=42, status="processing")
    db.query.return_value.filter.return_value.first.return_value = analysis

    captured = {}

    def fake_compute(passed_db, passed_analysis):
        captured["db"] = passed_db
        captured["analysis"] = passed_analysis
        return {"analysis_id": 42, "status": "processing", "progress": 10}

    monkeypatch.setattr(analysis_stream, "compute_current_analysis_state", fake_compute)

    import asyncio

    response = asyncio.run(
        analysis_stream.stream_analysis_events(
            analysis_id=42, db=db, current_user=SimpleNamespace(id=4)
        )
    )

    assert captured["analysis"] is analysis
    assert captured["db"] is db
    assert response.status_code == 200


# --- the state event is yielded first, before any live subscription -------


@pytest.mark.asyncio
async def test_state_event_is_emitted_before_subscribing_to_redis():
    initial_state = {"analysis_id": 7, "status": "processing", "progress": 42}
    generator = analysis_stream._analysis_event_stream(7, initial_state)

    first_chunk = await generator.__anext__()

    assert first_chunk.startswith("event: state\n")
    assert '"progress":42' in first_chunk
    assert '"status":"processing"' in first_chunk
    # Generator has not touched redis_client.pubsub() yet - that call sits
    # textually AFTER the first yield, so getting exactly one chunk out
    # proves this ordering without needing a real/fake Redis connection.
    await generator.aclose()


@pytest.mark.asyncio
async def test_state_event_never_reports_progress_100():
    initial_state = {"analysis_id": 7, "status": "completed", "progress": 99}
    generator = analysis_stream._analysis_event_stream(7, initial_state)

    first_chunk = await generator.__anext__()

    assert '"progress":99' in first_chunk
    assert "100" not in first_chunk
    await generator.aclose()
