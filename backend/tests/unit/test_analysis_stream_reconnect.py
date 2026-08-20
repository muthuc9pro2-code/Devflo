"""SSE reconnect correctness: on (re)connect the client must get current
persisted state immediately, then only future live events - no durable
replay of historical progress ticks, ownership/auth preserved, no new
endpoint (the existing GET /analyses/{id}/events is extended in place).
"""
import asyncio
import json
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


def test_stream_endpoint_passes_db_and_analysis_to_the_event_stream(monkeypatch):
    """stream_analysis_events is now just ownership-check + glue: the
    snapshot itself is computed lazily, inside _analysis_event_stream,
    only once the stream is actually driven (see Additional Requirement E
    below for why that ordering matters)."""
    db = Mock()
    analysis = SimpleNamespace(id=42, status="processing")
    db.query.return_value.filter.return_value.first.return_value = analysis

    async def _empty_gen():
        return
        yield  # pragma: no cover - never reached; unstarted generator only

    stream_mock = Mock(return_value=_empty_gen())
    monkeypatch.setattr(analysis_stream, "_analysis_event_stream", stream_mock)

    import asyncio

    response = asyncio.run(
        analysis_stream.stream_analysis_events(
            analysis_id=42, db=db, current_user=SimpleNamespace(id=4)
        )
    )

    stream_mock.assert_called_once_with(42, db, analysis)
    assert response.status_code == 200


# --- Additional Requirement E: subscribe before snapshotting --------------
#
# Redis pub/sub only delivers messages to clients already subscribed at
# publish time - it never buffers for a not-yet-subscribed client and there
# is no durable history to replay. So the snapshot (a DB read) must happen
# AFTER the live subscription is established, not before: subscribing
# first means the worst case is a harmless duplicate (a buffered live
# message whose value the snapshot, read moments later, already reflects),
# never a silently dropped update.


class _FakePubSub:
    def __init__(self, calls: list[str]):
        self._calls = calls

    def subscribe(self, channel):
        self._calls.append("subscribe")

    def get_message(self, **kwargs):
        return None

    def unsubscribe(self, channel):
        self._calls.append("unsubscribe")

    def close(self):
        pass


@pytest.mark.asyncio
async def test_subscribes_to_redis_before_computing_the_snapshot(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(analysis_stream.redis_client, "pubsub", lambda: _FakePubSub(calls))

    def fake_compute(db, analysis):
        calls.append("compute_state")
        return {"analysis_id": 7, "status": "processing", "progress": 42}

    monkeypatch.setattr(analysis_stream, "compute_current_analysis_state", fake_compute)

    generator = analysis_stream._analysis_event_stream(
        7, db=Mock(), analysis=SimpleNamespace(id=7)
    )
    first_chunk = await generator.__anext__()

    assert calls == ["subscribe", "compute_state"]
    assert first_chunk.startswith("event: state\n")
    assert '"progress":42' in first_chunk
    assert '"status":"processing"' in first_chunk
    await generator.aclose()


@pytest.mark.asyncio
async def test_state_event_never_reports_progress_100(monkeypatch):
    monkeypatch.setattr(analysis_stream.redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "completed", "progress": 99},
    )

    generator = analysis_stream._analysis_event_stream(
        7, db=Mock(), analysis=SimpleNamespace(id=7)
    )
    first_chunk = await generator.__anext__()

    assert '"progress":99' in first_chunk
    assert "100" not in first_chunk
    await generator.aclose()


@pytest.mark.asyncio
async def test_message_published_between_subscribe_and_snapshot_is_not_lost(monkeypatch):
    """The exact race Additional Requirement E fixes: a Celery worker
    publishes (e.g. the final investigation_result) in the window between
    this client's pubsub.subscribe() and the snapshot DB read completing.
    Because subscribe() already ran by then, Redis has buffered it for
    this client - it must still be delivered, just after the state event,
    never silently dropped."""

    class _PubSubWithBufferedMessage(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._delivered = False

        def get_message(self, **kwargs):
            if not self._delivered:
                self._delivered = True
                return {
                    "data": json.dumps(
                        {
                            "event": "investigation_result",
                            "data": {"investigation_path": "simple"},
                        }
                    )
                }
            return None

    calls: list[str] = []
    monkeypatch.setattr(
        analysis_stream.redis_client, "pubsub", lambda: _PubSubWithBufferedMessage(calls)
    )
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 53},
    )

    generator = analysis_stream._analysis_event_stream(
        7, db=Mock(), analysis=SimpleNamespace(id=7)
    )

    state_chunk = await generator.__anext__()
    live_chunk = await generator.__anext__()

    assert state_chunk.startswith("event: state\n")
    assert live_chunk.startswith("event: investigation_result\n")
    assert '"investigation_path":"simple"' in live_chunk
    await generator.aclose()


@pytest.mark.asyncio
async def test_stale_queued_progress_tick_does_not_regress_the_client(monkeypatch):
    """A progress=51 tick published (and buffered by Redis) in the window
    between subscribe() and the snapshot read - which already reports 53 -
    must not be relayed: it would visually move the client backward from
    53% to 51%. A genuinely later 60% tick after it must still pass
    through."""

    class _PubSubWithQueuedTicks(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._queue = [
                {"data": json.dumps({"event": "progress", "data": {"stage": "ingestion", "message": "m", "progress": 51}})},
                {"data": json.dumps({"event": "progress", "data": {"stage": "ingestion", "message": "m", "progress": 60}})},
            ]

        def get_message(self, **kwargs):
            if self._queue:
                return self._queue.pop(0)
            return None

    monkeypatch.setattr(
        analysis_stream.redis_client, "pubsub", lambda: _PubSubWithQueuedTicks([])
    )
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 53},
    )

    generator = analysis_stream._analysis_event_stream(
        7, db=Mock(), analysis=SimpleNamespace(id=7)
    )

    state_chunk = await generator.__anext__()
    next_chunk = await generator.__anext__()

    assert '"progress":53' in state_chunk
    # The stale 51% tick was dropped - the next thing the client sees is
    # the genuinely later 60% tick, never a regression to 51%.
    assert next_chunk.startswith("event: progress\n")
    assert '"progress":60' in next_chunk
    await generator.aclose()


@pytest.mark.asyncio
async def test_completed_snapshot_does_not_relay_a_redundant_live_investigation_result(monkeypatch):
    """If the snapshot itself already reports status="completed" (and
    therefore already carries the authoritative investigation_result), a
    stray buffered live investigation_result event for the same analysis
    must not be relayed a second time - DB state is the source of truth."""

    class _PubSubWithBufferedResult(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._delivered = False

        def get_message(self, **kwargs):
            if not self._delivered:
                self._delivered = True
                return {
                    "data": json.dumps(
                        {
                            "event": "investigation_result",
                            "data": {"investigation_path": "simple"},
                        }
                    )
                }
            return None

    monkeypatch.setattr(
        analysis_stream.redis_client, "pubsub", lambda: _PubSubWithBufferedResult([])
    )
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {
            "analysis_id": 7,
            "status": "completed",
            "progress": 99,
            "investigation_result": {"investigation_path": "simple"},
        },
    )

    generator = analysis_stream._analysis_event_stream(
        7, db=Mock(), analysis=SimpleNamespace(id=7)
    )

    state_chunk = await generator.__anext__()
    assert state_chunk.startswith("event: state\n")

    # The redundant investigation_result was silently skipped (not
    # forwarded), so the generator just keeps polling/sleeping rather than
    # yielding a second chunk - a bounded wait proves nothing arrives
    # without hanging the test on an infinite loop.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(generator.__anext__(), timeout=0.3)

    await generator.aclose()
