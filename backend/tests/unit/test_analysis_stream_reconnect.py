"""SSE reconnect correctness: on (re)connect the client must get current
persisted state immediately, then only future live events - no durable
replay of historical progress ticks, ownership/auth preserved, no new
endpoint (the existing GET /analyses/{id}/events is extended in place).

SSE transport hardening: the FastAPI SSE endpoint uses an async Redis
client/pubsub (never a synchronous, event-loop-blocking one) and holds no
long-lived DB Session/ORM object for the stream's lifetime - only short,
scoped sessionLocal() uses. Fake pubsub objects below are therefore async
(subscribe/get_message/unsubscribe/aclose), matching the real
redis.asyncio.client.PubSub API.
"""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from redis.exceptions import ConnectionError as RedisConnectionError

from app.api import analysis_stream


# --- shared fakes ---------------------------------------------------------


class _FakeSession:
    """Minimal sessionLocal()-shaped context manager: `db.query(Analysis)
    .filter(...).first()` returns whatever row this was constructed with,
    regardless of the actual filter - good enough since every test that
    cares about the ownership/lookup predicate monkeypatches
    compute_current_analysis_state (or the endpoint's own query result)
    directly rather than relying on real SQL filtering."""

    def __init__(self, row=None):
        self._row = row

    def query(self, *args, **kwargs):
        mock = Mock()
        mock.filter.return_value.first.return_value = self._row
        return mock

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Real SQLAlchemy Session.__exit__ closes the session - mirrored
        # here so tests can assert on close() actually running.
        self.close()
        return False

    def close(self):
        pass


def _session_local(row=None):
    return lambda: _FakeSession(row)


class _FakePubSub:
    def __init__(self, calls: list[str]):
        self._calls = calls

    async def subscribe(self, channel):
        self._calls.append("subscribe")

    async def get_message(self, **kwargs):
        return None

    async def unsubscribe(self, channel):
        self._calls.append("unsubscribe")

    async def aclose(self):
        self._calls.append("aclose")


# --- ownership/auth is preserved --------------------------------------


def test_stream_endpoint_404s_for_an_analysis_the_user_does_not_own(monkeypatch):
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=None))

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            analysis_stream.stream_analysis_events(analysis_id=999, current_user_id=4)
        )

    assert error.value.status_code == 404


def test_stream_endpoint_starts_the_stream_only_for_the_owning_user(monkeypatch):
    """stream_analysis_events is now just a short-lived ownership check +
    glue: it passes only the bare analysis_id into the generator - no DB
    Session or Analysis ORM object crosses into the (potentially
    minutes-long) stream."""
    monkeypatch.setattr(
        analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=42))
    )

    async def _empty_gen():
        return
        yield  # pragma: no cover - never reached; unstarted generator only

    stream_mock = Mock(return_value=_empty_gen())
    monkeypatch.setattr(analysis_stream, "_analysis_event_stream", stream_mock)

    response = asyncio.run(
        analysis_stream.stream_analysis_events(analysis_id=42, current_user_id=4)
    )

    stream_mock.assert_called_once_with(42)
    assert response.status_code == 200


# --- subscribe before snapshotting; DB Session scoped to the snapshot only -


@pytest.mark.asyncio
async def test_subscribes_to_redis_before_computing_the_snapshot(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub(calls))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))

    def fake_compute(db, analysis):
        calls.append("compute_state")
        return {"analysis_id": 7, "status": "processing", "progress": 42}

    monkeypatch.setattr(analysis_stream, "compute_current_analysis_state", fake_compute)

    generator = analysis_stream._analysis_event_stream(7)
    first_chunk = await generator.__anext__()

    assert calls == ["subscribe", "compute_state"]
    assert first_chunk.startswith("event: state\n")
    assert '"progress":42' in first_chunk
    assert '"status":"processing"' in first_chunk
    await generator.aclose()


@pytest.mark.asyncio
async def test_state_event_never_reports_progress_100(monkeypatch):
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "completed", "progress": 99},
    )

    generator = analysis_stream._analysis_event_stream(7)
    first_chunk = await generator.__anext__()

    assert '"progress":99' in first_chunk
    assert "100" not in first_chunk
    await generator.aclose()


@pytest.mark.asyncio
async def test_message_published_between_subscribe_and_snapshot_is_not_lost(monkeypatch):
    """The exact race SSE hardening must keep closed: a Celery worker
    publishes (e.g. the final investigation_result) in the window between
    this client's pubsub.subscribe() and the snapshot DB read completing.
    Because subscribe() already ran by then, Redis has buffered it for
    this client - it must still be delivered, just after the state event,
    never silently dropped."""

    class _PubSubWithBufferedMessage(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._delivered = False

        async def get_message(self, **kwargs):
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
        analysis_stream.async_redis_client, "pubsub", lambda: _PubSubWithBufferedMessage(calls)
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 53},
    )

    generator = analysis_stream._analysis_event_stream(7)

    state_chunk = await generator.__anext__()
    live_chunk = await generator.__anext__()

    assert state_chunk.startswith("event: state\n")
    assert live_chunk.startswith("event: investigation_result\n")
    assert '"investigation_path":"simple"' in live_chunk

    # investigation_result is delivered exactly once, then the generator
    # terminates - no more chunks follow it.
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


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

        async def get_message(self, **kwargs):
            if self._queue:
                return self._queue.pop(0)
            return None

    monkeypatch.setattr(
        analysis_stream.async_redis_client, "pubsub", lambda: _PubSubWithQueuedTicks([])
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 53},
    )

    generator = analysis_stream._analysis_event_stream(7)

    state_chunk = await generator.__anext__()
    next_chunk = await generator.__anext__()

    assert '"progress":53' in state_chunk
    # The stale 51% tick was dropped - the next thing the client sees is
    # the genuinely later 60% tick, never a regression to 51%.
    assert next_chunk.startswith("event: progress\n")
    assert '"progress":60' in next_chunk
    await generator.aclose()


# --- Terminal-state behavior ----------------------------------------------


@pytest.mark.asyncio
async def test_completed_initial_snapshot_yields_state_then_terminates(monkeypatch):
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
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

    generator = analysis_stream._analysis_event_stream(7)
    state_chunk = await generator.__anext__()

    assert state_chunk.startswith("event: state\n")
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_failed_initial_snapshot_yields_state_then_terminates(monkeypatch):
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "failed"},
    )

    generator = analysis_stream._analysis_event_stream(7)
    state_chunk = await generator.__anext__()

    assert state_chunk.startswith("event: state\n")
    assert '"status":"failed"' in state_chunk
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_cancelled_initial_snapshot_yields_state_then_terminates(monkeypatch):
    """A snapshot that is already "cancelled" (the analysis was
    cancelled before this client ever connected/reconnected) must behave
    exactly like completed/failed above - one state event, then done. No
    fake progress=100, no investigation_result."""
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "cancelled"},
    )

    generator = analysis_stream._analysis_event_stream(7)
    state_chunk = await generator.__anext__()

    assert state_chunk.startswith("event: state\n")
    assert '"status":"cancelled"' in state_chunk
    assert "progress" not in state_chunk
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_live_cancelled_event_is_yielded_once_then_terminates(monkeypatch):
    """A client already watching a still-processing analysis that
    gets cancelled mid-stream must receive the live "cancelled" event
    (published by POST /analysis/{id}/cancel) exactly once, then the
    stream ends - the same terminal treatment as a live investigation_result."""
    class _PubSubWithLiveCancellation(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._delivered = False

        async def get_message(self, **kwargs):
            if not self._delivered:
                self._delivered = True
                return {
                    "data": json.dumps(
                        {"event": "cancelled", "data": {"analysis_id": 7}}
                    )
                }
            return None

    monkeypatch.setattr(
        analysis_stream.async_redis_client, "pubsub", lambda: _PubSubWithLiveCancellation([])
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 40},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state
    cancelled_chunk = await generator.__anext__()

    assert cancelled_chunk.startswith("event: cancelled\n")
    assert '"analysis_id":7' in cancelled_chunk
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_live_investigation_result_is_yielded_once_then_terminates(monkeypatch):
    class _PubSubWithLiveResult(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._delivered = False

        async def get_message(self, **kwargs):
            if not self._delivered:
                self._delivered = True
                return {
                    "data": json.dumps(
                        {
                            "event": "investigation_result",
                            "data": {"investigation_path": "correlated"},
                        }
                    )
                }
            return None

    monkeypatch.setattr(
        analysis_stream.async_redis_client, "pubsub", lambda: _PubSubWithLiveResult([])
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 80},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state
    result_chunk = await generator.__anext__()

    assert result_chunk.startswith("event: investigation_result\n")
    assert '"investigation_path":"correlated"' in result_chunk
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


# --- Async Redis polling is actually used ---------------------------------


@pytest.mark.asyncio
async def test_async_redis_polling_is_used(monkeypatch):
    """get_message is awaited (a coroutine call), not the synchronous
    redis.Redis pubsub API - proven by driving the generator through a
    real async-def fake: an ordinary (sync-def) fake would raise a
    TypeError the moment the generator tried to `await` its return value."""
    monkeypatch.setattr(analysis_stream, "_SSE_HEARTBEAT_SECONDS", 0.0)
    calls: list[str] = []

    class _TrackingPubSub(_FakePubSub):
        async def get_message(self, **kwargs):
            calls.append("get_message")
            return None

    monkeypatch.setattr(
        analysis_stream.async_redis_client, "pubsub", lambda: _TrackingPubSub(calls)
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 10},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state
    heartbeat_chunk = await asyncio.wait_for(generator.__anext__(), timeout=1.0)

    assert heartbeat_chunk == ": keep-alive\n\n"
    assert "get_message" in calls
    await generator.aclose()


# --- Heartbeat --------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_emits_keep_alive_comment_without_a_progress_or_result_event(monkeypatch):
    monkeypatch.setattr(analysis_stream, "_SSE_HEARTBEAT_SECONDS", 0.0)
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 10},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state
    heartbeat_chunk = await generator.__anext__()

    assert heartbeat_chunk == ": keep-alive\n\n"
    assert "event:" not in heartbeat_chunk
    assert "progress" not in heartbeat_chunk
    await generator.aclose()


# --- DB Session closed before the wait loop --------------------------------


@pytest.mark.asyncio
async def test_db_session_is_closed_before_waiting_for_later_redis_messages(monkeypatch):
    closed = []

    class _TrackedSession(_FakeSession):
        def close(self):
            closed.append(True)

    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub([]))
    monkeypatch.setattr(
        analysis_stream, "sessionLocal", lambda: _TrackedSession(SimpleNamespace(id=7))
    )
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 10},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state: the DB session context manager has
    # already exited by the time this first chunk is produced.

    assert closed == [True]
    await generator.aclose()


# --- Redis failure terminates cleanly ---------------------------------------


@pytest.mark.asyncio
async def test_redis_subscribe_failure_falls_back_to_a_db_only_snapshot(monkeypatch):
    class _FailingPubSub:
        async def subscribe(self, channel):
            raise RedisConnectionError("connection refused")

        async def get_message(self, **kwargs):
            raise AssertionError("must not be called after a failed subscribe")

        async def unsubscribe(self, channel):
            pass

        async def aclose(self):
            pass

    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FailingPubSub())
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 33},
    )

    generator = analysis_stream._analysis_event_stream(7)
    state_chunk = await generator.__anext__()

    assert state_chunk.startswith("event: state\n")
    assert '"progress":33' in state_chunk
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_redis_disconnect_during_polling_terminates_stream_without_mutating_state(monkeypatch):
    analysis = SimpleNamespace(id=7, status="processing")

    class _PubSubThatDisconnects(_FakePubSub):
        async def get_message(self, **kwargs):
            raise RedisConnectionError("connection lost")

    monkeypatch.setattr(
        analysis_stream.async_redis_client, "pubsub", lambda: _PubSubThatDisconnects([])
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=analysis))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, a: {"analysis_id": 7, "status": "processing", "progress": 20},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state

    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()

    # The stream ending on a Redis error must never itself mutate the
    # Analysis row - nothing in this generator ever touches .status.
    assert analysis.status == "processing"


# --- Malformed Redis messages are skipped -----------------------------------


@pytest.mark.asyncio
async def test_malformed_redis_message_is_skipped_not_crashed_on(monkeypatch):
    class _PubSubWithMalformedThenValidMessage(_FakePubSub):
        def __init__(self, calls):
            super().__init__(calls)
            self._queue = [
                {"data": "not even json"},
                {"data": json.dumps({"event": "progress"})},  # missing "data" key
                {"data": json.dumps({"data": {"progress": 77}})},  # missing "event" key
                {
                    "data": json.dumps(
                        {"event": "progress", "data": {"stage": "ingestion", "message": "m", "progress": 77}}
                    )
                },
            ]

        async def get_message(self, **kwargs):
            if self._queue:
                return self._queue.pop(0)
            return None

    monkeypatch.setattr(
        analysis_stream.async_redis_client,
        "pubsub",
        lambda: _PubSubWithMalformedThenValidMessage([]),
    )
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 10},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()  # state
    next_chunk = await generator.__anext__()

    # All three malformed messages were silently skipped - the first thing
    # actually delivered is the real, well-formed progress event.
    assert next_chunk.startswith("event: progress\n")
    assert '"progress":77' in next_chunk
    await generator.aclose()


# --- cleanup: unsubscribe/aclose run even when the generator errors out --


@pytest.mark.asyncio
async def test_pubsub_is_unsubscribed_and_closed_on_generator_teardown(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(analysis_stream.async_redis_client, "pubsub", lambda: _FakePubSub(calls))
    monkeypatch.setattr(analysis_stream, "sessionLocal", _session_local(row=SimpleNamespace(id=7)))
    monkeypatch.setattr(
        analysis_stream,
        "compute_current_analysis_state",
        lambda db, analysis: {"analysis_id": 7, "status": "processing", "progress": 10},
    )

    generator = analysis_stream._analysis_event_stream(7)
    await generator.__anext__()
    await generator.aclose()

    assert "unsubscribe" in calls
    assert "aclose" in calls
