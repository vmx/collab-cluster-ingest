"""Cursor tracking and reconnect behaviour of the Jetstream consumer, driven by a fake `websockets.connect`."""

import asyncio
import json
import logging
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import websockets
from websockets.frames import Close

from collab_cluster_torrentizer import firehose
from collab_cluster_torrentizer.config import Config
from collab_cluster_torrentizer.state import State


class _FakeConnection:
    def __init__(self, messages: list[dict]):
        self._messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def __aiter__(self):
        for message in self._messages:
            yield json.dumps(message)
        raise websockets.ConnectionClosed(rcvd=Close(1011, "test close"), sent=None)


class _Stop(Exception):
    pass


def _cursor_of(url: str) -> int | None:
    values = parse_qs(urlparse(url).query).get("cursor")
    return int(values[0]) if values else None


async def test_reconnect_resumes_from_latest_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    state = State(tmp_path / "state.sqlite3")
    urls: list[str] = []

    def fake_connect(url: str, **_kwargs):
        urls.append(url)
        if len(urls) == 1:
            return _FakeConnection(
                [{"kind": "identity", "time_us": 100_000_000}, {"kind": "identity", "time_us": 200_000_000}]
            )
        raise _Stop

    async def fake_sleep(_delay: float):
        pass

    monkeypatch.setattr(firehose.websockets, "connect", fake_connect)
    monkeypatch.setattr(firehose.asyncio, "sleep", fake_sleep)

    with pytest.raises(_Stop):
        await firehose.run_firehose(Config(), state, asyncio.Queue(), firehose.CursorTracker(), Counter())

    assert _cursor_of(urls[0]) is None
    assert _cursor_of(urls[1]) == 200_000_000 - firehose._CURSOR_REWIND_US
    assert state.get_cursor() == 200_000_000


async def test_backoff_grows_when_connections_keep_failing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    state = State(tmp_path / "state.sqlite3")
    delays: list[float] = []

    def fake_connect(_url: str, **_kwargs):
        return _FakeConnection([])

    async def fake_sleep(delay: float):
        delays.append(delay)
        if len(delays) == 8:
            raise _Stop

    monkeypatch.setattr(firehose.websockets, "connect", fake_connect)
    monkeypatch.setattr(firehose.asyncio, "sleep", fake_sleep)

    with pytest.raises(_Stop):
        await firehose.run_firehose(Config(), state, asyncio.Queue(), firehose.CursorTracker(), Counter())

    for delay, base in zip(delays, [1, 2, 4, 8, 16, 32, 60, 60], strict=True):
        assert base <= delay <= 2 * base


def _commit(time_us: int, rkey: str) -> dict:
    return {
        "kind": "commit",
        "did": "did:plc:example",
        "time_us": time_us,
        "commit": {"operation": "create", "collection": "cx.vmx.matadisco", "rkey": rkey, "record": {"resource": "x"}},
    }


async def test_unfinished_events_hold_back_the_persisted_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    state = State(tmp_path / "state.sqlite3")
    tracker = firehose.CursorTracker()
    queue: asyncio.Queue = asyncio.Queue()
    urls: list[str] = []

    def fake_connect(url: str, **_kwargs):
        urls.append(url)
        if len(urls) == 1:
            return _FakeConnection([_commit(100, "a"), _commit(200, "b"), {"kind": "identity", "time_us": 300}])
        raise _Stop

    async def fake_sleep(_delay: float):
        pass

    monkeypatch.setattr(firehose.websockets, "connect", fake_connect)
    monkeypatch.setattr(firehose.asyncio, "sleep", fake_sleep)

    with pytest.raises(_Stop):
        await firehose.run_firehose(Config(), state, queue, tracker, Counter())

    # Both commits are still queued, so the oldest one bounds the persisted cursor...
    assert queue.qsize() == 2
    assert state.get_cursor() == 100
    # ...while an in-process reconnect resumes from the latest event seen, not re-queueing them.
    assert _cursor_of(urls[1]) == 300 - firehose._CURSOR_REWIND_US

    tracker.done(100)
    tracker.persist(state)
    assert state.get_cursor() == 200

    tracker.done(200)
    tracker.persist(state)
    assert state.get_cursor() == 300


def test_tracker_handles_duplicate_time_us():
    tracker = firehose.CursorTracker()
    assert tracker.safe_cursor() is None
    for time_us in (100, 100, 200):
        tracker.seen(time_us)
        tracker.start(time_us)

    tracker.done(100)
    assert tracker.safe_cursor() == 100
    tracker.done(100)
    assert tracker.safe_cursor() == 200
    tracker.done(200)
    assert tracker.safe_cursor() == 200


async def test_resume_gap_and_filtered_publishers_are_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    state = State(tmp_path / "state.sqlite3")
    state.set_cursor(1_000_000_000)
    counts: Counter[str] = Counter()
    urls: list[str] = []

    def fake_connect(url: str, **_kwargs):
        urls.append(url)
        if len(urls) == 1:
            # Jetstream resumes an hour after the requested cursor.
            return _FakeConnection([_commit(1_000_000_000 + 3_600_000_000, "a")])
        raise _Stop

    async def fake_sleep(_delay: float):
        pass

    monkeypatch.setattr(firehose.websockets, "connect", fake_connect)
    monkeypatch.setattr(firehose.asyncio, "sleep", fake_sleep)
    config = Config(allowed_publisher_dids=("did:plc:someone-else",))

    with caplog.at_level(logging.WARNING), pytest.raises(_Stop):
        await firehose.run_firehose(config, state, asyncio.Queue(), firehose.CursorTracker(), counts)

    assert counts["filtered"] == 1
    assert any("retention window" in r.getMessage() and "1h00m later" in r.getMessage() for r in caplog.records)
