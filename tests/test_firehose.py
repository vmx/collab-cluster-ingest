"""Reconnect behaviour of the Jetstream consumer, driven by a fake `websockets.connect`."""

import asyncio
import json
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

    def fake_connect(url: str):
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
        await firehose.run_firehose(Config(), state, asyncio.Queue())

    assert _cursor_of(urls[0]) is None
    assert _cursor_of(urls[1]) == 200_000_000 - firehose._CURSOR_REWIND_US
    assert state.get_cursor() == 200_000_000


async def test_backoff_grows_when_connections_keep_failing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    state = State(tmp_path / "state.sqlite3")
    delays: list[float] = []

    def fake_connect(_url: str):
        return _FakeConnection([])

    async def fake_sleep(delay: float):
        delays.append(delay)
        if len(delays) == 8:
            raise _Stop

    monkeypatch.setattr(firehose.websockets, "connect", fake_connect)
    monkeypatch.setattr(firehose.asyncio, "sleep", fake_sleep)

    with pytest.raises(_Stop):
        await firehose.run_firehose(Config(), state, asyncio.Queue())

    for delay, base in zip(delays, [1, 2, 4, 8, 16, 32, 60, 60], strict=True):
        assert base <= delay <= 2 * base

