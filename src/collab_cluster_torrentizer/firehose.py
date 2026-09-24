"""Tail Jetstream's legacy v1-compatible `/subscribe` endpoint for matadisco commits.

Jetstream (https://github.com/bluesky-social/jetstream) sits in front of the
real AT Proto relay and supports server-side collection filtering
(`wantedCollections`), so this only ever sees `cx.vmx.matadisco` events --
no CBOR/CAR decoding of the rest of the network needed.

Resuming uses `time_us` (a unix-microsecond timestamp), passed back as
`?cursor=<time_us>` on reconnect -- confirmed against the live endpoint:
despite docs describing a monotonic per-event `cursor` field, no event kind
(commit/identity/account) actually carries one; `time_us` is what's there.

Every (re)connect resumes from the latest persisted cursor, rewound a few
seconds so nothing at the boundary is missed (the processed-record ledger
absorbs the duplicates). Reconnects back off exponentially with jitter, and the
backoff only resets once a connection has stayed up for a while -- otherwise a
server that accepts and then immediately drops us would be hammered in a tight
loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import websockets

from .config import MATADISCO_COLLECTION, Config
from .filters import is_target_publisher
from .state import State

logger = logging.getLogger(__name__)

_CURSOR_REWIND_US = 5_000_000
_CURSOR_FLUSH_INTERVAL_S = 5.0
_BACKOFF_INITIAL_S = 1.0
_BACKOFF_MAX_S = 60.0
# A connection that lived at least this long counts as healthy and resets the backoff.
_STABLE_CONNECTION_S = 60.0


@dataclass(frozen=True, slots=True)
class MatadiscoEvent:
    """A candidate matadisco record pulled off Jetstream, not yet STAC-resolved."""

    at_uri: str
    repo_did: str
    time_us: int
    record: dict[str, Any]


def _subscribe_url(config: Config, cursor: int | None) -> str:
    params: dict[str, Any] = {"wantedCollections": MATADISCO_COLLECTION}
    if cursor is not None:
        params["cursor"] = cursor
    return f"{config.jetstream_url}/subscribe?{urlencode(params)}"


async def run_firehose(config: Config, state: State, queue: asyncio.Queue[MatadiscoEvent]) -> None:
    """Connect to Jetstream and feed matching records into `queue` until cancelled."""
    loop = asyncio.get_running_loop()
    backoff = _BACKOFF_INITIAL_S
    while True:
        cursor = state.get_cursor()
        if cursor is not None:
            cursor -= _CURSOR_REWIND_US

        connected_at = None
        try:
            async with websockets.connect(_subscribe_url(config, cursor)) as websocket:
                connected_at = loop.time()
                logger.info("connected to jetstream (cursor=%s)", cursor)
                await _consume(websocket, config, state, queue)
        except websockets.ConnectionClosed as exc:
            # str(exc) carries the close code and reason, e.g. "received 1011 (internal error) ...".
            logger.warning("jetstream connection closed: %s", exc)
        except (OSError, websockets.InvalidHandshake) as exc:
            logger.warning("jetstream connect failed: %s", exc)

        if connected_at is not None and loop.time() - connected_at >= _STABLE_CONNECTION_S:
            backoff = _BACKOFF_INITIAL_S
        delay = backoff + random.uniform(0, backoff)
        logger.warning("reconnecting to jetstream in %.1fs", delay)
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, _BACKOFF_MAX_S)


async def _consume(websocket: Any, config: Config, state: State, queue: asyncio.Queue[MatadiscoEvent]) -> None:
    """Read events off one connection, persisting the cursor at most every few seconds."""
    loop = asyncio.get_running_loop()
    last_flush = loop.time()
    time_us = None
    try:
        async for raw_message in websocket:
            event = json.loads(raw_message)
            time_us = event["time_us"]
            if loop.time() - last_flush >= _CURSOR_FLUSH_INTERVAL_S:
                state.set_cursor(time_us)
                last_flush = loop.time()

            if event.get("kind") != "commit":
                continue

            commit = event["commit"]
            if commit.get("operation") != "create" or commit.get("collection") != MATADISCO_COLLECTION:
                continue

            did = event["did"]
            if not is_target_publisher(did, config.allowed_publisher_dids):
                continue

            record = commit.get("record")
            if not record:
                continue

            at_uri = f"at://{did}/{commit['collection']}/{commit['rkey']}"
            await queue.put(MatadiscoEvent(at_uri, did, time_us, record))
    finally:
        if time_us is not None:
            state.set_cursor(time_us)
