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
absorbs the duplicates). The persisted cursor never moves past an event that
is still queued or being processed (see `CursorTracker`), so a restart redoes
unfinished work instead of skipping it. Reconnects back off exponentially with jitter, and the
backoff only resets once a connection has stayed up for a while -- otherwise a
server that accepts and then immediately drops us would be hammered in a tight
loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import websockets

from .config import MATADISCO_COLLECTION, Config
from .filters import is_target_publisher
from .state import State
from .status import format_duration, lag_seconds

logger = logging.getLogger(__name__)

_CURSOR_REWIND_US = 5_000_000
_CURSOR_FLUSH_INTERVAL_S = 5.0
_BACKOFF_INITIAL_S = 1.0
_BACKOFF_MAX_S = 60.0
# A connection that lived at least this long counts as healthy and resets the backoff.
_STABLE_CONNECTION_S = 60.0
# Identity/account events arrive every couple of seconds, so a first event this much
# later than the requested cursor means Jetstream no longer had the events in between.
_RESUME_GAP_WARNING_US = 60_000_000


@dataclass(frozen=True, slots=True)
class MatadiscoEvent:
    """A candidate matadisco record pulled off Jetstream, not yet STAC-resolved."""

    at_uri: str
    repo_did: str
    time_us: int
    record: dict[str, Any]


class CursorTracker:
    """Tracks which queued events are unfinished, to derive a cursor that is safe to persist.

    The firehose calls `seen` for every event and `start` for every event it
    queues; workers call `done` once an event has been handled (successfully or
    not). An event interrupted by shutdown is never marked done, so it keeps
    holding the cursor back and gets redelivered after a restart.
    """

    def __init__(self) -> None:
        self.latest: int | None = None
        # time_us -> number of unfinished events with it (time_us isn't guaranteed unique).
        self._pending: Counter[int] = Counter()

    def seen(self, time_us: int) -> None:
        self.latest = time_us

    def start(self, time_us: int) -> None:
        self._pending[time_us] += 1

    def done(self, time_us: int) -> None:
        self._pending[time_us] -= 1
        if self._pending[time_us] <= 0:
            del self._pending[time_us]

    @property
    def pending_count(self) -> int:
        return self._pending.total()

    def safe_cursor(self) -> int | None:
        """The oldest unfinished event's time_us, or the latest seen one if nothing is pending."""
        return min(self._pending) if self._pending else self.latest

    def persist(self, state: State) -> None:
        cursor = self.safe_cursor()
        if cursor is not None:
            state.set_cursor(cursor)


def _subscribe_url(config: Config, cursor: int | None) -> str:
    params: dict[str, Any] = {"wantedCollections": MATADISCO_COLLECTION}
    if cursor is not None:
        params["cursor"] = cursor
    return f"{config.jetstream_url}/subscribe?{urlencode(params)}"


async def run_firehose(
    config: Config,
    state: State,
    queue: asyncio.Queue[MatadiscoEvent],
    tracker: CursorTracker,
    counts: Counter[str],
) -> None:
    """Connect to Jetstream and feed matching records into `queue` until cancelled.

    Records dropped by the publisher filter are counted under "filtered" in `counts`.
    """
    loop = asyncio.get_running_loop()
    backoff = _BACKOFF_INITIAL_S
    while True:
        # Within one process, events still pending are in memory already, so resume
        # from the latest one seen; only a fresh start falls back to the persisted cursor.
        cursor = tracker.latest if tracker.latest is not None else state.get_cursor()
        if cursor is not None:
            cursor -= _CURSOR_REWIND_US

        connected_at = None
        try:
            async with websockets.connect(_subscribe_url(config, cursor)) as websocket:
                connected_at = loop.time()
                if cursor is None:
                    logger.info("connected to jetstream, starting live (no cursor)")
                else:
                    logger.info(
                        "connected to jetstream, resuming %s behind live (cursor=%d)",
                        format_duration(lag_seconds(cursor, time.time())),
                        cursor,
                    )
                await _consume(websocket, config, state, queue, tracker, counts, cursor)
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


async def _consume(
    websocket: Any,
    config: Config,
    state: State,
    queue: asyncio.Queue[MatadiscoEvent],
    tracker: CursorTracker,
    counts: Counter[str],
    cursor: int | None,
) -> None:
    """Read events off one connection, persisting the safe cursor at most every few seconds."""
    loop = asyncio.get_running_loop()
    last_flush = loop.time()
    first = True
    try:
        async for raw_message in websocket:
            event = json.loads(raw_message)
            time_us = event["time_us"]
            if first and cursor is not None and time_us - cursor > _RESUME_GAP_WARNING_US:
                logger.warning(
                    "requested cursor %d but jetstream resumed %s later; "
                    "events in between were likely past its retention window and are lost",
                    cursor,
                    format_duration((time_us - cursor) / 1_000_000),
                )
            first = False
            tracker.seen(time_us)
            if loop.time() - last_flush >= _CURSOR_FLUSH_INTERVAL_S:
                tracker.persist(state)
                last_flush = loop.time()

            if event.get("kind") != "commit":
                continue

            commit = event["commit"]
            if commit.get("operation") != "create" or commit.get("collection") != MATADISCO_COLLECTION:
                continue

            did = event["did"]
            if not is_target_publisher(did, config.allowed_publisher_dids):
                counts["filtered"] += 1
                continue

            record = commit.get("record")
            if not record:
                continue

            at_uri = f"at://{did}/{commit['collection']}/{commit['rkey']}"
            tracker.start(time_us)
            await queue.put(MatadiscoEvent(at_uri, did, time_us, record))
    finally:
        tracker.persist(state)
