"""Tail Jetstream's legacy v1-compatible `/subscribe` endpoint for matadisco commits.

Jetstream (https://github.com/bluesky-social/jetstream) sits in front of the
real AT Proto relay and supports server-side collection filtering
(`wantedCollections`), so this only ever sees `cx.vmx.matadisco` events --
no CBOR/CAR decoding of the rest of the network needed.

Resuming uses `time_us` (a unix-microsecond timestamp), passed back as
`?cursor=<time_us>` on reconnect -- confirmed against the live endpoint:
despite docs describing a monotonic per-event `cursor` field, no event kind
(commit/identity/account) actually carries one; `time_us` is what's there.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import websockets

from .config import MATADISCO_COLLECTION, Config
from .filters import is_target_publisher
from .state import State

logger = logging.getLogger(__name__)


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
    async for websocket in websockets.connect(_subscribe_url(config, state.get_cursor())):
        try:
            async for raw_message in websocket:
                event = json.loads(raw_message)
                time_us = event["time_us"]
                state.set_cursor(time_us)

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
        except websockets.ConnectionClosed:
            logger.warning("jetstream connection closed, reconnecting")
            continue
