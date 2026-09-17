"""CLI entrypoint: wires config, state, the Jetstream consumer, and the worker pool."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

import httpx

from .config import Config
from .firehose import MatadiscoEvent, run_firehose
from .pipeline import process_event
from .state import State

logger = logging.getLogger(__name__)


async def _worker(
    queue: asyncio.Queue[MatadiscoEvent], *, config: Config, state: State, client: httpx.AsyncClient
) -> None:
    while True:
        event = await queue.get()
        try:
            await process_event(event, config=config, state=state, client=client)
        except Exception:
            logger.exception("unhandled error processing %s", event.at_uri)
        finally:
            queue.task_done()


async def _run(config: Config) -> None:
    state = State(Path(config.state_db_path))
    queue: asyncio.Queue[MatadiscoEvent] = asyncio.Queue(maxsize=config.queue_maxsize)

    async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
        workers = [
            asyncio.create_task(_worker(queue, config=config, state=state, client=client))
            for _ in range(config.worker_concurrency)
        ]
        try:
            await run_firehose(config, state, queue)
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            state.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    config = Config.from_env()
    asyncio.run(_run(config))


if __name__ == "__main__":
    main()
