"""CLI entrypoint: wires config, state, the Jetstream consumer, and the worker pool."""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from pathlib import Path

import httpx

from .config import USER_AGENT, Config
from .firehose import CursorTracker, MatadiscoEvent, run_firehose
from .pipeline import Outcome, process_event
from .state import State
from .status import StatusReporter, run_status_reporter

logger = logging.getLogger(__name__)


async def _worker(
    queue: asyncio.Queue[MatadiscoEvent],
    *,
    config: Config,
    state: State,
    client: httpx.AsyncClient,
    tracker: CursorTracker,
    reporter: StatusReporter,
) -> None:
    while True:
        event = await queue.get()
        try:
            outcome = await process_event(event, config=config, state=state, client=client)
        except Exception:
            logger.exception("unhandled error processing %s", event.at_uri)
            outcome = Outcome.FAILED
        finally:
            queue.task_done()
        reporter.counts[outcome.value] += 1
        # Not reached on cancellation, so an interrupted event keeps holding the cursor back.
        tracker.done(event.time_us)


async def _run(config: Config) -> None:
    state = State(Path(config.state_db_path))
    queue: asyncio.Queue[MatadiscoEvent] = asyncio.Queue(maxsize=config.queue_maxsize)
    tracker = CursorTracker()
    reporter = StatusReporter(config, tracker, queue, now=time.time())

    async with httpx.AsyncClient(
        timeout=config.http_timeout_seconds, headers={"User-Agent": USER_AGENT}
    ) as client:
        tasks = [
            asyncio.create_task(
                _worker(queue, config=config, state=state, client=client, tracker=tracker, reporter=reporter)
            )
            for _ in range(config.worker_concurrency)
        ]
        tasks.append(asyncio.create_task(run_status_reporter(reporter)))
        try:
            await run_firehose(config, state, queue, tracker, reporter.counts)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            tracker.persist(state)
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
