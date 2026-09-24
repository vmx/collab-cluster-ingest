"""Worker bookkeeping: only events that were actually handled release the cursor."""

import asyncio

import pytest

from collab_cluster_torrentizer import cli
from collab_cluster_torrentizer.config import Config
from collab_cluster_torrentizer.firehose import CursorTracker, MatadiscoEvent
from collab_cluster_torrentizer.status import StatusReporter


async def test_worker_marks_done_on_failure_but_not_on_cancellation(monkeypatch: pytest.MonkeyPatch):
    started = asyncio.Event()

    async def fake_process_event(event, **_kwargs):
        if event.time_us == 100:
            raise RuntimeError("boom")
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(cli, "process_event", fake_process_event)

    tracker = CursorTracker()
    queue: asyncio.Queue[MatadiscoEvent] = asyncio.Queue()
    for time_us in (100, 200):
        tracker.seen(time_us)
        tracker.start(time_us)
        await queue.put(MatadiscoEvent(f"at://x/{time_us}", "did:plc:x", time_us, {}))
    tracker.seen(300)  # a later, non-queued event

    reporter = StatusReporter(Config(), tracker, queue, now=0.0)
    worker = asyncio.create_task(
        cli._worker(queue, config=Config(), state=None, client=None, tracker=tracker, reporter=reporter)
    )
    await started.wait()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    # The failed event is done and counted; the interrupted one still holds the cursor back.
    assert reporter.counts == {"failed": 1}
    assert tracker.safe_cursor() == 200
    tracker.done(200)
    assert tracker.safe_cursor() == 300
