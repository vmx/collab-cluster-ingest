"""Status line and can't-keep-up warnings."""

import asyncio
import logging

import pytest

from collab_cluster_torrentizer.config import Config
from collab_cluster_torrentizer.firehose import CursorTracker
from collab_cluster_torrentizer.status import StatusReporter, format_duration

_CONFIG = Config(queue_maxsize=2, status_interval_seconds=300, lag_warning_seconds=600, queue_full_warning_seconds=180)


def _messages(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    messages = [(r.levelno, r.getMessage()) for r in caplog.records]
    caplog.clear()
    return messages


def test_format_duration():
    assert format_duration(42.9) == "42s"
    assert format_duration(185) == "3m05s"
    assert format_duration(3 * 3600 + 12 * 60 + 5) == "3h12m"


def test_lag_warning_fires_once_and_recovers(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO)
    tracker = CursorTracker()
    reporter = StatusReporter(_CONFIG, tracker, asyncio.Queue(maxsize=2), now=1_700.0)

    tracker.seen(1_000 * 1_000_000)
    reporter.check(1_000 + 700)
    reporter.check(1_000 + 800)
    assert _messages(caplog) == [(logging.WARNING, "falling behind: stream position is 11m40s behind live")]

    tracker.seen(1_790 * 1_000_000)
    reporter.check(1_800)
    assert _messages(caplog) == [(logging.INFO, "caught up: stream position is 10s behind live")]


def test_queue_full_warning_fires_after_threshold_and_recovers(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO)
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    reporter = StatusReporter(_CONFIG, CursorTracker(), queue, now=0.0)
    queue.put_nowait(1)
    queue.put_nowait(2)

    reporter.check(10)
    reporter.check(150)
    assert _messages(caplog) == []
    reporter.check(200)
    reporter.check(250)
    assert _messages(caplog) == [
        (logging.WARNING, "workers can't keep up: queue has been full for 3m10s, firehose is blocked")
    ]

    queue.get_nowait()
    reporter.check(260)
    assert _messages(caplog) == [(logging.INFO, "queue drained after being full for 4m10s")]


def test_status_line_reports_and_resets_counts(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO)
    tracker = CursorTracker()
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    reporter = StatusReporter(_CONFIG, tracker, queue, now=0.0)
    for time_us in (100_000_000, 200_000_000, 300_000_000):
        tracker.seen(time_us)
        tracker.start(time_us)
    queue.put_nowait(object())
    reporter.counts.update({"packaged": 2, "duplicate": 5, "filtered": 1})

    reporter.check(299)
    assert _messages(caplog) == []
    reporter.check(300)
    assert _messages(caplog) == [
        (
            logging.INFO,
            "status: 0s behind live, queue 1/2, 2 processing; last 5m00s: "
            "2 packaged, 5 already processed, 0 skipped, 1 filtered, 0 failed",
        )
    ]
    assert not reporter.counts
