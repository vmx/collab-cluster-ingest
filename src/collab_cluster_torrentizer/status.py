"""Periodic status line plus warnings when the service can't keep up.

Lag is measured as wall-clock time minus the `time_us` of the latest event
seen. That works even with collection filtering: Jetstream delivers identity
and account events regardless of `wantedCollections`, network-wide, every
couple of seconds, so a quiet matadisco collection doesn't read as lag.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from typing import TYPE_CHECKING

from .config import Config

if TYPE_CHECKING:
    from .firehose import CursorTracker, MatadiscoEvent

logger = logging.getLogger(__name__)

_CHECK_INTERVAL_S = 10.0


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def lag_seconds(time_us: int, now: float) -> float:
    # Clamped: Jetstream's clock can be slightly ahead of ours.
    return max(0.0, now - time_us / 1_000_000)


class StatusReporter:
    """Counts per-record outcomes and turns them plus lag/queue state into log lines.

    `check` is called periodically; the status line is logged every
    `status_interval_seconds`, the lag and queue-full warnings once each time
    their threshold is crossed (with a matching recovery message), so a long
    stall doesn't flood the log.
    """

    def __init__(
        self, config: Config, tracker: CursorTracker, queue: asyncio.Queue[MatadiscoEvent], now: float
    ) -> None:
        self._config = config
        self._tracker = tracker
        self._queue = queue
        # Outcome name ("packaged", "duplicate", ...) or "filtered" -> count since the last status line.
        self.counts: Counter[str] = Counter()
        self._last_report = now
        self._lagging = False
        self._queue_full_since: float | None = None
        self._queue_full_warned = False

    def check(self, now: float) -> None:
        latest = self._tracker.latest
        lag = lag_seconds(latest, now) if latest is not None else None

        if lag is not None and lag > self._config.lag_warning_seconds and not self._lagging:
            self._lagging = True
            logger.warning("falling behind: stream position is %s behind live", format_duration(lag))
        elif lag is not None and lag <= self._config.lag_warning_seconds and self._lagging:
            self._lagging = False
            logger.info("caught up: stream position is %s behind live", format_duration(lag))

        if self._queue.full():
            if self._queue_full_since is None:
                self._queue_full_since = now
            full_for = now - self._queue_full_since
            if full_for > self._config.queue_full_warning_seconds and not self._queue_full_warned:
                self._queue_full_warned = True
                logger.warning(
                    "workers can't keep up: queue has been full for %s, firehose is blocked", format_duration(full_for)
                )
        else:
            if self._queue_full_warned:
                logger.info("queue drained after being full for %s", format_duration(now - self._queue_full_since))
            self._queue_full_since = None
            self._queue_full_warned = False

        if now - self._last_report >= self._config.status_interval_seconds:
            self._log_status(lag, now - self._last_report)
            self._last_report = now

    def _log_status(self, lag: float | None, period: float) -> None:
        queued = self._queue.qsize()
        processing = max(0, self._tracker.pending_count - queued)
        counts = self.counts
        logger.info(
            "status: %s behind live, queue %d/%d, %d processing; last %s: "
            "%d packaged, %d already processed, %d skipped, %d filtered, %d failed",
            format_duration(lag) if lag is not None else "unknown",
            queued,
            self._queue.maxsize,
            processing,
            format_duration(period),
            counts["packaged"],
            counts["duplicate"],
            counts["skipped"],
            counts["filtered"],
            counts["failed"],
        )
        counts.clear()


async def run_status_reporter(reporter: StatusReporter) -> None:
    while True:
        await asyncio.sleep(_CHECK_INTERVAL_S)
        reporter.check(time.time())
