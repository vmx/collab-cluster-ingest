"""Minimal retry-with-backoff helper.

Deliberately hand-rolled instead of pulling in a dependency like `tenacity`:
we only need "retry a couple of times with exponential backoff" around two
call sites (STAC fetch, image fetch), which doesn't warrant a general-purpose
retry policy library.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def retry_async(
    func: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay_seconds: float = 1.0,
) -> T:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return await func()
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(base_delay_seconds * (2**attempt))
    assert last_exc is not None
    raise last_exc
