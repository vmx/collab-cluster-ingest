"""Fetch and parse the STAC item referenced by a matadisco record's `resource` field."""

from __future__ import annotations

from typing import Any

import httpx

from .retry import retry_async


class StacFetchError(Exception):
    """Raised when a STAC item can't be fetched or parsed."""


async def fetch_stac_item(client: httpx.AsyncClient, resource_url: str) -> dict[str, Any]:
    async def _get() -> dict[str, Any]:
        response = await client.get(resource_url)
        response.raise_for_status()
        return response.json()

    try:
        return await retry_async(_get)
    except httpx.HTTPError as exc:
        raise StacFetchError(f"failed to fetch STAC item from {resource_url}: {exc}") from exc
