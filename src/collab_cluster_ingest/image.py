"""True-color image acquisition for a Sentinel-2 STAC item.

Reads the STAC item's `visual` asset (a cloud-optimized GeoTIFF, e.g.
`TCI.tif`) directly over HTTP via GDAL's `/vsicurl/` virtual filesystem, and
selects one COG overview level below full resolution instead of fetching
the whole file. GDAL only issues HTTP range requests for the tiles that
overview actually needs. Confirmed empirically against a real Sentinel-2
`visual` asset: reading overview level 0 (5490x5490, half resolution) pulled
~2MB over the wire, against ~9.7MB for the full 10980x10980 file.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import rasterio
from rasterio.env import Env

# GDAL's OVERVIEW_LEVEL open option is 0-indexed from the most detailed
# overview (one step down from full resolution) -- "one size smaller".
_OVERVIEW_LEVEL = 0


async def fetch_true_color_image(stac_item: dict[str, Any], dest_dir: Path) -> Path:
    """Fetch one reduced-resolution overview of `stac_item`'s `visual` asset into `dest_dir`."""
    href = stac_item["assets"]["visual"]["href"]
    dest_path = dest_dir / f"{stac_item['id']}_tci.tif"

    await asyncio.to_thread(_download_overview, href, dest_path)
    return dest_path


def _download_overview(href: str, dest_path: Path) -> None:
    """Blocking GDAL I/O -- always call via `asyncio.to_thread`, never directly from async code."""
    with Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR"):
        with rasterio.open(f"/vsicurl/{href}", OVERVIEW_LEVEL=_OVERVIEW_LEVEL) as src:
            profile = src.profile
            data = src.read()

    with rasterio.open(dest_path, "w", **profile) as dst:
        dst.write(data)
