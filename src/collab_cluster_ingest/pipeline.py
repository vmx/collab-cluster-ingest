"""Per-record pipeline: matadisco event -> STAC item -> image -> packaged torrent."""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
import rasterio.errors

from .config import Config
from .filters import is_sentinel2_stac
from .firehose import MatadiscoEvent
from .image import fetch_true_color_image
from .metadata import build_metadata
from .packaging import package_image
from .stac_client import StacFetchError, fetch_stac_item
from .state import State
from .torrent import create_torrent

logger = logging.getLogger(__name__)


async def process_event(event: MatadiscoEvent, *, config: Config, state: State, client: httpx.AsyncClient) -> None:
    if state.is_processed(event.at_uri):
        return

    resource_url = event.record.get("resource")
    if not resource_url:
        logger.warning("matadisco record %s has no resource, skipping", event.at_uri)
        return

    try:
        stac_item = await fetch_stac_item(client, resource_url)
    except StacFetchError:
        logger.exception("failed to fetch STAC item for %s", event.at_uri)
        return

    if not is_sentinel2_stac(stac_item, config.target_stac_collection):
        logger.debug("%s is not a %s STAC item, skipping", event.at_uri, config.target_stac_collection)
        return

    item_id = stac_item["id"]
    output_dir = Path(config.output_dir)

    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            image_path = await fetch_true_color_image(stac_item, Path(tmp_dir))
        except rasterio.errors.RasterioIOError:
            logger.exception("failed to fetch true-color image for %s", event.at_uri)
            return

        metadata = build_metadata(matadisco_uri=event.at_uri, matadisco_record=event.record, stac_item=stac_item)
        item_dir = package_image(output_dir, item_id, image_path)

    torrent_path = output_dir / f"{item_id}.torrent"
    create_torrent(item_dir, torrent_path, metadata=metadata)

    state.mark_processed(event.at_uri, str(torrent_path), datetime.now(UTC).isoformat())
    logger.info("packaged %s -> %s", event.at_uri, torrent_path)
