"""Pure predicates for deciding which matadisco records to act on.

matadisco is meant to host many kinds of datasets (see
https://github.com/ipfs-fdn/matadisco); this project only acts on Sentinel-2
imagery, so records are filtered in tiers: collection (done server-side by
Jetstream, double-checked here), publisher DID (optional allowlist), and
finally the STAC item's own `collection` field once it's been fetched.
"""

from __future__ import annotations

from typing import Any


def is_target_publisher(did: str, allowed_dids: tuple[str, ...]) -> bool:
    """An empty allowlist means "accept any publisher"."""
    return not allowed_dids or did in allowed_dids


def is_sentinel2_stac(stac_item: dict[str, Any], target_collection: str) -> bool:
    return stac_item.get("collection") == target_collection and "visual" in stac_item.get("assets", {})
