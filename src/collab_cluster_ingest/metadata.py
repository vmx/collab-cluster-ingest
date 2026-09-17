"""Build the metadata.json payload packaged alongside each torrent's image."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

# STAC `properties` fields worth carrying over into our metadata.json.
_PROPERTIES_TO_KEEP = (
    "created",
    "datetime",
    "eo:cloud_cover",
    "platform",
    "instruments",
    "proj:epsg",
)


def build_metadata(
    *,
    matadisco_uri: str,
    matadisco_record: dict[str, Any],
    stac_item: dict[str, Any],
) -> dict[str, Any]:
    properties = stac_item.get("properties", {})
    return {
        "matadisco": {
            "uri": matadisco_uri,
            "publishedAt": matadisco_record.get("publishedAt"),
            "resource": matadisco_record.get("resource"),
            "preview": matadisco_record.get("preview"),
        },
        "stac": {
            "id": stac_item.get("id"),
            "collection": stac_item.get("collection"),
            "bbox": stac_item.get("bbox"),
            "geometry": stac_item.get("geometry"),
            "properties": {key: properties[key] for key in _PROPERTIES_TO_KEEP if key in properties},
        },
        "ingestedAt": datetime.now(UTC).isoformat(),
    }
