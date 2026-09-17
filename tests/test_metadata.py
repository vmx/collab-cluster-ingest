import json
from pathlib import Path

from collab_cluster_ingest.metadata import build_metadata

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "stac_item_sample.json").read_text())

MATADISCO_RECORD = {
    "publishedAt": "2026-09-17T10:00:00Z",
    "resource": (
        "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items/S2B_36LWK_20260917_0_L2A"
    ),
    "preview": {"mimeType": "image/jpeg", "url": "https://example.com/preview.jpg"},
}


def test_build_metadata_carries_matadisco_and_stac_fields():
    metadata = build_metadata(
        matadisco_uri="at://did:plc:example/cx.vmx.matadisco/abc123",
        matadisco_record=MATADISCO_RECORD,
        stac_item=FIXTURE,
    )

    assert metadata["matadisco"]["uri"] == "at://did:plc:example/cx.vmx.matadisco/abc123"
    assert metadata["matadisco"]["publishedAt"] == MATADISCO_RECORD["publishedAt"]
    assert metadata["stac"]["id"] == FIXTURE["id"]
    assert metadata["stac"]["collection"] == "sentinel-2-l2a"
    assert metadata["stac"]["properties"]["eo:cloud_cover"] == FIXTURE["properties"]["eo:cloud_cover"]
    assert "ingestedAt" in metadata
