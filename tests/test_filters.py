import json
from pathlib import Path

from collab_cluster_ingest.filters import is_sentinel2_stac, is_target_publisher

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "stac_item_sample.json").read_text())


def test_is_sentinel2_stac_matches_real_item():
    assert is_sentinel2_stac(FIXTURE, "sentinel-2-l2a")


def test_is_sentinel2_stac_rejects_other_collection():
    assert not is_sentinel2_stac(FIXTURE, "some-other-collection")


def test_is_sentinel2_stac_rejects_missing_visual_asset():
    item = {**FIXTURE, "assets": {k: v for k, v in FIXTURE["assets"].items() if k != "visual"}}
    assert not is_sentinel2_stac(item, "sentinel-2-l2a")


def test_is_target_publisher_empty_allowlist_accepts_any():
    assert is_target_publisher("did:plc:anyone", ())


def test_is_target_publisher_allowlist_filters():
    assert is_target_publisher("did:plc:allowed", ("did:plc:allowed",))
    assert not is_target_publisher("did:plc:other", ("did:plc:allowed",))
