"""Exercise the packaging + torrent-creation half of the pipeline end-to-end,
using a fixture file in place of the (currently stubbed) image-acquisition step.
"""

import json
from pathlib import Path

import libtorrent as lt

from collab_cluster_ingest.metadata import build_metadata
from collab_cluster_ingest.packaging import package_image
from collab_cluster_ingest.torrent import METADATA_KEY, create_torrent


def test_package_and_torrent_creation_is_v2_only_with_embedded_metadata(tmp_path: Path):
    image_path = tmp_path / "true_color.png"
    image_path.write_bytes(b"not a real png, just fixture bytes")

    metadata = build_metadata(
        matadisco_uri="at://did:plc:example/cx.vmx.matadisco/abc123",
        matadisco_record={"publishedAt": "2026-09-17T10:00:00Z", "resource": "https://example.com/item"},
        stac_item={"id": "TEST_ITEM", "collection": "sentinel-2-l2a", "bbox": [0, 0, 1, 1], "properties": {}},
    )

    output_dir = tmp_path / "output"
    item_dir = package_image(output_dir, "TEST_ITEM", image_path)
    assert item_dir == output_dir / "TEST_ITEM"
    assert (item_dir / "true_color.png").read_bytes() == image_path.read_bytes()
    assert not (item_dir / "metadata.json").exists()

    torrent_path = output_dir / "TEST_ITEM.torrent"
    create_torrent(item_dir, torrent_path, metadata=metadata)

    info = lt.torrent_info(str(torrent_path))
    assert info.info_hashes().has_v2()
    assert not info.info_hashes().has_v1()

    # Only the image is part of the downloadable payload -- metadata is not a file.
    # (BEP 52 pad-file entries, flagged with flag_pad_file, are filtered out the
    # same way a real client would; they aren't real content.)
    layout = info.layout()
    file_names = {
        Path(layout.file_path(i)).name
        for i in range(layout.num_files())
        if not (layout.file_flags(i) & lt.file_storage.flag_pad_file)
    }
    assert file_names == {"true_color.png"}

    decoded = lt.bdecode(torrent_path.read_bytes())
    embedded_metadata = json.loads(decoded[METADATA_KEY])
    assert embedded_metadata == metadata
