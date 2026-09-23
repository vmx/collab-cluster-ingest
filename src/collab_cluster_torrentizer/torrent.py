"""Build a v2-only (BEP 52) .torrent file from a packaged item directory.

Metadata is embedded directly in the .torrent file as a custom top-level
bencode key, not shipped as an extra file in the downloadable payload --
same idea as the standard `comment`/`created by` keys; unrelated torrent
clients just ignore keys they don't recognize.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import libtorrent as lt

METADATA_KEY = b"collab-cluster-torrentizer-metadata"


def create_torrent(item_dir: Path, torrent_path: Path, *, metadata: dict[str, Any]) -> Path:
    """Create a v2-only torrent covering every file in `item_dir`.

    libtorrent's file paths are stored relative to `item_dir`'s parent, so
    piece hashing is rooted there too. `.pad` entries that may show up in the
    resulting file list are BEP 52 padding files, not real content --
    standards-compliant torrent clients filter them out.

    Writes the result to `torrent_path` and returns it.
    """
    files = lt.list_files(str(item_dir))
    torrent_creator = lt.create_torrent(files, flags=lt.create_torrent.v2_only)
    lt.set_piece_hashes(torrent_creator, str(item_dir.parent))

    entry = torrent_creator.generate()
    entry[METADATA_KEY] = json.dumps(metadata).encode()

    torrent_path.write_bytes(lt.bencode(entry))
    return torrent_path
