"""Move a fetched image into its final per-item directory under the output directory.

Each item gets its own `<output_dir>/<item_id>/` directory rather than a bare
file: today there's only the one true-color image, but this leaves room for
more files per item (extra bands, alternate resolutions, ...) later on.
"""

from __future__ import annotations

import shutil
from pathlib import Path


def package_image(output_dir: Path, item_id: str, image_path: Path) -> Path:
    """Copy `image_path` into `<output_dir>/<item_id>/`, keeping its filename.

    Returns the item directory -- the torrent is built over the whole
    directory, not just this one file.
    """
    item_dir = output_dir / item_id
    item_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(image_path, item_dir / image_path.name)
    return item_dir
