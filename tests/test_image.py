"""Tests for the true-color image fetch, entirely offline.

GDAL's `/vsicurl/` needs a real HTTP server that understands `Range`
requests (Python's stdlib `http.server` doesn't support them), so this
spins up a tiny local one serving a synthetic COG -- no internet access
involved.
"""

from __future__ import annotations

import http.server
import threading
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_origin

from collab_cluster_torrentizer.image import fetch_true_color_image

_FULL_SIZE = 512
_OVERVIEW_FACTORS = [2, 4]


class _RangeRequestHandler(http.server.BaseHTTPRequestHandler):
    """Minimal handler that serves `self.server.file_bytes`, honoring `Range`."""

    def _serve(self, *, head_only: bool) -> None:
        data: bytes = self.server.file_bytes  # type: ignore[attr-defined]
        length = len(data)

        range_header = self.headers.get("Range")
        if range_header:
            start_s, _, end_s = range_header.partition("=")[2].partition("-")
            start = int(start_s) if start_s else 0
            end = min(int(end_s), length - 1) if end_s else length - 1
            chunk = data[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{length}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            if not head_only:
                self.wfile.write(chunk)
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            if not head_only:
                self.wfile.write(data)

    def do_HEAD(self) -> None:  # noqa: N802
        self._serve(head_only=True)

    def do_GET(self) -> None:  # noqa: N802
        self._serve(head_only=False)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # keep test output quiet


def _build_fixture_cog(path: Path) -> None:
    rng = np.random.default_rng(seed=0)
    data = rng.integers(0, 255, size=(3, _FULL_SIZE, _FULL_SIZE), dtype="uint8")
    transform = from_origin(500_000, 8_500_000, 10, 10)

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_FULL_SIZE,
        width=_FULL_SIZE,
        count=3,
        dtype="uint8",
        crs="EPSG:32736",
        transform=transform,
        tiled=True,
        blockxsize=256,
        blockysize=256,
    ) as dst:
        dst.write(data)

    with rasterio.open(path, "r+") as dst:
        dst.build_overviews(_OVERVIEW_FACTORS, Resampling.average)


@pytest.fixture
def local_cog_url(tmp_path: Path) -> Iterator[str]:
    """Serve a synthetic COG (with overviews) over local HTTP; yield its URL."""
    cog_path = tmp_path / "source.tif"
    _build_fixture_cog(cog_path)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RangeRequestHandler)
    server.file_bytes = cog_path.read_bytes()  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        port = server.server_address[1]
        yield f"http://127.0.0.1:{port}/source.tif"
    finally:
        server.shutdown()
        thread.join()


async def test_fetch_true_color_image_reads_first_overview_level(local_cog_url: str, tmp_path: Path):
    stac_item = {"id": "TEST_ITEM", "assets": {"visual": {"href": local_cog_url}}}
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()

    image_path = await fetch_true_color_image(stac_item, dest_dir)

    assert image_path == dest_dir / "TEST_ITEM_tci.tif"
    with rasterio.open(image_path) as src:
        # Overview level 0 is the first (least-decimated) overview -- one
        # size smaller than the full _FULL_SIZE x _FULL_SIZE image.
        expected_size = _FULL_SIZE // _OVERVIEW_FACTORS[0]
        assert (src.width, src.height) == (expected_size, expected_size)
        assert src.count == 3
        assert src.dtypes == ("uint8", "uint8", "uint8")
