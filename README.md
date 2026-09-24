collab-cluster-torrentizer
===========================

Tails [Jetstream](https://github.com/bluesky-social/jetstream) for
[matadisco](https://github.com/ipfs-fdn/matadisco) records describing
Sentinel-2 imagery, resolves them to their STAC item, and produces a
v2-only (BEP 52) `.torrent` file per record whose payload is a per-item
directory (`<output_dir>/<item id>/`, currently holding just the
true-color image, but built to hold more files per item later). Selected
STAC/matadisco metadata is embedded directly in the `.torrent` file itself
(a custom top-level bencode key), not shipped as an extra file alongside
the image.

The true-color image for each item is one COG overview level below full
resolution (half the resolution of the `visual` asset), read directly over
HTTP via GDAL's `/vsicurl/` -- only the bytes for that overview are
fetched, not the whole file.

Setup
-----

```console
> uv sync
```

Run
---

```console
> uv run collab-cluster-torrentizer
```

Configuration is via environment variables (see `Config` in
`src/collab_cluster_torrentizer/config.py`): `JETSTREAM_URL`, `OUTPUT_DIR`,
`STATE_DB_PATH`, `ALLOWED_PUBLISHER_DIDS` (comma-separated, optional),
`TARGET_STAC_COLLECTION`, `WORKER_CONCURRENCY`, `QUEUE_MAXSIZE`,
`HTTP_TIMEOUT_SECONDS`, `STATUS_INTERVAL_SECONDS`, `LAG_WARNING_SECONDS`,
`QUEUE_FULL_WARNING_SECONDS`.

Every `STATUS_INTERVAL_SECONDS` a status line is logged with how far the
stream position trails live, the queue fill, and how many records were
packaged, skipped as already processed, filtered out, or failed. Warnings are
logged when the lag or a full queue persist past their thresholds, and when a
resume lands later than the requested cursor (i.e. events were lost to
Jetstream's retention window).

Running in the background
--------------------------

To just run the pipeline in the current terminal session, use
`uv run collab-cluster-torrentizer` directly (see Run above).

For something that keeps running in the background and survives a
reboot/re-login, install it as a persistent `systemd --user` unit. This
requires lingering to be enabled for your user (`loginctl show-user
$USER -p Linger`) -- without it, `systemd --user` units (and their
manager) are killed as soon as your last session logs out, persistent
unit or not:

```console
> sudo loginctl enable-linger $USER   # if not already enabled
> cp deploy/.env.example deploy/.env   # then edit as needed
> ./deploy/service.sh install
> ./deploy/service.sh logs             # journalctl --user -u collab-cluster-torrentizer -f
> ./deploy/service.sh uninstall
```

The generated unit file (`deploy/collab-cluster-torrentizer.service`, built
from the checked-in `.template`) stays inside the repo; `install`
only adds a `systemctl --user link` symlink under
`~/.config/systemd/user` pointing back at it, and `uninstall` removes
that symlink again. Once installed, `./deploy/service.sh` also has
`start`/`stop`/`restart`/`status` for controlling the unit without
reinstalling it.

Tests
-----

```console
> uv run pytest
```

License
-------

This project is licensed under either of

 - Apache License, Version 2.0, ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
 - MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

at your option.

[LICENSE-APACHE]: ./LICENSE-APACHE
[LICENSE-MIT]: ./LICENSE-MIT
