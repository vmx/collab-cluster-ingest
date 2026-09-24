"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

# The matadisco lexicon NSID (https://github.com/ipfs-fdn/matadisco).
MATADISCO_COLLECTION = "cx.vmx.matadisco"

# Sent with every outbound request (STAC API, imagery, Jetstream) so operators know whom to contact.
USER_AGENT = "Volker Mische (https://vmx.cx/)"


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in value.split(",") if v.strip())


@dataclass(frozen=True)
class Config:
    jetstream_url: str = "wss://jetstream1.us-east.bsky.network"
    output_dir: str = "./output"
    state_db_path: str = "./state.sqlite3"
    # Empty means "accept matadisco records from any publisher DID".
    allowed_publisher_dids: tuple[str, ...] = ()
    target_stac_collection: str = "sentinel-2-l2a"
    worker_concurrency: int = 4
    queue_maxsize: int = 100
    http_timeout_seconds: float = 30.0
    # How often a status line (lag, queue, per-outcome counts) is logged.
    status_interval_seconds: float = 300.0
    # Warn once the stream position trails wall-clock time by more than this.
    lag_warning_seconds: float = 600.0
    # Warn once the worker queue has been full (firehose blocked) for longer than this.
    queue_full_warning_seconds: float = 180.0

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        env = env if env is not None else os.environ
        return cls(
            jetstream_url=env.get("JETSTREAM_URL", cls.jetstream_url),
            output_dir=env.get("OUTPUT_DIR", cls.output_dir),
            state_db_path=env.get("STATE_DB_PATH", cls.state_db_path),
            allowed_publisher_dids=_split_csv(env.get("ALLOWED_PUBLISHER_DIDS", "")),
            target_stac_collection=env.get("TARGET_STAC_COLLECTION", cls.target_stac_collection),
            worker_concurrency=int(env.get("WORKER_CONCURRENCY", cls.worker_concurrency)),
            queue_maxsize=int(env.get("QUEUE_MAXSIZE", cls.queue_maxsize)),
            http_timeout_seconds=float(env.get("HTTP_TIMEOUT_SECONDS", cls.http_timeout_seconds)),
            status_interval_seconds=float(env.get("STATUS_INTERVAL_SECONDS", cls.status_interval_seconds)),
            lag_warning_seconds=float(env.get("LAG_WARNING_SECONDS", cls.lag_warning_seconds)),
            queue_full_warning_seconds=float(env.get("QUEUE_FULL_WARNING_SECONDS", cls.queue_full_warning_seconds)),
        )
