"""SQLite-backed Jetstream cursor + processed-record ledger.

The cursor (a unix-microsecond `time_us` value, per the legacy v1 wire --
see firehose.py) lets the firehose consumer resume after a restart; the
ledger makes per-record processing idempotent in case a commit gets
redelivered (Jetstream is at-least-once delivery, so this can happen).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


class State:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("CREATE TABLE IF NOT EXISTS cursor (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL)")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_records (
                at_uri TEXT PRIMARY KEY,
                torrent_path TEXT NOT NULL,
                processed_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def get_cursor(self) -> int | None:
        row = self._conn.execute("SELECT value FROM cursor WHERE id = 0").fetchone()
        return row[0] if row else None

    def set_cursor(self, value: int) -> None:
        self._conn.execute(
            "INSERT INTO cursor (id, value) VALUES (0, ?) ON CONFLICT (id) DO UPDATE SET value = excluded.value",
            (value,),
        )
        self._conn.commit()

    def is_processed(self, at_uri: str) -> bool:
        row = self._conn.execute("SELECT 1 FROM processed_records WHERE at_uri = ?", (at_uri,)).fetchone()
        return row is not None

    def mark_processed(self, at_uri: str, torrent_path: str, processed_at: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO processed_records (at_uri, torrent_path, processed_at) VALUES (?, ?, ?)",
            (at_uri, torrent_path, processed_at),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
