"""Local durable tick outbox. Only acknowledged DB batches are deleted."""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import orjson


class TickSpoolFull(RuntimeError):  # noqa: N818 - capacity signal
    pass


class TickSpool:
    def __init__(self, path: str, *, max_bytes: int = 256 * 1024 * 1024):
        target = Path(path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        self.path = target
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        self._db = sqlite3.connect(target, timeout=1, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                tick_time TEXT NOT NULL,
                payload BLOB NOT NULL,
                UNIQUE(symbol, tick_time)
            );
            CREATE TABLE IF NOT EXISTS spool_state (
                id INTEGER PRIMARY KEY CHECK (id=1), rows INTEGER NOT NULL, bytes INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO spool_state
                SELECT 1, count(*), coalesce(sum(length(payload)),0) FROM ticks;
        """)
        self._db.commit()

    def append(self, row: dict[str, Any]) -> int:
        payload = orjson.dumps(row)
        key = (row["symbol"], row["time_msc"].isoformat())
        with self._lock, self._db:
            exists = self._db.execute(
                "SELECT id FROM ticks WHERE symbol=? AND tick_time=?", key
            ).fetchone()
            count, size = self._db.execute("SELECT rows, bytes FROM spool_state WHERE id=1").fetchone()
            if exists:
                return count
            if size + len(payload) > self.max_bytes:
                raise TickSpoolFull("Durable tick spool capacity exceeded; source coverage is at risk")
            self._db.execute("INSERT INTO ticks(symbol,tick_time,payload) VALUES(?,?,?)", (*key, payload))
            self._db.execute("UPDATE spool_state SET rows=rows+1, bytes=bytes+? WHERE id=1", (len(payload),))
            return count + 1

    def peek(self, limit: int = 5000) -> tuple[int | None, list[dict[str, Any]]]:
        with self._lock:
            records = self._db.execute("SELECT id,payload FROM ticks ORDER BY id LIMIT ?", (limit,)).fetchall()
        rows = []
        for _seq, payload in records:
            row = orjson.loads(payload)
            row["time_msc"] = datetime.fromisoformat(row["time_msc"])
            rows.append(row)
        return (records[-1][0] if records else None), rows

    def acknowledge(self, last_id: int) -> int:
        with self._lock, self._db:
            count, size = self._db.execute(
                "SELECT count(*),coalesce(sum(length(payload)),0) FROM ticks WHERE id<=?", (last_id,)
            ).fetchone()
            self._db.execute("DELETE FROM ticks WHERE id<=?", (last_id,))
            self._db.execute("UPDATE spool_state SET rows=rows-?,bytes=bytes-? WHERE id=1", (count, size))
            return self._db.execute("SELECT rows FROM spool_state WHERE id=1").fetchone()[0]

    def stats(self) -> dict[str, int]:
        with self._lock:
            count, size = self._db.execute("SELECT rows, bytes FROM spool_state WHERE id=1").fetchone()
        return {"pending": count, "payload_bytes": size}

    def close(self) -> None:
        with self._lock:
            self._db.close()
