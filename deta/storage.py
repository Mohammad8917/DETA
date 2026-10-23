"""Durable local storage. Checkpoints advance only in the same transaction as saved rows."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from deta.domain import DataRecord


class SQLiteStore:
    """A restart-safe store for raw public payloads and per-dataset progress."""

    def __init__(self, database_path: Path) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(database_path, timeout=30.0)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                exchange_id TEXT NOT NULL, market_type TEXT NOT NULL, data_type TEXT NOT NULL,
                symbol TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, record_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (exchange_id, market_type, data_type, symbol, record_key)
            );
            CREATE INDEX IF NOT EXISTS idx_records_time
              ON records(exchange_id, market_type, data_type, symbol, timestamp_ms);
            CREATE TABLE IF NOT EXISTS checkpoints (
                exchange_id TEXT NOT NULL, market_type TEXT NOT NULL, data_type TEXT NOT NULL,
                symbol TEXT NOT NULL, cursor TEXT, status TEXT NOT NULL DEFAULT 'partial',
                detail TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange_id, market_type, data_type, symbol)
            );
            CREATE TABLE IF NOT EXISTS symbols (
                exchange_id TEXT NOT NULL, market_type TEXT NOT NULL, symbol TEXT NOT NULL,
                metadata_json TEXT NOT NULL, discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange_id, market_type, symbol)
            );
            """
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def save_symbols(self, symbols: list[dict[str, Any]]) -> None:
        """Refresh current metadata while retaining symbols absent from the latest discovery."""
        if not symbols:
            raise ValueError("Refusing to replace symbol metadata with an empty discovery result")
        identities = {(str(s["exchange_id"]), str(s["market_type"])) for s in symbols}
        if len(identities) != 1:
            raise ValueError("A symbol discovery batch must contain exactly one exchange and market")
        exchange_id, market_type = next(iter(identities))
        incoming_names = {str(s["symbol"]) for s in symbols}
        with self._connection:
            existing = self._connection.execute(
                """SELECT symbol, metadata_json FROM symbols
                   WHERE exchange_id=? AND market_type=?""",
                (exchange_id, market_type),
            ).fetchall()
            for symbol, metadata_json in existing:
                if symbol not in incoming_names:
                    metadata = json.loads(metadata_json)
                    metadata["status"] = "NOT_PRESENT_IN_LATEST_DISCOVERY"
                    metadata["is_currently_listed"] = False
                    self._connection.execute(
                        """UPDATE symbols SET metadata_json=?
                           WHERE exchange_id=? AND market_type=? AND symbol=?""",
                        (json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                         exchange_id, market_type, symbol),
                    )
            self._connection.executemany(
                """INSERT INTO symbols(exchange_id, market_type, symbol, metadata_json)
                   VALUES (?, ?, ?, ?) ON CONFLICT(exchange_id, market_type, symbol)
                   DO UPDATE SET metadata_json=excluded.metadata_json,
                                 discovered_at=CURRENT_TIMESTAMP""",
                [(s["exchange_id"], s["market_type"], s["symbol"],
                  json.dumps({**s, "is_currently_listed": True},
                             ensure_ascii=False, sort_keys=True)) for s in symbols],
            )

    def get_symbols(self, exchange_id: str, market_type: str) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT metadata_json FROM symbols WHERE exchange_id=? AND market_type=? ORDER BY symbol",
            (exchange_id, market_type),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def get_checkpoint(self, exchange_id: str, market_type: str,
                       data_type: str, symbol: str) -> str | None:
        row = self._connection.execute(
            """SELECT cursor FROM checkpoints WHERE exchange_id=? AND market_type=?
               AND data_type=? AND symbol=?""",
            (exchange_id, market_type, data_type, symbol),
        ).fetchone()
        return None if row is None else row[0]

    def save_batch(self, records: list[DataRecord], *, cursor: str | None,
                   status: str = "partial", detail: str = "") -> int:
        if not records:
            raise ValueError("Empty batches cannot advance a checkpoint")
        first = records[0]
        identity = (first.exchange_id, first.market_type, first.data_type, first.symbol)
        if any((r.exchange_id, r.market_type, r.data_type, r.symbol) != identity for r in records):
            raise ValueError("A storage batch must contain exactly one dataset identity")
        with self._connection:
            before_records = self._connection.total_changes
            self._connection.executemany(
                """INSERT INTO records(exchange_id, market_type, data_type, symbol,
                   timestamp_ms, record_key, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(exchange_id, market_type, data_type, symbol, record_key) DO NOTHING""",
                [(r.exchange_id, r.market_type, r.data_type, r.symbol, r.timestamp_ms,
                  r.record_key, json.dumps(r.payload, ensure_ascii=False, sort_keys=True))
                 for r in records],
            )
            inserted_rows = self._connection.total_changes - before_records
            self._connection.execute(
                """INSERT INTO checkpoints(exchange_id, market_type, data_type, symbol,
                   cursor, status, detail, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(exchange_id, market_type, data_type, symbol) DO UPDATE SET
                   cursor=excluded.cursor, status=excluded.status, detail=excluded.detail,
                   updated_at=CURRENT_TIMESTAMP""",
                (*identity, cursor, status, detail),
            )
            return inserted_rows

    def update_checkpoint(self, exchange_id: str, market_type: str, data_type: str,
                          symbol: str, cursor: str | None, status: str, detail: str = "") -> None:
        with self._connection:
            self._connection.execute(
                """INSERT INTO checkpoints(exchange_id, market_type, data_type, symbol,
                   cursor, status, detail, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(exchange_id, market_type, data_type, symbol) DO UPDATE SET
                   cursor=excluded.cursor, status=excluded.status, detail=excluded.detail,
                   updated_at=CURRENT_TIMESTAMP""",
                (exchange_id, market_type, data_type, symbol, cursor, status, detail),
            )

    def dataset_summary(self) -> list[dict[str, Any]]:
        """Include attempted datasets even when their first request yielded no records."""
        rows = self._connection.execute(
            """SELECT c.exchange_id, c.market_type, c.data_type, c.symbol,
                      COUNT(r.record_key), MIN(r.timestamp_ms), MAX(r.timestamp_ms),
                      c.status, c.detail
               FROM checkpoints c
               LEFT JOIN records r USING(exchange_id, market_type, data_type, symbol)
               GROUP BY c.exchange_id, c.market_type, c.data_type, c.symbol, c.status, c.detail
               ORDER BY c.exchange_id, c.market_type, c.data_type, c.symbol"""
        ).fetchall()
        keys = ("exchange_id", "market_type", "data_type", "symbol", "rows",
                "first_timestamp_ms", "last_timestamp_ms", "status", "detail")
        return [dict(zip(keys, row, strict=True)) for row in rows]

    def __enter__(self) -> SQLiteStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
