"""Durable local storage. Checkpoints advance only in the same transaction as saved rows."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
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
            CREATE TABLE IF NOT EXISTS archive_manifest (
                archive_url TEXT PRIMARY KEY,
                exchange_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                data_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                archive_period TEXT NOT NULL,
                checksum_sha256 TEXT,
                size_bytes INTEGER,
                row_count INTEGER,
                status TEXT NOT NULL DEFAULT 'discovered',
                imported_at TEXT,
                error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CHECK (status IN ('discovered', 'downloaded', 'verified', 'imported',
                                  'failed', 'missing', 'unavailable'))
            );
            CREATE INDEX IF NOT EXISTS idx_archive_manifest_dataset
              ON archive_manifest(exchange_id, market_type, data_type, symbol, archive_period);
            CREATE TABLE IF NOT EXISTS archive_record_provenance (
                archive_url TEXT NOT NULL,
                exchange_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                data_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                record_key TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY (archive_url, record_key)
            );
            CREATE INDEX IF NOT EXISTS idx_archive_provenance_dataset_time
              ON archive_record_provenance(exchange_id, market_type, data_type, symbol, timestamp_ms);
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

    def save_archive_batch(
        self, archive_url: str, records: list[DataRecord], *, cursor: str | None,
        status: str = "partial", detail: str = "",
    ) -> int:
        """Atomically save archive records, per-archive provenance, and checkpoint state."""
        if not records:
            raise ValueError("Empty archive batches cannot be imported")
        manifest = self._connection.execute(
            """SELECT exchange_id, market_type, data_type, symbol, archive_period, status,
                      checksum_sha256, size_bytes, row_count
               FROM archive_manifest WHERE archive_url=?""",
            (archive_url,),
        ).fetchone()
        if manifest is None:
            raise KeyError(f"Archive URL is not registered in manifest: {archive_url}")
        (exchange_id, market_type, data_type, symbol, archive_period, manifest_status,
         checksum_sha256, size_bytes, manifest_row_count) = manifest
        if manifest_status != "verified":
            raise ValueError("Archive must be verified before its records can be imported")
        if (
            not isinstance(checksum_sha256, str)
            or len(checksum_sha256) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in checksum_sha256)
            or not isinstance(size_bytes, int)
            or size_bytes <= 0
        ):
            raise ValueError("Archive integrity metadata must be valid before import")
        if not isinstance(manifest_row_count, int) or manifest_row_count != len(records):
            raise ValueError("Archive record count does not match verified manifest row_count")
        identity = (exchange_id, market_type, data_type, symbol)
        if any((r.exchange_id, r.market_type, r.data_type, r.symbol) != identity for r in records):
            raise ValueError("Archive records do not match the manifest dataset identity")
        if re.fullmatch(r"\d{4}-\d{2}", archive_period):
            period_start = datetime.strptime(archive_period, "%Y-%m").replace(tzinfo=timezone.utc)
            period_end = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", archive_period):
            period_start = datetime.strptime(archive_period, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            period_end = period_start + timedelta(days=1)
        else:
            raise ValueError("Archive period must be a valid UTC month or day")
        start_ms = int(period_start.timestamp() * 1000)
        end_ms = int(period_end.timestamp() * 1000)
        if any(not start_ms <= record.timestamp_ms < end_ms for record in records):
            raise ValueError("Archive contains records outside its declared UTC period")
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
            self._connection.executemany(
                """INSERT INTO archive_record_provenance(
                       archive_url, exchange_id, market_type, data_type, symbol,
                       record_key, timestamp_ms, payload_sha256)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(archive_url, record_key) DO UPDATE SET
                       timestamp_ms=excluded.timestamp_ms, payload_sha256=excluded.payload_sha256""",
                [(archive_url, *identity, r.record_key, r.timestamp_ms,
                  hashlib.sha256(
                      json.dumps(r.payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
                  ).hexdigest()) for r in records],
            )
            self._connection.execute(
                """INSERT INTO checkpoints(exchange_id, market_type, data_type, symbol,
                   cursor, status, detail, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(exchange_id, market_type, data_type, symbol) DO UPDATE SET
                   cursor=excluded.cursor, status=excluded.status, detail=excluded.detail,
                   updated_at=CURRENT_TIMESTAMP""",
                (*identity, cursor, status, detail),
            )
            return inserted_rows

    def archive_provenance_summary(self, archive_url: str) -> dict[str, int]:
        """Verify archive provenance against stored record existence and payload fingerprints."""
        rows = self._connection.execute(
            """SELECT p.record_key, p.timestamp_ms, p.payload_sha256,
                      r.timestamp_ms, r.payload_json
               FROM archive_record_provenance p
               LEFT JOIN records r
                 ON r.exchange_id=p.exchange_id AND r.market_type=p.market_type
                AND r.data_type=p.data_type AND r.symbol=p.symbol
                AND r.record_key=p.record_key
               WHERE p.archive_url=?""",
            (archive_url,),
        ).fetchall()
        stored_count = 0
        payload_mismatches = 0
        timestamp_mismatches = 0
        for _record_key, expected_timestamp, expected_hash, stored_timestamp, payload_json in rows:
            if payload_json is None:
                continue
            stored_count += 1
            if stored_timestamp != expected_timestamp:
                timestamp_mismatches += 1
            actual_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
            if actual_hash != expected_hash:
                payload_mismatches += 1
        return {
            "provenance_rows": len(rows),
            "stored_record_rows": stored_count,
            "timestamp_mismatches": timestamp_mismatches,
            "payload_hash_mismatches": payload_mismatches,
        }

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

    def upsert_archive_manifest(
        self,
        *,
        archive_url: str,
        exchange_id: str,
        market_type: str,
        data_type: str,
        symbol: str,
        archive_period: str,
        status: str = "discovered",
        checksum_sha256: str | None = None,
        size_bytes: int | None = None,
        row_count: int | None = None,
        imported_at: str | None = None,
        error: str = "",
    ) -> None:
        """Persist one archive's audit state without downgrading a later state on rediscovery."""
        allowed = {"discovered", "downloaded", "verified", "imported", "failed", "missing", "unavailable"}
        if status not in allowed:
            raise ValueError(f"Unsupported archive manifest status: {status}")
        if not archive_url.strip() or not symbol.strip() or not archive_period.strip():
            raise ValueError("Archive URL, symbol, and period must be non-empty")
        if size_bytes is not None and size_bytes < 0:
            raise ValueError("Archive size must not be negative")
        if row_count is not None and row_count < 0:
            raise ValueError("Archive row count must not be negative")
        if checksum_sha256 is not None and (
            len(checksum_sha256) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in checksum_sha256)
        ):
            raise ValueError("Archive checksum must be a 64-character SHA-256 hex digest")
        with self._connection:
            self._connection.execute(
                """INSERT INTO archive_manifest(
                       archive_url, exchange_id, market_type, data_type, symbol, archive_period,
                       checksum_sha256, size_bytes, row_count, status, imported_at, error, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(archive_url) DO UPDATE SET
                       exchange_id=excluded.exchange_id,
                       market_type=excluded.market_type,
                       data_type=excluded.data_type,
                       symbol=excluded.symbol,
                       archive_period=excluded.archive_period,
                       checksum_sha256=COALESCE(excluded.checksum_sha256, archive_manifest.checksum_sha256),
                       size_bytes=COALESCE(excluded.size_bytes, archive_manifest.size_bytes),
                       row_count=COALESCE(excluded.row_count, archive_manifest.row_count),
                       status=CASE
                           WHEN archive_manifest.status='imported' AND excluded.status='discovered'
                           THEN archive_manifest.status ELSE excluded.status END,
                       imported_at=COALESCE(excluded.imported_at, archive_manifest.imported_at),
                       error=excluded.error,
                       updated_at=CURRENT_TIMESTAMP""",
                (archive_url, exchange_id, market_type, data_type, symbol, archive_period,
                 checksum_sha256, size_bytes, row_count, status, imported_at, error),
            )

    def update_archive_manifest(
        self,
        archive_url: str,
        *,
        status: str,
        checksum_sha256: str | None = None,
        size_bytes: int | None = None,
        row_count: int | None = None,
        imported_at: str | None = None,
        error: str = "",
    ) -> None:
        """Update an inventoried archive and invalidate stale import evidence on checksum change."""
        allowed = {"discovered", "downloaded", "verified", "imported", "failed", "missing", "unavailable"}
        if status not in allowed:
            raise ValueError(f"Unsupported archive manifest status: {status}")
        if checksum_sha256 is not None and (
            len(checksum_sha256) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in checksum_sha256)
        ):
            raise ValueError("Archive checksum must be a 64-character SHA-256 hex digest")
        if size_bytes is not None and size_bytes < 0:
            raise ValueError("Archive size must not be negative")
        if row_count is not None and row_count < 0:
            raise ValueError("Archive row count must not be negative")
        with self._connection:
            existing = self._connection.execute(
                "SELECT checksum_sha256, status, imported_at FROM archive_manifest WHERE archive_url=?",
                (archive_url,),
            ).fetchone()
            if existing is None:
                raise KeyError(f"Archive URL is not registered in manifest: {archive_url}")
            previous_checksum, previous_status, previous_imported_at = existing
            checksum_changed = (
                checksum_sha256 is not None
                and previous_checksum is not None
                and checksum_sha256.lower() != previous_checksum.lower()
            )
            # A changed archive is a different content version. Old import evidence must
            # not survive that change, even if the caller accidentally supplies an old timestamp.
            effective_imported_at = None if checksum_changed else (
                imported_at if imported_at is not None else previous_imported_at
            )
            if checksum_changed:
                # Provenance belongs to the exact archive content version. Keeping it
                # after a checksum change would make old rows look like new-import evidence.
                self._connection.execute(
                    "DELETE FROM archive_record_provenance WHERE archive_url=?",
                    (archive_url,),
                )
            effective_status = status
            if previous_status == "imported" and not checksum_changed and status in {
                "discovered", "downloaded", "verified"
            }:
                # Re-discovery/re-download/re-verification is not itself proof that the
                # already-imported, checksum-identical archive ceased to be imported.
                effective_status = "imported"
            if checksum_changed and status == "imported":
                raise ValueError("A changed archive checksum must be verified before it can be marked imported")
            cursor = self._connection.execute(
                """UPDATE archive_manifest SET
                       checksum_sha256=COALESCE(?, checksum_sha256),
                       size_bytes=COALESCE(?, size_bytes),
                       row_count=COALESCE(?, row_count),
                       status=?, imported_at=?, error=?, updated_at=CURRENT_TIMESTAMP
                   WHERE archive_url=?""",
                (checksum_sha256, size_bytes, row_count, effective_status,
                 effective_imported_at, error, archive_url),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Archive URL is not registered in manifest: {archive_url}")

    def archive_manifest_summary(
        self,
        *,
        exchange_id: str | None = None,
        market_type: str | None = None,
        data_type: str | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return auditable archive states, optionally filtered by dataset identity."""
        clauses: list[str] = []
        values: list[str] = []
        for column, value in (
            ("exchange_id", exchange_id), ("market_type", market_type),
            ("data_type", data_type), ("symbol", symbol),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            """SELECT archive_url, exchange_id, market_type, data_type, symbol, archive_period,
                      checksum_sha256, size_bytes, row_count, status, imported_at, error, updated_at
               FROM archive_manifest""" + where + " ORDER BY market_type, data_type, symbol, archive_period, archive_url",
            values,
        ).fetchall()
        keys = ("archive_url", "exchange_id", "market_type", "data_type", "symbol",
                "archive_period", "checksum_sha256", "size_bytes", "row_count", "status",
                "imported_at", "error", "updated_at")
        return [dict(zip(keys, row, strict=True)) for row in rows]

    def record_timestamps(
        self, *, exchange_id: str, market_type: str, data_type: str, symbol: str,
        start_ms: int, end_ms: int,
    ) -> list[int]:
        """Read distinct stored timestamps for one bounded dataset window, in ascending order."""
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("Invalid timestamp range")
        rows = self._connection.execute(
            """SELECT DISTINCT timestamp_ms FROM records
               WHERE exchange_id=? AND market_type=? AND data_type=? AND symbol=?
                 AND timestamp_ms>=? AND timestamp_ms<=?
               ORDER BY timestamp_ms""",
            (exchange_id, market_type, data_type, symbol, start_ms, end_ms),
        ).fetchall()
        return [int(row[0]) for row in rows]

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
