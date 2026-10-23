from pathlib import Path

import pytest

from deta.domain import DataRecord
from deta.storage import SQLiteStore


def test_batch_is_idempotent_and_checkpoint_is_saved(tmp_path: Path) -> None:
    path = tmp_path / "data.sqlite3"
    record = DataRecord("test", "spot", "trades", "ABC", 123, "id-1", {"price": "1.0"})
    with SQLiteStore(path) as store:
        assert store.save_batch([record], cursor="id-2") == 1
        assert store.save_batch([record], cursor="id-2") == 0
        assert store.get_checkpoint("test", "spot", "trades", "ABC") == "id-2"
        assert store.dataset_summary()[0]["rows"] == 1
    with SQLiteStore(path) as store:
        assert store.get_checkpoint("test", "spot", "trades", "ABC") == "id-2"
        assert store.dataset_summary()[0]["rows"] == 1


def test_rejects_mixed_dataset_batch(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        records = [DataRecord("x", "spot", "trades", "A", 1, "1", {}),
                   DataRecord("x", "spot", "trades", "B", 2, "2", {})]
        with pytest.raises(ValueError, match="exactly one dataset identity"):
            store.save_batch(records, cursor="3")


def test_status_includes_checkpoint_without_records(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        store.update_checkpoint("binance", "spot", "klines_1m", "EMPTY", None,
                                "partial", "No data returned")
        summary = store.dataset_summary()
        assert len(summary) == 1
        assert summary[0]["symbol"] == "EMPTY"
        assert summary[0]["rows"] == 0
        assert summary[0]["first_timestamp_ms"] is None
        assert summary[0]["detail"] == "No data returned"


def test_symbol_refresh_marks_missing_symbols_not_current(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        store.save_symbols([{"exchange_id": "binance", "market_type": "spot",
                             "symbol": "OLDUSDT", "status": "TRADING"}])
        store.save_symbols([{"exchange_id": "binance", "market_type": "spot",
                             "symbol": "NEWUSDT", "status": "TRADING"}])
        symbols = {item["symbol"]: item for item in store.get_symbols("binance", "spot")}
        assert symbols["OLDUSDT"]["is_currently_listed"] is False
        assert symbols["OLDUSDT"]["status"] == "NOT_PRESENT_IN_LATEST_DISCOVERY"
        assert symbols["NEWUSDT"]["is_currently_listed"] is True


def test_empty_symbol_discovery_is_rejected_without_mutating_existing_data(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        store.save_symbols([{"exchange_id": "binance", "market_type": "spot",
                             "symbol": "BTCUSDT", "status": "TRADING"}])
        with pytest.raises(ValueError, match="empty discovery"):
            store.save_symbols([])
        assert store.get_symbols("binance", "spot")[0]["symbol"] == "BTCUSDT"


def test_archive_manifest_persists_and_filters_states(tmp_path: Path) -> None:
    path = tmp_path / "data.sqlite3"
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    digest = "a" * 64
    with SQLiteStore(path) as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256=digest, size_bytes=1024, row_count=31,
        )
        store.update_archive_manifest(
            url, status="imported", imported_at="2026-10-24T00:00:00Z", row_count=31,
        )
        # Re-discovery must not silently downgrade an imported archive.
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        rows = store.archive_manifest_summary(
            exchange_id="binance", market_type="spot", data_type="klines_1m", symbol="BTCUSDT",
        )
        assert len(rows) == 1
        assert rows[0]["status"] == "imported"
        assert rows[0]["checksum_sha256"] == digest
        assert rows[0]["size_bytes"] == 1024
        assert rows[0]["row_count"] == 31
    with SQLiteStore(path) as reopened:
        assert reopened.archive_manifest_summary()[0]["status"] == "imported"


def test_archive_manifest_rejects_invalid_metadata_and_orphan_updates(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        with pytest.raises(ValueError, match="SHA-256"):
            store.upsert_archive_manifest(
                archive_url="https://example.test/a.zip", exchange_id="binance",
                market_type="spot", data_type="klines_1m", symbol="BTCUSDT",
                archive_period="2024-01", checksum_sha256="not-a-digest",
            )
        with pytest.raises(ValueError, match="must not be negative"):
            store.upsert_archive_manifest(
                archive_url="https://example.test/a.zip", exchange_id="binance",
                market_type="spot", data_type="klines_1m", symbol="BTCUSDT",
                archive_period="2024-01", size_bytes=-1,
            )
        with pytest.raises(KeyError, match="not registered"):
            store.update_archive_manifest(
                "https://example.test/missing.zip", status="failed", error="not found",
            )


def test_archive_manifest_rejects_unknown_status(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        with pytest.raises(ValueError, match="Unsupported archive manifest status"):
            store.upsert_archive_manifest(
                archive_url="https://example.test/a.zip", exchange_id="binance",
                market_type="spot", data_type="klines_1m", symbol="BTCUSDT",
                archive_period="2024-01", status="complete",
            )



def test_archive_manifest_checksum_change_invalidates_import_evidence(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        url = "https://example.test/BTCUSDT-1m-2024-01.zip"
        first_digest = "a" * 64
        second_digest = "b" * 64
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256=first_digest, size_bytes=100,
        )
        store.update_archive_manifest(
            url, status="imported", imported_at="2026-10-24T00:00:00Z", row_count=10,
        )

        # A rediscovery or re-verification of identical bytes must not erase import state.
        store.update_archive_manifest(url, status="verified", checksum_sha256=first_digest)
        row = store.archive_manifest_summary()[0]
        assert row["status"] == "imported"
        assert row["imported_at"] == "2026-10-24T00:00:00Z"

        # A different checksum represents different content; the old import timestamp
        # must be invalidated and the new content must pass verification/import again.
        store.update_archive_manifest(
            url, status="downloaded", checksum_sha256=second_digest, size_bytes=101,
        )
        row = store.archive_manifest_summary()[0]
        assert row["status"] == "downloaded"
        assert row["checksum_sha256"] == second_digest
        assert row["imported_at"] is None

        with pytest.raises(ValueError, match="must be verified"):
            store.update_archive_manifest(url, status="imported", checksum_sha256="c" * 64)
