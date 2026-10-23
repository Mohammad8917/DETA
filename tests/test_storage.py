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
