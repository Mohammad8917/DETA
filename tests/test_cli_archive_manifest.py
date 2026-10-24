from __future__ import annotations

import hashlib
import json
from pathlib import Path

from deta.domain import DataRecord
from deta.storage import SQLiteStore
import deta.cli as cli


ARCHIVE_URL = (
    "https://data.binance.vision/data/spot/monthly/klines/"
    "BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
)
ARCHIVE_BYTES = b"verified archive bytes"


class FakeSource:
    def __init__(self) -> None:
        pass

    def discover_symbols(self) -> list[dict[str, str]]:
        return []

    def close(self) -> None:
        pass


class FakeArchive:
    def __init__(self, market: str) -> None:
        self.market_type = {"spot": "spot", "usd-m": "usd_m", "coin-m": "coin_m"}[market]
        self.last_verified_checksum_sha256: str | None = None
        self.last_verified_size_bytes: int | None = None
        self.should_fail = False

    def list_archive_symbols(self, dataset: str, frequency: str = "monthly") -> list[str]:
        return ["BTCUSDT"]

    def list_monthly_archives(
        self, symbol: str, interval: str, start_month: str | None = None,
        end_month: str | None = None,
    ) -> list[str]:
        return [ARCHIVE_URL]

    def list_daily_archives(
        self, dataset: str, symbol: str, interval: str | None = None,
        start_month: str | None = None, end_month: str | None = None,
    ) -> list[str]:
        return []

    def fetch_kline_archive(self, url: str) -> list[DataRecord]:
        if self.should_fail:
            raise ValueError("checksum mismatch")
        self.last_verified_checksum_sha256 = hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        self.last_verified_size_bytes = len(ARCHIVE_BYTES)
        return [
            DataRecord(
                "binance", "spot", "klines_1m", "BTCUSDT", 1704067200000,
                "1704067200000", {"close": "2", "archive_url": url},
            )
        ]

    def list_monthly_price_kline_archives(
        self, symbol: str, interval: str, price_type: str,
        start_month: str | None = None, end_month: str | None = None,
    ) -> list[str]:
        return [
            f"https://data.binance.vision/data/futures/um/monthly/{price_type}Klines/"
            f"{symbol}/{interval}/{symbol}-{interval}-2024-01.zip"
        ]

    def fetch_price_kline_archive(self, url: str, price_type: str) -> list[DataRecord]:
        if self.should_fail:
            raise ValueError("checksum mismatch")
        self.last_verified_checksum_sha256 = hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        self.last_verified_size_bytes = len(ARCHIVE_BYTES)
        return [
            DataRecord(
                "binance", "usd_m", f"{price_type}_klines_1m", "BTCUSDT",
                1704067200000, "1704067200000",
                {"close": "2", "archive_url": url},
            )
        ]

    def close(self) -> None:
        pass


def test_kline_import_updates_manifest_only_after_verified_data_is_saved(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "BinanceSpotAdapter", FakeSource)
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_klines(
            store, "spot", ["BTCUSDT"], ["1m"], "2024-01", "2024-01"
        )
        assert result == 0
        manifest = store.archive_manifest_summary()
        assert len(manifest) == 1
        assert manifest[0]["status"] == "imported"
        assert manifest[0]["checksum_sha256"] == hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        assert manifest[0]["size_bytes"] == len(ARCHIVE_BYTES)
        assert manifest[0]["row_count"] == 1
        assert manifest[0]["imported_at"]
        summary = store.dataset_summary()
        assert summary[0]["rows"] == 1


def test_kline_import_marks_manifest_failed_when_archive_verification_fails(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "BinanceSpotAdapter", FakeSource)

    class FailingArchive(FakeArchive):
        def __init__(self, market: str) -> None:
            super().__init__(market)
            self.should_fail = True

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FailingArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_klines(
            store, "spot", ["BTCUSDT"], ["1m"], "2024-01", "2024-01"
        )
        assert result == 1
        manifest = store.archive_manifest_summary()
        assert len(manifest) == 1
        assert manifest[0]["status"] == "failed"
        assert "checksum mismatch" in manifest[0]["error"]
        assert manifest[0]["imported_at"] is None



class FakeDerivativesSource(FakeSource):
    market_type = "usd_m"

    def __init__(self, market: str) -> None:
        super().__init__()
        assert market in {"usd-m", "coin-m"}


class FakeFundingArchive(FakeArchive):
    def list_monthly_funding_rate_archives(
        self, symbol: str, start_month: str | None = None,
        end_month: str | None = None,
    ) -> list[str]:
        return [
            "https://data.binance.vision/data/futures/um/monthly/fundingRate/"
            "BTCUSDT/BTCUSDT-fundingRate-2024-01.zip"
        ]

    def fetch_funding_rate_archive(self, url: str) -> list[DataRecord]:
        if self.should_fail:
            raise ValueError("checksum mismatch")
        self.last_verified_checksum_sha256 = hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        self.last_verified_size_bytes = len(ARCHIVE_BYTES)
        return [
            DataRecord(
                "binance", "usd_m", "funding_rate", "BTCUSDT", 1704067200000,
                "1704067200000", {"funding_rate": "0.0001", "archive_url": url},
            )
        ]


def test_funding_rate_import_records_verified_manifest_after_save(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "BinanceDerivativesAdapter", FakeDerivativesSource)
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeFundingArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_funding_rates(
            store, "usd-m", ["BTCUSDT"], "2024-01", "2024-01"
        )
        assert result == 0
        manifest = store.archive_manifest_summary(
            exchange_id="binance", market_type="usd_m", data_type="funding_rate",
            symbol="BTCUSDT",
        )
        assert len(manifest) == 1
        assert manifest[0]["status"] == "imported"
        assert manifest[0]["checksum_sha256"] == hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        assert manifest[0]["size_bytes"] == len(ARCHIVE_BYTES)
        assert manifest[0]["row_count"] == 1
        assert manifest[0]["imported_at"]



class FakeTradeArchive(FakeArchive):
    def list_monthly_trade_archives(
        self, symbol: str, start_month: str | None = None,
        end_month: str | None = None,
    ) -> list[str]:
        return [
            "https://data.binance.vision/data/spot/monthly/trades/"
            "BTCUSDT/BTCUSDT-trades-2024-01.zip"
        ]

    def fetch_trade_archive(self, url: str) -> list[DataRecord]:
        if self.should_fail:
            raise ValueError("checksum mismatch")
        self.last_verified_checksum_sha256 = hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        self.last_verified_size_bytes = len(ARCHIVE_BYTES)
        return [
            DataRecord(
                "binance", "spot", "trades", "BTCUSDT", 1704067200000,
                "1001", {"price": "2", "archive_url": url},
            )
        ]


def test_trade_import_records_verified_manifest_after_save(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "BinanceSpotAdapter", FakeSource)
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeTradeArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_trade_dataset(
            store, "spot", ["BTCUSDT"], "2024-01", "2024-01", "trades"
        )
        assert result == 0
        manifest = store.archive_manifest_summary(
            exchange_id="binance", market_type="spot", data_type="trades",
            symbol="BTCUSDT",
        )
        assert len(manifest) == 1
        assert manifest[0]["status"] == "imported"
        assert manifest[0]["checksum_sha256"] == hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        assert manifest[0]["size_bytes"] == len(ARCHIVE_BYTES)
        assert manifest[0]["row_count"] == 1
        assert manifest[0]["imported_at"]


def test_price_kline_import_records_verified_manifest_after_save(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "BinanceDerivativesAdapter", FakeDerivativesSource)
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_price_klines(
            store, "usd-m", "markPrice", ["BTCUSDT"], ["1m"], "2024-01", "2024-01"
        )
        assert result == 0
        manifest = store.archive_manifest_summary(
            exchange_id="binance", market_type="usd_m",
            data_type="mark_price_klines_1m", symbol="BTCUSDT",
        )
        assert len(manifest) == 1
        assert manifest[0]["status"] == "imported"
        assert manifest[0]["checksum_sha256"] == hashlib.sha256(ARCHIVE_BYTES).hexdigest()
        assert manifest[0]["size_bytes"] == len(ARCHIVE_BYTES)
        assert manifest[0]["row_count"] == 1
        assert manifest[0]["imported_at"]


def test_price_kline_import_marks_manifest_failed_on_archive_verification_failure(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "BinanceDerivativesAdapter", FakeDerivativesSource)

    class FailingPriceArchive(FakeArchive):
        def __init__(self, market: str) -> None:
            super().__init__(market)
            self.should_fail = True

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FailingPriceArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_price_klines(
            store, "usd-m", "markPrice", ["BTCUSDT"], ["1m"], "2024-01", "2024-01"
        )
        assert result == 1
        manifest = store.archive_manifest_summary(
            exchange_id="binance", market_type="usd_m",
            data_type="mark_price_klines_1m", symbol="BTCUSDT",
        )
        assert len(manifest) == 1
        assert manifest[0]["status"] == "failed"
        assert "checksum mismatch" in manifest[0]["error"]
        assert manifest[0]["imported_at"] is None


def test_archive_manifest_cli_reports_status_counts(
    tmp_path: Path, capsys,
) -> None:
    database = tmp_path / "data.sqlite3"
    with SQLiteStore(database) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="discovered",
        )
    result = cli.main([
        "--db", str(database), "archive-manifest", "--market-type", "spot",
        "--symbol", "BTCUSDT",
    ])
    assert result == 0
    report = json.loads(capsys.readouterr().out)
    assert report["archive_count"] == 1
    assert report["status_counts"] == {"discovered": 1}
    assert report["archives"][0]["archive_url"] == ARCHIVE_URL
    assert "not proof" in report["coverage_claim"]

def test_empty_verified_archive_is_not_marked_imported(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    monkeypatch.setattr(cli, "BinanceSpotAdapter", FakeSource)

    class EmptyArchive(FakeArchive):
        def fetch_kline_archive(self, url: str) -> list[DataRecord]:
            self.last_verified_checksum_sha256 = hashlib.sha256(ARCHIVE_BYTES).hexdigest()
            self.last_verified_size_bytes = len(ARCHIVE_BYTES)
            return []

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", EmptyArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_klines(
            store, "spot", ["BTCUSDT"], ["1m"], "2024-01", "2024-01"
        )
        assert result == 0
        run_report = json.loads(capsys.readouterr().out)
        assert run_report["archives_verified"] == 1
        assert run_report["archives_imported"] == 0
        manifest = store.archive_manifest_summary(
            exchange_id="binance", market_type="spot", data_type="klines_1m",
            symbol="BTCUSDT",
        )
        assert len(manifest) == 1
        assert manifest[0]["status"] == "verified"
        assert manifest[0]["row_count"] == 0
        assert manifest[0]["imported_at"] is None
        checkpoint = store.dataset_summary()
        assert checkpoint[0]["status"] == "partial"
        assert "contained no kline rows" in checkpoint[0]["detail"]


def test_archive_without_checksum_metadata_is_not_counted_as_verified(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    monkeypatch.setattr(cli, "BinanceSpotAdapter", FakeSource)

    class MissingChecksumArchive(FakeArchive):
        def fetch_kline_archive(self, url: str) -> list[DataRecord]:
            return [
                DataRecord(
                    "binance", "spot", "klines_1m", "BTCUSDT", 1704067200000,
                    "1704067200000", {"close": "2", "archive_url": url},
                )
            ]

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", MissingChecksumArchive)
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        result = cli._backfill_archive_klines(
            store, "spot", ["BTCUSDT"], ["1m"], "2024-01", "2024-01"
        )
        assert result == 1
        report = json.loads(capsys.readouterr().out)
        assert report["archives_verified"] == 0
        assert report["archives_imported"] == 0
        manifest = store.archive_manifest_summary(
            exchange_id="binance", market_type="spot", data_type="klines_1m",
            symbol="BTCUSDT",
        )
        assert len(manifest) == 1
        assert manifest[0]["status"] == "failed"
        assert "without verified checksum metadata" in manifest[0]["error"]
        assert manifest[0]["imported_at"] is None


def test_reconcile_archive_klines_compares_source_listing_with_manifest(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    feb_url = (
        "https://data.binance.vision/data/spot/monthly/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-02.zip"
    )

    class ListedArchive(FakeArchive):
        def list_monthly_archives(
            self, symbol: str, interval: str, start_month: str | None = None,
            end_month: str | None = None,
        ) -> list[str]:
            return [ARCHIVE_URL, feb_url]

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", ListedArchive)
    database = tmp_path / "data.sqlite3"
    with SQLiteStore(database) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="verified", checksum_sha256=hashlib.sha256(ARCHIVE_BYTES).hexdigest(),
            size_bytes=len(ARCHIVE_BYTES), row_count=0,
        )
    result = cli.main([
        "--db", str(database), "reconcile-archive-klines",
        "--market", "spot", "--symbol", "BTCUSDT", "--interval", "1m",
        "--start-month", "2024-01", "--end-month", "2024-02",
    ])
    assert result == 0
    report = json.loads(capsys.readouterr().out)
    assert report["source_listed_archive_count"] == 2
    assert report["verified_but_not_imported"] == [ARCHIVE_URL]
    assert report["missing_from_manifest"] == [feb_url]
    assert report["imported_archives"] == []
    assert "cannot prove" in report["coverage_claim"]



def test_reconcile_archive_klines_reports_stale_manifest_entries_and_daily_fallback(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    daily_url = (
        "https://data.binance.vision/data/spot/daily/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-02-01.zip"
    )
    stale_url = (
        "https://data.binance.vision/data/spot/monthly/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    )

    class DailyFallbackArchive(FakeArchive):
        def list_monthly_archives(
            self, symbol: str, interval: str, start_month: str | None = None,
            end_month: str | None = None,
        ) -> list[str]:
            return []

        def list_daily_archives(
            self, dataset: str, symbol: str, interval: str | None = None,
            start_month: str | None = None, end_month: str | None = None,
        ) -> list[str]:
            return [daily_url]

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", DailyFallbackArchive)
    database = tmp_path / "data.sqlite3"
    with SQLiteStore(database) as store:
        store.upsert_archive_manifest(
            archive_url=stale_url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="imported", checksum_sha256=hashlib.sha256(ARCHIVE_BYTES).hexdigest(),
            size_bytes=len(ARCHIVE_BYTES), row_count=1, imported_at="2024-01-31T00:00:00+00:00",
        )
    result = cli.main([
        "--db", str(database), "reconcile-archive-klines",
        "--market", "spot", "--symbol", "BTCUSDT", "--interval", "1m",
        "--start-month", "2024-01", "--end-month", "2024-02",
    ])
    assert result == 0
    report = json.loads(capsys.readouterr().out)
    assert report["source_listed_archive_count"] == 1
    assert report["missing_from_manifest"] == [daily_url]
    assert report["manifest_entries_not_in_current_source_listing"] == [stale_url]



def test_reconcile_archive_klines_includes_daily_manifest_period_in_month_scope(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    daily_url = (
        "https://data.binance.vision/data/spot/daily/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-02-01.zip"
    )

    class DailyArchive(FakeArchive):
        def list_monthly_archives(
            self, symbol: str, interval: str, start_month: str | None = None,
            end_month: str | None = None,
        ) -> list[str]:
            return []

        def list_daily_archives(
            self, dataset: str, symbol: str, interval: str | None = None,
            start_month: str | None = None, end_month: str | None = None,
        ) -> list[str]:
            return [daily_url]

    monkeypatch.setattr(cli, "BinanceArchiveAdapter", DailyArchive)
    database = tmp_path / "data.sqlite3"
    with SQLiteStore(database) as store:
        store.upsert_archive_manifest(
            archive_url=daily_url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-02-01",
            status="imported", checksum_sha256=hashlib.sha256(ARCHIVE_BYTES).hexdigest(),
            size_bytes=len(ARCHIVE_BYTES), row_count=10, imported_at="2024-02-02T00:00:00+00:00",
        )
    result = cli.main([
        "--db", str(database), "reconcile-archive-klines",
        "--market", "spot", "--symbol", "BTCUSDT", "--interval", "1m",
        "--start-month", "2024-02", "--end-month", "2024-02",
    ])
    assert result == 0
    report = json.loads(capsys.readouterr().out)
    assert report["manifest_entries_in_scope"] == 1
    assert report["imported_archives"] == [daily_url]
    assert report["manifest_entries_not_in_current_source_listing"] == []


def test_audit_kline_gaps_reports_missing_timestamp_in_bounded_range(
    tmp_path: Path, capsys,
) -> None:
    database = tmp_path / "data.sqlite3"
    with SQLiteStore(database) as store:
        store.save_batch([
            DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 0, "0", {"open_time": 0}),
            DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 120_000, "120000", {"open_time": 120_000}),
        ], cursor="120001")
    result = cli.main([
        "--db", str(database), "audit-kline-gaps",
        "--market-type", "spot", "--symbol", "BTCUSDT", "--interval", "1m",
        "--start-ms", "0", "--end-ms", "120000",
    ])
    assert result == 1
    report = json.loads(capsys.readouterr().out)
    assert report["expected_candles"] == 3
    assert report["stored_distinct_timestamps"] == 2
    assert report["missing_candle_count"] == 1
    assert report["first_missing_timestamps_ms"] == [60_000]
    assert "does not prove source/archive completeness" in report["coverage_claim"]


def test_audit_kline_gaps_rejects_unaligned_range(tmp_path: Path, capsys) -> None:
    result = cli.main([
        "--db", str(tmp_path / "data.sqlite3"), "audit-kline-gaps",
        "--market-type", "spot", "--symbol", "BTCUSDT", "--interval", "1m",
        "--start-ms", "1", "--end-ms", "120000",
    ])
    assert result == 2
    assert "align" in capsys.readouterr().err
