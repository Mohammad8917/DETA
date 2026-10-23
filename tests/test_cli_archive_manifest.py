from __future__ import annotations

import hashlib
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
