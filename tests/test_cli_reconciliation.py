from __future__ import annotations

import json
from pathlib import Path

import pytest

from deta import cli
from deta.storage import SQLiteStore


ARCHIVE_URL = (
    "https://data.binance.vision/data/spot/monthly/klines/"
    "BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
)


class FakeArchiveAdapter:
    market_type = "spot"

    def __init__(self, market: str) -> None:
        assert market == "spot"

    def list_monthly_archives(
        self, symbol: str, interval: str, start_month: str, end_month: str
    ) -> list[str]:
        assert (symbol, interval, start_month, end_month) == (
            "BTCUSDT", "1m", "2024-01", "2024-01"
        )
        return [ARCHIVE_URL]

    def list_daily_archives(
        self, dataset: str, symbol: str, interval: str, start_month: str, end_month: str
    ) -> list[str]:
        return []

    def close(self) -> None:
        pass


def _args(db_path: Path) -> list[str]:
    return [
        "--db", str(db_path),
        "reconcile-archive-klines",
        "--market", "spot",
        "--symbol", "BTCUSDT",
        "--interval", "1m",
        "--start-month", "2024-01",
        "--end-month", "2024-01",
    ]


def _register_imported_archive(db_path: Path) -> None:
    with SQLiteStore(db_path) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL,
            exchange_id="binance",
            market_type="spot",
            data_type="klines_1m",
            symbol="BTCUSDT",
            archive_period="2024-01",
        )
        store.update_archive_manifest(
            ARCHIVE_URL,
            status="verified",
            checksum_sha256="a" * 64,
            size_bytes=1234,
            row_count=10,
        )
        store.update_archive_manifest(
            ARCHIVE_URL,
            status="imported",
            imported_at="2026-10-24T00:00:00+00:00",
            row_count=10,
        )


def test_reconciliation_fails_when_source_archive_is_missing_from_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(tmp_path / "missing.sqlite3"))

    report = json.loads(capsys.readouterr().out)
    assert report["source_listed_archive_count"] == 1
    assert report["missing_from_manifest"] == [ARCHIVE_URL]
    assert exit_code == 1


def test_reconciliation_passes_only_when_listed_archive_is_imported_and_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "imported.sqlite3"
    _register_imported_archive(db_path)
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["source_listed_archive_count"] == 1
    assert report["imported_archives"] == [ARCHIVE_URL]
    assert report["missing_checksum_or_size"] == []
    assert report["missing_from_manifest"] == []
    assert exit_code == 0
