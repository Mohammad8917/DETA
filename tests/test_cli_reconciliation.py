from __future__ import annotations

import json
from pathlib import Path

import pytest

from deta import cli
from deta.domain import DataRecord
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
        records = [
            DataRecord(
                "binance", "spot", "klines_1m", "BTCUSDT",
                1_704_067_200_000 + index * 60_000, str(1_704_067_200_000 + index * 60_000),
                {"close": "1"},
            )
            for index in range(10)
        ]
        store.save_archive_batch(
            ARCHIVE_URL, records, cursor=str(records[-1].timestamp_ms + 1)
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



def test_reconciliation_fails_when_manifest_says_imported_but_database_rows_are_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "missing-imported-rows.sqlite3"
    _register_imported_archive(db_path)
    # Remove all records while preserving the imported manifest, simulating stale or
    # manually fabricated import evidence.
    with SQLiteStore(db_path) as store:
        store._connection.execute("DELETE FROM records")
        store._connection.commit()
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["imported_row_count_mismatches"] == [{
        "archive_url": ARCHIVE_URL,
        "archive_period": "2024-01",
        "manifest_row_count": 10,
        "archive_provenance_rows": 10,
        "stored_record_rows_with_provenance": 0,
        "timestamp_mismatches": 0,
        "payload_hash_mismatches": 0,
    }]
    assert exit_code == 1

def test_reconciliation_fails_when_archive_is_verified_but_not_imported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "verified.sqlite3"
    with SQLiteStore(db_path) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL,
            exchange_id="binance",
            market_type="spot",
            data_type="klines_1m",
            symbol="BTCUSDT",
            archive_period="2024-01",
            status="verified",
            checksum_sha256="b" * 64,
            size_bytes=1234,
            row_count=10,
        )
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["verified_but_not_imported"] == [ARCHIVE_URL]
    assert exit_code == 1


def test_reconciliation_fails_when_manifest_has_unlisted_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "extra.sqlite3"
    extra_url = ARCHIVE_URL.replace("2024-01.zip", "2024-01-extra.zip")
    with SQLiteStore(db_path) as store:
        for url in (ARCHIVE_URL, extra_url):
            store.upsert_archive_manifest(
                archive_url=url,
                exchange_id="binance",
                market_type="spot",
                data_type="klines_1m",
                symbol="BTCUSDT",
                archive_period="2024-01",
                status="discovered",
                checksum_sha256="c" * 64,
                size_bytes=1234,
                row_count=10,
                imported_at="2026-10-24T00:00:00+00:00",
            )
            # Deliberately corrupt each row to test fail-closed reconciliation.
            store._connection.execute(
                "UPDATE archive_manifest SET status='imported' WHERE archive_url=?",
                (url,),
            )
        store._connection.commit()
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["manifest_entries_not_in_current_source_listing"] == [extra_url]
    assert exit_code == 1


def test_reconciliation_fails_when_source_listing_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    def no_monthly_archives(self, symbol, interval, start_month, end_month):
        return []

    def no_daily_archives(self, dataset, symbol, interval, start_month, end_month):
        return []

    monkeypatch.setattr(FakeArchiveAdapter, "list_monthly_archives", no_monthly_archives)
    monkeypatch.setattr(FakeArchiveAdapter, "list_daily_archives", no_daily_archives)

    exit_code = cli.main(_args(tmp_path / "empty-source.sqlite3"))

    report = json.loads(capsys.readouterr().out)
    assert report["source_listed_archive_count"] == 0
    assert exit_code == 1


def test_reconciliation_fails_when_imported_manifest_lacks_checksum_or_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "missing-integrity.sqlite3"
    with SQLiteStore(db_path) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL,
            exchange_id="binance",
            market_type="spot",
            data_type="klines_1m",
            symbol="BTCUSDT",
            archive_period="2024-01",
            status="discovered",
            imported_at="2026-10-24T00:00:00+00:00",
            row_count=10,
        )
        # Deliberately corrupt persisted state to test fail-closed reconciliation.
        store._connection.execute(
            "UPDATE archive_manifest SET status='imported' WHERE archive_url=?",
            (ARCHIVE_URL,),
        )
        store._connection.commit()
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["missing_checksum_or_size"] == [ARCHIVE_URL]
    assert exit_code == 1


def test_reconciliation_fails_when_imported_archive_has_no_import_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "missing-import-time.sqlite3"
    with SQLiteStore(db_path) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL,
            exchange_id="binance",
            market_type="spot",
            data_type="klines_1m",
            symbol="BTCUSDT",
            archive_period="2024-01",
            status="discovered",
            checksum_sha256="d" * 64,
            size_bytes=1234,
            row_count=10,
        )
        # Deliberately corrupt persisted state to test fail-closed reconciliation.
        store._connection.execute(
            "UPDATE archive_manifest SET status='imported' WHERE archive_url=?",
            (ARCHIVE_URL,),
        )
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["missing_import_timestamp"] == [ARCHIVE_URL]
    assert exit_code == 1


def test_reconciliation_rejects_zero_byte_archive_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "zero-size.sqlite3"
    with SQLiteStore(db_path) as store:
        store.upsert_archive_manifest(
            archive_url=ARCHIVE_URL,
            exchange_id="binance",
            market_type="spot",
            data_type="klines_1m",
            symbol="BTCUSDT",
            archive_period="2024-01",
            status="discovered",
            checksum_sha256="e" * 64,
            size_bytes=0,
            row_count=10,
            imported_at="2026-10-24T00:00:00+00:00",
        )
        # Deliberately corrupt persisted state to test fail-closed reconciliation.
        store._connection.execute(
            "UPDATE archive_manifest SET status='imported' WHERE archive_url=?",
            (ARCHIVE_URL,),
        )
    monkeypatch.setattr(cli, "BinanceArchiveAdapter", FakeArchiveAdapter)

    exit_code = cli.main(_args(db_path))

    report = json.loads(capsys.readouterr().out)
    assert report["missing_checksum_or_size"] == [ARCHIVE_URL]
    assert exit_code == 1



def test_archive_period_preserves_daily_date_while_month_helper_groups_it() -> None:
    daily_url = (
        "https://data.binance.vision/data/spot/daily/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-01-31.zip"
    )
    assert cli._archive_period(daily_url) == "2024-01-31"
    assert cli._archive_month(daily_url) == "2024-01"


def test_archive_manifest_command_passes_when_imported_provenance_matches(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "manifest-audit-pass.sqlite3"
    _register_imported_archive(db_path)

    exit_code = cli.main([
        "--db", str(db_path), "archive-manifest",
        "--market-type", "spot", "--data-type", "klines_1m", "--symbol", "BTCUSDT",
    ])

    report = json.loads(capsys.readouterr().out)
    assert report["audit_passed"] is True
    assert report["imported_provenance_audit_failures"] == []
    assert report["archives"][0]["provenance_audit"]["passed"] is True
    assert exit_code == 0


def test_archive_manifest_command_fails_when_imported_provenance_is_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "manifest-audit-fail.sqlite3"
    _register_imported_archive(db_path)
    with SQLiteStore(db_path) as store:
        store._connection.execute("DELETE FROM records")
        store._connection.commit()

    exit_code = cli.main([
        "--db", str(db_path), "archive-manifest",
        "--market-type", "spot", "--data-type", "klines_1m", "--symbol", "BTCUSDT",
    ])

    report = json.loads(capsys.readouterr().out)
    assert report["audit_passed"] is False
    assert report["imported_provenance_audit_failures"][0]["stored_record_rows"] == 0
    assert exit_code == 1
