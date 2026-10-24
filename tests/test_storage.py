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
            url, status="verified", checksum_sha256=first_digest, size_bytes=100, row_count=1,
        )
        record = DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 1_704_067_200_000, "1704067200000", {"close": "1"})
        store.save_archive_batch(url, [record], cursor="1704067200001")
        store.update_archive_manifest(
            url, status="imported", imported_at="2026-10-24T00:00:00Z", row_count=1,
        )

        # A rediscovery or re-verification of identical bytes must not erase import state.
        store.update_archive_manifest(url, status="verified", checksum_sha256=first_digest)
        row = store.archive_manifest_summary()[0]
        assert row["status"] == "imported"
        assert row["imported_at"] == "2026-10-24T00:00:00Z"
        # Re-importing the same verified bytes is idempotent and keeps provenance valid.
        assert store.save_archive_batch(url, [record], cursor="1704067200001") == 0
        assert store.archive_provenance_summary(url)["provenance_rows"] == 1

        # A different checksum represents different content; the old import timestamp
        # must be invalidated and the new content must pass verification/import again.
        store.update_archive_manifest(
            url, status="downloaded", checksum_sha256=second_digest, size_bytes=101,
        )
        row = store.archive_manifest_summary()[0]
        assert row["status"] == "downloaded"
        assert row["checksum_sha256"] == second_digest
        assert row["imported_at"] is None
        assert store.archive_provenance_summary(url)["provenance_rows"] == 0
        summary = next(item for item in store.dataset_summary() if item["symbol"] == "BTCUSDT")
        assert summary["rows"] == 0

        # The new content version can now be imported without conflicting with stale rows.
        store.update_archive_manifest(
            url, status="verified", checksum_sha256=second_digest, size_bytes=101, row_count=1,
        )
        replacement = DataRecord(
            "binance", "spot", "klines_1m", "BTCUSDT",
            1_704_067_200_000, "1704067200000", {"close": "2"},
        )
        assert store.save_archive_batch(url, [replacement], cursor="1704067200001") == 1
        assert store.archive_manifest_summary()[0]["status"] == "imported"
        assert store.archive_provenance_summary(url)["payload_hash_mismatches"] == 0

        with pytest.raises(ValueError, match="must be verified"):
            store.update_archive_manifest(url, status="imported", checksum_sha256="c" * 64)

        # Re-importing the newly verified archive version must replace stale row payload,
        # restore provenance, and atomically transition the manifest to imported.
        store.update_archive_manifest(
            url, status="verified", checksum_sha256=second_digest, size_bytes=101, row_count=1,
        )
        revised = DataRecord(
            "binance", "spot", "klines_1m", "BTCUSDT",
            1_704_067_200_000, "1704067200000", {"close": "2"},
        )
        assert store.save_archive_batch(url, [revised], cursor="1704067200001") == 0
        summary = store.archive_provenance_summary(url)
        assert summary == {
            "provenance_rows": 1, "stored_record_rows": 1,
            "timestamp_mismatches": 0, "payload_hash_mismatches": 0,
        }
        manifest = store.archive_manifest_summary()[0]
        assert manifest["status"] == "imported"
        assert manifest["checksum_sha256"] == second_digest
        assert manifest["imported_at"]
        stored = store._connection.execute(
            """SELECT payload_json FROM records
               WHERE exchange_id='binance' AND market_type='spot'
                 AND data_type='klines_1m' AND symbol='BTCUSDT'
                 AND record_key='1704067200000'"""
        ).fetchone()
        assert json.loads(stored[0]) == {"close": "2"}


def test_record_timestamps_are_bounded_distinct_and_sorted(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "data.sqlite3") as store:
        records = [
            DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 120_000, "b", {}),
            DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 60_000, "a", {}),
            DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 180_000, "c", {}),
            DataRecord("binance", "spot", "klines_1m", "ETHUSDT", 120_000, "other", {}),
        ]
        store.save_batch(records[:3], cursor="180000")
        store.save_batch([records[3]], cursor="120000")
        assert store.record_timestamps(
            exchange_id="binance", market_type="spot", data_type="klines_1m",
            symbol="BTCUSDT", start_ms=60_000, end_ms=120_000,
        ) == [60_000, 120_000]
        with pytest.raises(ValueError, match="Invalid timestamp range"):
            store.record_timestamps(
                exchange_id="binance", market_type="spot", data_type="klines_1m",
                symbol="BTCUSDT", start_ms=120_000, end_ms=60_000,
            )

def test_checkpoint_resume_after_interruption_uses_last_committed_batch(tmp_path: Path) -> None:
    """A restart resumes after the last batch committed with its checkpoint."""
    path = tmp_path / "resume.sqlite3"
    first_page = [
        DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 1_704_067_200_000, "1704067200000", {"close": "1"}),
        DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 120_000, "120000", {"close": "2"}),
    ]
    with SQLiteStore(path) as store:
        assert store.save_batch(first_page, cursor="120001", status="partial") == 2

    # Simulate process termination: reopen the same durable database.
    with SQLiteStore(path) as reopened:
        assert reopened.get_checkpoint("binance", "spot", "klines_1m", "BTCUSDT") == "120001"
        # Replaying the last record is safe: the primary key prevents duplicates,
        # and the next checkpoint is committed atomically with the replayed batch.
        replay = [first_page[-1], DataRecord(
            "binance", "spot", "klines_1m", "BTCUSDT", 180_000, "180000", {"close": "3"}
        )]
        assert reopened.save_batch(replay, cursor="180001", status="partial") == 1
        assert reopened.get_checkpoint("binance", "spot", "klines_1m", "BTCUSDT") == "180001"
        summary = next(
            item for item in reopened.dataset_summary()
            if item["symbol"] == "BTCUSDT" and item["data_type"] == "klines_1m"
        )
        assert summary["rows"] == 3


def test_archive_provenance_detects_missing_records_and_payload_mutation(tmp_path: Path) -> None:
    path = tmp_path / "provenance.sqlite3"
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    record = DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 1_704_067_200_000, "1704067200000", {"close": "1"})
    with SQLiteStore(path) as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        assert store.save_archive_batch(url, [record], cursor="1704067200001") == 1
        assert store.archive_provenance_summary(url) == {
            "provenance_rows": 1, "stored_record_rows": 1,
            "timestamp_mismatches": 0, "payload_hash_mismatches": 0,
        }
        store._connection.execute(
            """UPDATE records SET payload_json=?
               WHERE exchange_id='binance' AND market_type='spot'
                 AND data_type='klines_1m' AND symbol='BTCUSDT' AND record_key='1704067200000'""",
            ('{"close":"2"}',),
        )
        store._connection.commit()
        assert store.archive_provenance_summary(url)["payload_hash_mismatches"] == 1
        store._connection.execute(
            """UPDATE records SET payload_json=?, timestamp_ms=1704067260000
               WHERE exchange_id='binance' AND market_type='spot'
                 AND data_type='klines_1m' AND symbol='BTCUSDT' AND record_key='1704067200000'""",
            ('{"close":"1"}',),
        )
        store._connection.commit()
        assert store.archive_provenance_summary(url)["timestamp_mismatches"] == 1
        store._connection.execute("DELETE FROM records")
        store._connection.commit()
        summary = store.archive_provenance_summary(url)
        assert summary["provenance_rows"] == 1
        assert summary["stored_record_rows"] == 0


def test_manifest_cannot_be_marked_imported_without_matching_provenance(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    with SQLiteStore(tmp_path / "unproven-import.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        with pytest.raises(ValueError, match="provenance"):
            store.update_archive_manifest(url, status="imported")
        assert store.archive_manifest_summary()[0]["status"] == "verified"


def test_archive_batch_rejects_verified_manifest_row_count_mismatch(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    record = DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 1_704_067_200_000, "1704067200000", {"close": "1"})
    with SQLiteStore(tmp_path / "row-count.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=2,
        )
        with pytest.raises(ValueError, match="does not match verified manifest row_count"):
            store.save_archive_batch(url, [record], cursor="60001")
        assert store.archive_provenance_summary(url)["provenance_rows"] == 0
        assert store.dataset_summary() == []


def test_archive_batch_rejects_timestamps_outside_declared_period(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-31.zip"
    record = DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 60_000, "60000", {"close": "1"})
    with SQLiteStore(tmp_path / "period.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01-31",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        with pytest.raises(ValueError, match="outside its declared UTC period"):
            store.save_archive_batch(url, [record], cursor="60001")
        assert store.archive_provenance_summary(url)["provenance_rows"] == 0
        assert store.dataset_summary() == []


def test_archive_manifest_upsert_checksum_change_invalidates_provenance(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    record = DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 1_704_067_200_000, "1704067200000", {"close": "1"})
    with SQLiteStore(tmp_path / "upsert-checksum.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        store.save_archive_batch(url, [record], cursor="1704067200001")
        store.update_archive_manifest(
            url, status="imported", imported_at="2026-10-24T00:00:00Z",
        )
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="verified", checksum_sha256="b" * 64, size_bytes=101, row_count=1,
        )
        manifest = store.archive_manifest_summary()[0]
        assert manifest["status"] == "verified"
        assert manifest["imported_at"] is None
        assert manifest["checksum_sha256"] == "b" * 64
        assert store.archive_provenance_summary(url)["provenance_rows"] == 0


def test_archive_import_atomically_marks_manifest_imported(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    record = DataRecord(
        "binance", "spot", "klines_1m", "BTCUSDT",
        1_704_067_200_000, "1704067200000", {"close": "1"},
    )
    with SQLiteStore(tmp_path / "atomic-import.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        assert store.save_archive_batch(url, [record], cursor="1704067200001") == 1
        manifest = store.archive_manifest_summary()[0]
        assert manifest["status"] == "imported"
        assert isinstance(manifest["imported_at"], str) and manifest["imported_at"]
        assert manifest["row_count"] == 1
        assert store.archive_provenance_summary(url) == {
            "provenance_rows": 1, "stored_record_rows": 1,
            "timestamp_mismatches": 0, "payload_hash_mismatches": 0,
        }
        assert store.get_checkpoint("binance", "spot", "klines_1m", "BTCUSDT") == "1704067200001"


def test_archive_import_rejects_duplicate_record_keys_before_writing(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    record = DataRecord(
        "binance", "spot", "klines_1m", "BTCUSDT",
        1_704_067_200_000, "1704067200000", {"close": "1"},
    )
    with SQLiteStore(tmp_path / "duplicate-records.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=2,
        )
        with pytest.raises(ValueError, match="duplicate record keys"):
            store.save_archive_batch(url, [record, record], cursor="1704067200001")
        assert store.archive_provenance_summary(url)["provenance_rows"] == 0
        assert store.archive_manifest_summary()[0]["status"] == "verified"
        assert store.dataset_summary() == []


def test_archive_import_rolls_back_when_existing_record_conflicts(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    existing = DataRecord(
        "binance", "spot", "klines_1m", "BTCUSDT",
        1_704_067_200_000, "1704067200000", {"close": "0.9"},
    )
    archived = DataRecord(
        "binance", "spot", "klines_1m", "BTCUSDT",
        1_704_067_200_000, "1704067200000", {"close": "1.0"},
    )
    with SQLiteStore(tmp_path / "conflicting-record.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
        )
        store.update_archive_manifest(
            url, status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        store.save_batch([existing], cursor="1704067200001")
        with pytest.raises(RuntimeError, match="conflicts with an existing stored record"):
            store.save_archive_batch(url, [archived], cursor="1704067200001")
        manifest = next(item for item in store.archive_manifest_summary() if item["archive_url"] == url)
        assert manifest["status"] == "verified"
        assert manifest["imported_at"] is None
        assert store.archive_provenance_summary(url)["provenance_rows"] == 0
        stored = store._connection.execute(
            """SELECT payload_json FROM records
               WHERE exchange_id='binance' AND market_type='spot'
                 AND data_type='klines_1m' AND symbol='BTCUSDT'
                 AND record_key='1704067200000'"""
        ).fetchone()
        assert stored[0] == '{"close": "0.9"}'


def test_archive_manifest_upsert_cannot_change_url_dataset_identity(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    with SQLiteStore(tmp_path / "immutable-identity.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        with pytest.raises(ValueError, match="identity is immutable"):
            store.upsert_archive_manifest(
                archive_url=url, exchange_id="binance", market_type="spot",
                data_type="klines_1m", symbol="ETHUSDT", archive_period="2024-01",
                status="discovered",
            )
        manifest = store.archive_manifest_summary()[0]
        assert manifest["symbol"] == "BTCUSDT"
        assert manifest["checksum_sha256"] == "a" * 64


def test_archive_manifest_upsert_does_not_downgrade_imported_state(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    record = DataRecord(
        "binance", "spot", "klines_1m", "BTCUSDT",
        1_704_067_200_000, "1704067200000", {"close": "1"},
    )
    with SQLiteStore(tmp_path / "monotonic-status.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        store.save_archive_batch(url, [record], cursor="1704067200001")
        before = store.archive_manifest_summary()[0]
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="verified", checksum_sha256="a" * 64, size_bytes=100, row_count=1,
        )
        after = store.archive_manifest_summary()[0]
        assert before["status"] == after["status"] == "imported"
        assert before["imported_at"] == after["imported_at"]
        assert store.archive_provenance_summary(url)["payload_hash_mismatches"] == 0


def test_archive_manifest_upsert_migrates_legacy_daily_period_to_exact_date(tmp_path: Path) -> None:
    url = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-31.zip"
    with SQLiteStore(tmp_path / "legacy-daily-period.sqlite3") as store:
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01",
            status="discovered",
        )
        store.upsert_archive_manifest(
            archive_url=url, exchange_id="binance", market_type="spot",
            data_type="klines_1m", symbol="BTCUSDT", archive_period="2024-01-31",
            status="discovered",
        )
        assert store.archive_manifest_summary()[0]["archive_period"] == "2024-01-31"
