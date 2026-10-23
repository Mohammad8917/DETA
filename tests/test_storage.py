from pathlib import Path

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
        try:
            store.save_batch(records, cursor="3")
        except ValueError:
            pass
        else:
            raise AssertionError("mixed dataset identities must be rejected")
