import pytest

from deta.domain import DataRecord
from deta.transforms import trades_to_klines


def trade(timestamp: int, key: str, price: str, qty: str) -> DataRecord:
    return DataRecord(
        "binance", "spot", "trades", "BTCUSDT", timestamp, key,
        {"price": price, "qty": qty},
    )


def test_raw_trades_reconstruct_ohlcv_and_quote_volume() -> None:
    records = [
        trade(61_000, "2", "12", "2"),
        trade(1_000, "1", "10", "3"),
        trade(59_999, "3", "8", "1"),
    ]
    result = trades_to_klines(records, interval_ms=60_000)
    assert len(result) == 2
    first = result[0]
    assert first.timestamp_ms == 0
    assert first.payload["open"] == "10"
    assert first.payload["high"] == "10"
    assert first.payload["low"] == "10"
    assert first.payload["close"] == "10"
    assert first.payload["volume"] == "3"
    assert first.payload["quote_volume"] == "30"
    assert first.payload["trade_count"] == 1
    second = result[1]
    assert second.timestamp_ms == 60_000
    assert second.payload["open"] == "8"
    assert second.payload["high"] == "12"
    assert second.payload["low"] == "8"
    assert second.payload["close"] == "12"
    assert second.payload["volume"] == "3"
    assert second.payload["quote_volume"] == "32"
    assert second.payload["trade_count"] == 2
    assert second.payload["derived"] is True


def test_trade_reconstruction_rejects_non_trade_dataset() -> None:
    candle = DataRecord("binance", "spot", "klines_1m", "BTCUSDT", 1, "1", {
        "price": "10", "qty": "1",
    })
    with pytest.raises(ValueError, match="Unsupported source data_type"):
        trades_to_klines([candle], interval_ms=60_000)


def test_trade_reconstruction_rejects_mixed_symbols() -> None:
    records = [
        trade(1, "1", "10", "1"),
        DataRecord("binance", "spot", "trades", "ETHUSDT", 2, "2", {
            "price": "20", "qty": "1",
        }),
    ]
    with pytest.raises(ValueError, match="one exchange, market, and symbol"):
        trades_to_klines(records, interval_ms=60_000)


@pytest.mark.parametrize("price,qty", [("0", "1"), ("NaN", "1"), ("10", "-1"), ("10", "Infinity")])
def test_trade_reconstruction_rejects_invalid_numeric_values(price: str, qty: str) -> None:
    with pytest.raises(ValueError):
        trades_to_klines([trade(1, "1", price, qty)], interval_ms=60_000)


def test_trade_reconstruction_requires_positive_interval() -> None:
    with pytest.raises(ValueError, match="interval_ms must be positive"):
        trades_to_klines([trade(1, "1", "10", "1")], interval_ms=0)
