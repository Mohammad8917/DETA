"""Deterministic reconstruction of OHLCV candles from stored raw trade records.

This transform only reconstructs native trade-derived OHLCV. It must not be used
for mark/index/premium prices or funding rates, which require their own sources.
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from deta.domain import DataRecord


def trades_to_klines(
    trades: Iterable[DataRecord],
    *,
    interval_ms: int,
    data_type: str = "klines_derived",
) -> list[DataRecord]:
    """Aggregate individual trade records into UTC-aligned OHLCV candles.

    Trade payloads must provide price and quantity (qty or quantity). Timestamp
    is taken from the DataRecord. Invalid/non-positive prices and negative
    quantities are rejected rather than silently corrupting the output.
    """
    if interval_ms <= 0:
        raise ValueError("interval_ms must be positive")

    groups: dict[tuple[str, str, str, str, int], list[tuple[int, str, Decimal, Decimal]]] = defaultdict(list)
    dataset_identity: tuple[str, str, str] | None = None

    for trade in trades:
        identity = (trade.exchange_id, trade.market_type, trade.symbol)
        if dataset_identity is None:
            dataset_identity = identity
        elif identity != dataset_identity:
            raise ValueError("All trades must belong to one exchange, market, and symbol")
        if trade.data_type not in {"trades", "individual_trades"}:
            raise ValueError(f"Unsupported source data_type for trade reconstruction: {trade.data_type}")
        payload: dict[str, Any] = trade.payload
        try:
            price = Decimal(str(payload["price"]))
            quantity_value = payload.get("qty", payload.get("quantity"))
            if quantity_value is None:
                raise KeyError("qty/quantity")
            quantity = Decimal(str(quantity_value))
        except (KeyError, InvalidOperation, ValueError) as exc:
            raise ValueError(f"Trade {trade.record_key} has invalid price/quantity fields") from exc
        if not price.is_finite() or price <= 0:
            raise ValueError(f"Trade {trade.record_key} has non-positive or non-finite price")
        if not quantity.is_finite() or quantity < 0:
            raise ValueError(f"Trade {trade.record_key} has negative or non-finite quantity")
        bucket = (trade.timestamp_ms // interval_ms) * interval_ms
        groups[(trade.exchange_id, trade.market_type, trade.symbol, data_type, bucket)].append(
            (trade.timestamp_ms, trade.record_key, price, quantity)
        )

    output: list[DataRecord] = []
    for (exchange_id, market_type, symbol, result_type, bucket), rows in sorted(groups.items()):
        rows.sort(key=lambda row: (row[0], row[1]))
        prices = [row[2] for row in rows]
        volume = sum((row[3] for row in rows), Decimal("0"))
        quote_volume = sum((row[2] * row[3] for row in rows), Decimal("0"))
        payload = {
            "open": str(prices[0]),
            "high": str(max(prices)),
            "low": str(min(prices)),
            "close": str(prices[-1]),
            "volume": str(volume),
            "quote_volume": str(quote_volume),
            "trade_count": len(rows),
            "source": "individual_trades",
            "interval_ms": interval_ms,
            "derived": True,
        }
        output.append(DataRecord(
            exchange_id, market_type, result_type, symbol, bucket, str(bucket), payload
        ))
    return output
