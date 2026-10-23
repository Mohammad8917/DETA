"""Binance public Spot REST adapter; no credentials or trading endpoints are used."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import httpx

from deta.domain import DataRecord


class BinanceSpotAdapter:
    exchange_id = "binance"
    market_type = "spot"
    base_url = "https://api.binance.com"
    candle_intervals = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M")

    def __init__(self, *, timeout: float = 20.0, request_pause: float = 0.15,
                 client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=timeout, headers={"User-Agent": "DETA/0.1"})
        self._owns_client = client is None
        self.request_pause = max(0.0, request_pause)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                response = self._client.get(f"{self.base_url}{path}", params=params)
                if response.status_code in (418, 429):
                    time.sleep(float(response.headers.get("Retry-After", min(60, 2 ** attempt))))
                    continue
                if response.status_code >= 500:
                    time.sleep(min(30.0, 2 ** attempt))
                    continue
                response.raise_for_status()
                if self.request_pause:
                    time.sleep(self.request_pause)
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == 5:
                    break
                time.sleep(min(30.0, 2 ** attempt))
        raise RuntimeError(f"Binance request failed for {path}: {last_error}") from last_error

    def discover_symbols(self) -> list[dict[str, Any]]:
        payload = self._get("/api/v3/exchangeInfo")
        symbols: list[dict[str, Any]] = []
        for item in payload.get("symbols", []):
            if not isinstance(item, dict) or not item.get("symbol"):
                continue
            symbols.append({
                "exchange_id": self.exchange_id, "market_type": self.market_type,
                "symbol": str(item["symbol"]), "base_asset": item.get("baseAsset"),
                "quote_asset": item.get("quoteAsset"), "status": item.get("status"),
                "is_spot_trading_allowed": item.get("isSpotTradingAllowed"),
                "raw_metadata": item,
            })
        return symbols

    def iter_klines(self, symbol: str, interval: str, start_ms: int,
                    end_ms: int | None = None) -> Iterator[list[DataRecord]]:
        """Yield one API page at a time so historical backfills have bounded memory use."""
        if interval not in self.candle_intervals:
            raise ValueError(f"Unsupported Binance kline interval: {interval}")
        if start_ms < 0 or (end_ms is not None and end_ms < start_ms):
            raise ValueError("Invalid kline time range")
        cursor = start_ms
        while True:
            params: dict[str, Any] = {"symbol": symbol, "interval": interval,
                                      "startTime": cursor, "limit": 1000}
            if end_ms is not None:
                params["endTime"] = end_ms
            rows = self._get("/api/v3/klines", params)
            if not rows:
                return
            records: list[DataRecord] = []
            for row in rows:
                open_time = int(row[0])
                if end_ms is not None and open_time > end_ms:
                    continue
                payload = {"interval": interval, "open_time_ms": open_time,
                    "open": row[1], "high": row[2], "low": row[3], "close": row[4],
                    "volume": row[5], "close_time_ms": int(row[6]), "quote_volume": row[7],
                    "trade_count": int(row[8]), "taker_buy_base_volume": row[9],
                    "taker_buy_quote_volume": row[10], "source": "binance_rest_native"}
                records.append(DataRecord(self.exchange_id, self.market_type,
                    f"klines_{interval}", symbol, open_time, str(open_time), payload))
            if records:
                yield records
            next_cursor = int(rows[-1][0]) + 1
            if (end_ms is not None and next_cursor > end_ms) or next_cursor <= cursor or len(rows) < 1000:
                return
            cursor = next_cursor

    def iter_aggregate_trades(
        self, symbol: str, start_ms: int = 0, end_ms: int | None = None,
        start_id: int | None = None,
    ) -> Iterator[list[DataRecord]]:
        """Stream aggregate trades page-by-page, resuming by aggregate trade ID when available."""
        if start_ms < 0 or (end_ms is not None and end_ms < start_ms):
            raise ValueError("Invalid aggregate-trade time range")
        if start_id is not None and start_id < 0:
            raise ValueError("Aggregate trade ID must be non-negative")
        cursor_id = start_id
        while True:
            params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
            if cursor_id is None:
                params["startTime"] = start_ms
            else:
                params["fromId"] = cursor_id
            rows = self._get("/api/v3/aggTrades", params)
            if not rows:
                return
            records: list[DataRecord] = []
            stop_after_page = False
            for row in rows:
                trade_id = int(row["a"])
                timestamp = int(row["T"])
                if end_ms is not None and timestamp > end_ms:
                    stop_after_page = True
                    break
                if timestamp < start_ms:
                    continue
                payload = {
                    "aggregate_trade_id": trade_id, "price": row["p"],
                    "quantity": row["q"], "first_trade_id": int(row["f"]),
                    "last_trade_id": int(row["l"]), "timestamp_ms": timestamp,
                    "is_buyer_maker": bool(row["m"]),
                    "is_best_match": bool(row.get("M", False)),
                    "source": "binance_rest_native",
                }
                records.append(DataRecord(self.exchange_id, self.market_type, "agg_trades",
                    symbol, timestamp, str(trade_id), payload))
            if records:
                yield records
            if stop_after_page or len(rows) < 1000:
                return
            next_cursor = int(rows[-1]["a"]) + 1
            if cursor_id is not None and next_cursor <= cursor_id:
                raise RuntimeError(f"Non-advancing aggregate trade cursor for {symbol}")
            cursor_id = next_cursor

    def fetch_aggregate_trades(self, symbol: str, start_ms: int = 0,
                               end_ms: int | None = None) -> list[DataRecord]:
        """Convenience method for small tests; production backfills should use the iterator."""
        return [record for page in self.iter_aggregate_trades(symbol, start_ms, end_ms)
                for record in page]

    def fetch_24h_tickers(self) -> list[dict[str, Any]]:
        """Return a point-in-time public ticker snapshot; this is not historical data."""
        payload = self._get("/api/v3/ticker/24hr")
        return payload if isinstance(payload, list) else []
