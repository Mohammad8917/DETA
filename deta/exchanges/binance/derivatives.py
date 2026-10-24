"""Public Binance USD-M and COIN-M futures market-data adapter.

This module only reads public market data. It does not place, cancel, or manage orders.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import httpx

from deta.domain import DataRecord


class BinanceDerivativesAdapter:
    """Read public klines and contract metadata for one Binance futures market."""

    candle_intervals = (
        "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h",
        "12h", "1d", "3d", "1w", "1M",
    )
    _markets = {
        "usd-m": ("binance_usd_m_futures", "https://fapi.binance.com", "/fapi/v1"),
        "coin-m": ("binance_coin_m_futures", "https://dapi.binance.com", "/dapi/v1"),
    }

    def __init__(self, market: str, *, timeout: float = 20.0,
                 request_pause: float = 0.15, client: httpx.Client | None = None) -> None:
        if market not in self._markets:
            raise ValueError("market must be 'usd-m' or 'coin-m'")
        self.market = market
        self.exchange_id, self.base_url, self.api_prefix = self._markets[market]
        self.market_type = self.exchange_id
        self._client = client or httpx.Client(
            timeout=timeout, headers={"User-Agent": "DETA/0.1"}
        )
        self._owns_client = client is None
        self.request_pause = max(0.0, request_pause)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                response = self._client.get(
                    f"{self.base_url}{self.api_prefix}{endpoint}", params=params
                )
                if response.status_code in (418, 429):
                    time.sleep(float(response.headers.get("Retry-After", min(60, 2**attempt))))
                    continue
                if response.status_code >= 500:
                    time.sleep(min(30.0, 2**attempt))
                    continue
                response.raise_for_status()
                if self.request_pause:
                    time.sleep(self.request_pause)
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == 5:
                    break
                time.sleep(min(30.0, 2**attempt))
        raise RuntimeError(
            f"Binance {self.market} request failed for {endpoint}: {last_error}"
        ) from last_error

    def discover_symbols(self) -> list[dict[str, Any]]:
        """Discover all contracts returned by the exchange, including non-perpetual types."""
        payload = self._get("/exchangeInfo")
        result: list[dict[str, Any]] = []
        for item in payload.get("symbols", []):
            if not isinstance(item, dict) or not item.get("symbol"):
                continue
            result.append({
                "exchange_id": "binance",
                "market_type": self.market_type,
                "symbol": str(item["symbol"]),
                "pair": item.get("pair"),
                "base_asset": item.get("baseAsset"),
                "quote_asset": item.get("quoteAsset"),
                "margin_asset": item.get("marginAsset"),
                "status": item.get("status"),
                "contract_type": item.get("contractType"),
                "delivery_date_ms": item.get("deliveryDate"),
                "onboard_date_ms": item.get("onboardDate"),
                "raw_metadata": item,
            })
        return result

    def iter_klines(
        self, symbol: str, interval: str, start_ms: int = 0, end_ms: int | None = None
    ) -> Iterator[list[DataRecord]]:
        """Stream native futures candles, retaining the contract type in every record."""
        if interval not in self.candle_intervals:
            raise ValueError(f"Unsupported Binance futures kline interval: {interval}")
        if start_ms < 0 or (end_ms is not None and end_ms < start_ms):
            raise ValueError("Invalid kline time range")
        cursor = start_ms
        # Freeze the cutoff for this iterator so a candle cannot become "closed" mid-page.
        closed_before_ms = int(time.time() * 1000)
        while True:
            params: dict[str, Any] = {
                "symbol": symbol, "interval": interval, "startTime": cursor, "limit": 1500
            }
            if end_ms is not None:
                params["endTime"] = end_ms
            rows = self._get("/klines", params)
            if not rows:
                return
            records: list[DataRecord] = []
            for row in rows:
                open_time = int(row[0])
                close_time = int(row[6])
                if end_ms is not None and open_time > end_ms:
                    continue
                # Do not persist the currently forming candle as final historical data.
                if close_time >= closed_before_ms:
                    continue
                payload = {
                    "interval": interval, "open_time_ms": open_time,
                    "open": row[1], "high": row[2], "low": row[3], "close": row[4],
                    "volume": row[5], "close_time_ms": int(row[6]),
                    "quote_volume_or_contract_volume": row[7], "trade_count": int(row[8]),
                    "taker_buy_volume": row[9], "taker_buy_quote_volume": row[10],
                    "source": "binance_futures_rest_native", "market": self.market,
                }
                records.append(DataRecord(
                    "binance", self.market_type, f"klines_{interval}", symbol,
                    open_time, str(open_time), payload,
                ))
            if records:
                yield records
            next_cursor = int(rows[-1][0]) + 1
            if next_cursor <= cursor or len(rows) < 1500:
                return
            if end_ms is not None and next_cursor > end_ms:
                return
            cursor = next_cursor

    def iter_funding_rates(
        self, symbol: str, start_ms: int = 0, end_ms: int | None = None
    ) -> Iterator[list[DataRecord]]:
        """Stream public historical funding-rate records in funding-time order."""
        if not symbol:
            raise ValueError("symbol must not be empty")
        if start_ms < 0 or (end_ms is not None and end_ms < start_ms):
            raise ValueError("Invalid funding-rate time range")
        cursor = start_ms
        while True:
            params: dict[str, Any] = {
                "symbol": symbol, "startTime": cursor, "limit": 1000
            }
            if end_ms is not None:
                params["endTime"] = end_ms
            rows = self._get("/fundingRate", params)
            if not rows:
                return
            records: list[DataRecord] = []
            timestamps: list[int] = []
            for row in rows:
                funding_time = int(row["fundingTime"])
                if end_ms is not None and funding_time > end_ms:
                    continue
                if funding_time < cursor:
                    continue
                payload = {
                    "funding_time_ms": funding_time,
                    "funding_rate": row["fundingRate"],
                    "mark_price": row.get("markPrice"),
                    "source": "binance_futures_rest_native",
                    "market": self.market,
                }
                records.append(DataRecord(
                    "binance", self.market_type, "funding_rate", symbol,
                    funding_time, str(funding_time), payload,
                ))
                timestamps.append(funding_time)
            if records:
                records.sort(key=lambda record: record.timestamp_ms)
                yield records
            next_cursor = max(timestamps, default=cursor - 1) + 1
            if next_cursor <= cursor or len(rows) < 1000:
                return
            if end_ms is not None and next_cursor > end_ms:
                return
            cursor = next_cursor
