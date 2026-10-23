from typing import Any

import httpx
import pytest

from deta.exchanges.binance.derivatives import BinanceDerivativesAdapter


class MockClient:
    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def get(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        return httpx.Response(200, json=self.payload, request=httpx.Request("GET", url))

    def close(self) -> None:
        pass


@pytest.mark.parametrize(
    ("market", "base_url", "prefix", "market_type"),
    [
        ("usd-m", "https://fapi.binance.com", "/fapi/v1", "binance_usd_m_futures"),
        ("coin-m", "https://dapi.binance.com", "/dapi/v1", "binance_coin_m_futures"),
    ],
)
def test_market_endpoints_and_contract_discovery(
    market: str, base_url: str, prefix: str, market_type: str
) -> None:
    adapter = BinanceDerivativesAdapter(market, client=MockClient({
        "symbols": [{
            "symbol": "BTCUSDT", "pair": "BTCUSDT", "baseAsset": "BTC",
            "quoteAsset": "USDT", "marginAsset": "USDT", "status": "TRADING",
            "contractType": "PERPETUAL", "deliveryDate": 0, "onboardDate": 1,
        }]
    }), request_pause=0)
    assert adapter.base_url == base_url
    assert adapter.api_prefix == prefix
    symbols = adapter.discover_symbols()
    assert symbols[0]["market_type"] == market_type
    assert symbols[0]["contract_type"] == "PERPETUAL"
    adapter.close()


def test_futures_kline_records_keep_market_identity() -> None:
    row = [1000, "1", "3", "0.5", "2", "10", 1999, "20", 4, "6", "12", "0"]
    adapter = BinanceDerivativesAdapter("usd-m", client=MockClient([row]), request_pause=0)
    page = next(adapter.iter_klines("BTCUSDT", "1m", 0))
    assert page[0].market_type == "binance_usd_m_futures"
    assert page[0].data_type == "klines_1m"
    assert page[0].timestamp_ms == 1000
    adapter.close()


def test_funding_rate_records_preserve_market_identity_and_source_values() -> None:
    adapter = BinanceDerivativesAdapter(
        "coin-m",
        client=MockClient([{
            "symbol": "BTCUSD_PERP",
            "fundingTime": 1700000000000,
            "fundingRate": "0.0001",
            "markPrice": "65000.5",
        }]),
        request_pause=0,
    )
    pages = list(adapter.iter_funding_rates("BTCUSD_PERP", start_ms=0))
    assert len(pages) == 1
    record = pages[0][0]
    assert record.market_type == "binance_coin_m_futures"
    assert record.data_type == "funding_rate"
    assert record.timestamp_ms == 1700000000000
    assert record.payload["funding_rate"] == "0.0001"
    assert record.payload["mark_price"] == "65000.5"
    adapter.close()


def test_funding_rate_rejects_invalid_time_range() -> None:
    adapter = BinanceDerivativesAdapter("usd-m", client=MockClient([]), request_pause=0)
    with pytest.raises(ValueError, match="Invalid funding-rate time range"):
        list(adapter.iter_funding_rates("BTCUSDT", start_ms=10, end_ms=9))
    adapter.close()
