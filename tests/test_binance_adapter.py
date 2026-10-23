from typing import Any

import httpx

from deta.exchanges.binance import BinanceSpotAdapter


class MockClient:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows
        self.calls = 0

    def get(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        self.calls += 1
        request = httpx.Request("GET", url)
        return httpx.Response(200, json=self.rows if self.calls == 1 else [], request=request)

    def close(self) -> None:
        pass


def test_kline_normalization_preserves_native_fields() -> None:
    row = [1000, "1", "3", "0.5", "2", "10", 1999, "20", 4, "6", "12", "0"]
    client = MockClient([row])
    adapter = BinanceSpotAdapter(client=client, request_pause=0)
    records = adapter.fetch_klines("ABCUSDT", "1m", 0)
    assert len(records) == 1
    assert records[0].timestamp_ms == 1000
    assert records[0].payload["close"] == "2"
    assert records[0].payload["trade_count"] == 4
    adapter.close()
