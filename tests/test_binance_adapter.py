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



def test_spot_kline_iterator_excludes_unclosed_candle() -> None:
    row = [9999999990000, "1", "3", "0.5", "2", "10", 9999999999999, "20", 4, "6", "12", "0"]
    adapter = BinanceSpotAdapter(client=MockClient([row]), request_pause=0)
    assert list(adapter.iter_klines("ABCUSDT", "1m", 0)) == []
    adapter.close()


def test_aggregate_trades_uses_time_then_id_cursor_and_streams_pages() -> None:
    adapter = BinanceSpotAdapter(client=MockClient([]), request_pause=0)
    calls: list[dict[str, Any]] = []
    first_page = [
        {"a": index, "p": "2", "q": "1", "f": index, "l": index,
         "T": index * 100, "m": False, "M": True}
        for index in range(1000, 2000)
    ]
    second_page = [
        {"a": 2000, "p": "3", "q": "2", "f": 2000, "l": 2000,
         "T": 200000, "m": True, "M": True}
    ]

    def fake_get(path: str, params: dict[str, Any] | None = None) -> Any:
        assert path == "/api/v3/aggTrades"
        calls.append(dict(params or {}))
        return first_page if len(calls) == 1 else second_page if len(calls) == 2 else []

    adapter._get = fake_get  # type: ignore[method-assign]
    pages = list(adapter.iter_aggregate_trades("ABCUSDT", start_ms=0))
    assert [len(page) for page in pages] == [1000, 1]
    assert calls[0]["startTime"] == 0
    assert "fromId" not in calls[0]
    assert calls[1]["fromId"] == 2000
    assert pages[0][-1].record_key == "1999"
    assert pages[1][0].record_key == "2000"
    adapter.close()


def test_aggregate_trades_respects_inclusive_end_time() -> None:
    client = MockClient([
        {"a": 1, "p": "2", "q": "1", "f": 1, "l": 1, "T": 100, "m": False, "M": True},
        {"a": 2, "p": "3", "q": "1", "f": 2, "l": 2, "T": 201, "m": True, "M": True},
    ])
    adapter = BinanceSpotAdapter(client=client, request_pause=0)
    records = adapter.fetch_aggregate_trades("ABCUSDT", start_ms=0, end_ms=200)
    assert [record.record_key for record in records] == ["1"]
    adapter.close()
