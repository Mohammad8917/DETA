from __future__ import annotations

import hashlib
import io
import zipfile

import httpx
import pytest

from deta.exchanges.binance.archive import BinanceArchiveAdapter


class MockClient:
    def __init__(self, responses: dict[str, bytes | str]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str) -> httpx.Response:
        self.calls.append(url)
        payload = self.responses[url]
        request = httpx.Request("GET", url)
        if isinstance(payload, str):
            return httpx.Response(200, text=payload, request=request)
        return httpx.Response(200, content=payload, request=request)

    def close(self) -> None:
        pass


def make_zip(csv_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sample.csv", csv_text)
    return buffer.getvalue()


def checksum_text(payload: bytes, filename: str) -> str:
    return f"{hashlib.sha256(payload).hexdigest()}  {filename}\n"


def test_archive_discovery_filters_exact_months_and_ignores_checksums() -> None:
    directory = (
        "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/"
    )
    listing = """
    <a href="BTCUSDT-1m-2023-12.zip">old</a>
    <a href="BTCUSDT-1m-2024-01.zip">jan</a>
    <a href="BTCUSDT-1m-2024-01.zip.CHECKSUM">checksum</a>
    <a href="BTCUSDT-1m-2024-02.zip">feb</a>
    <a href="BTCUSDT-5m-2024-01.zip">wrong interval</a>
    """
    client = MockClient({directory: listing})
    adapter = BinanceArchiveAdapter("spot", client=client, request_pause=0)
    urls = adapter.list_monthly_archives(
        "BTCUSDT", "1m", start_month="2024-01", end_month="2024-01"
    )
    assert urls == [directory + "BTCUSDT-1m-2024-01.zip"]
    adapter.close()


def test_archive_csv_is_normalized_with_source_identity_preserved() -> None:
    url = (
        "https://data.binance.vision/data/futures/um/monthly/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    )
    csv_text = (
        "open_time,open,high,low,close,volume,close_time,quote_volume,"
        "count,taker_buy_volume,taker_buy_quote_volume,ignore\n"
        "1704067200000,1,3,0.5,2,10,1704067259999,20,4,6,12,0\n"
    )
    archive_bytes = make_zip(csv_text)
    client = MockClient({
        url: archive_bytes,
        url + ".CHECKSUM": checksum_text(archive_bytes, url.rsplit("/", 1)[-1]),
    })
    adapter = BinanceArchiveAdapter("usd-m", client=client, request_pause=0)
    records = adapter.fetch_kline_archive(url)
    assert len(records) == 1
    record = records[0]
    assert record.exchange_id == "binance"
    assert record.market_type == "binance_usd_m_futures"
    assert record.data_type == "klines_1m"
    assert record.symbol == "BTCUSDT"
    assert record.timestamp_ms == 1704067200000
    assert record.payload["close"] == "2"
    assert record.payload["source"] == "binance_official_bulk_archive"
    adapter.close()


@pytest.mark.parametrize("month", ["2024-1", "2024-13", "invalid"])
def test_invalid_month_is_rejected(month: str) -> None:
    with pytest.raises(ValueError):
        BinanceArchiveAdapter._parse_month(month)


def test_archive_adapter_rejects_unknown_market() -> None:
    with pytest.raises(ValueError, match="Unsupported Binance archive market"):
        BinanceArchiveAdapter("options")  # type: ignore[arg-type]


def test_aggregate_trade_archive_discovery_and_parsing() -> None:
    directory = (
        "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/"
    )
    archive_url = directory + "BTCUSDT-aggTrades-2024-01.zip"
    listing = (
        '<a href="BTCUSDT-aggTrades-2024-01.zip">jan</a>'
        '<a href="BTCUSDT-aggTrades-2024-01.zip.CHECKSUM">checksum</a>'
    )
    csv_text = (
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,"
        "is_buyer_maker,is_best_match\n"
        "123,65000.1,0.002,120,125,1704067200000,true,true\n"
    )
    archive_bytes = make_zip(csv_text)
    client = MockClient({
        directory: listing,
        archive_url: archive_bytes,
        archive_url + ".CHECKSUM": checksum_text(archive_bytes, archive_url.rsplit("/", 1)[-1]),
    })
    adapter = BinanceArchiveAdapter("spot", client=client, request_pause=0)
    assert adapter.list_monthly_agg_trade_archives(
        "BTCUSDT", start_month="2024-01", end_month="2024-01"
    ) == [archive_url]
    records = adapter.fetch_agg_trade_archive(archive_url)
    assert len(records) == 1
    assert records[0].data_type == "agg_trades"
    assert records[0].record_key == "123"
    assert records[0].payload["is_buyer_maker"] is True
    assert records[0].payload["is_best_match"] is True
    adapter.close()


def test_archive_checksum_mismatch_is_rejected() -> None:
    url = (
        "https://data.binance.vision/data/spot/monthly/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-01.zip"
    )
    archive_bytes = make_zip(
        "1704067200000,1,3,0.5,2,10,1704067259999,20,4,6,12,0\n"
    )
    client = MockClient({
        url: archive_bytes,
        url + ".CHECKSUM": "0" * 64 + "  BTCUSDT-1m-2024-01.zip\n",
    })
    adapter = BinanceArchiveAdapter("spot", client=client, request_pause=0)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        adapter.fetch_kline_archive(url)
    adapter.close()


def test_spot_microsecond_archive_timestamps_are_normalized_to_milliseconds() -> None:
    url = (
        "https://data.binance.vision/data/spot/monthly/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2025-01.zip"
    )
    archive_bytes = make_zip(
        "1735689600000000,1,3,0.5,2,10,1735689659999999,20,4,6,12,0\n"
    )
    client = MockClient({
        url: archive_bytes,
        url + ".CHECKSUM": checksum_text(archive_bytes, url.rsplit("/", 1)[-1]),
    })
    adapter = BinanceArchiveAdapter("spot", client=client, request_pause=0)
    record = adapter.fetch_kline_archive(url)[0]
    assert record.timestamp_ms == 1735689600000
    assert record.payload["source_open_time"] == 1735689600000000
    assert record.payload["source_timestamp_unit"] == "microseconds"
    assert record.payload["close_time_ms"] == 1735689659999
    adapter.close()


def test_spot_aggregate_trade_microsecond_timestamp_is_normalized() -> None:
    url = (
        "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/"
        "BTCUSDT-aggTrades-2025-01.zip"
    )
    archive_bytes = make_zip(
        "0,0.2,50,0,0,1735689600010866,False,True\n"
    )
    client = MockClient({
        url: archive_bytes,
        url + ".CHECKSUM": checksum_text(archive_bytes, url.rsplit("/", 1)[-1]),
    })
    adapter = BinanceArchiveAdapter("spot", client=client, request_pause=0)
    record = adapter.fetch_agg_trade_archive(url)[0]
    assert record.timestamp_ms == 1735689600010
    assert record.payload["source_timestamp"] == 1735689600010866
    assert record.payload["source_timestamp_unit"] == "microseconds"
    adapter.close()


def test_individual_trade_archive_discovery_and_parsing() -> None:
    directory = "https://data.binance.vision/data/spot/monthly/trades/ETHUSDT/"
    archive_url = directory + "ETHUSDT-trades-2025-01.zip"
    listing = '<a href="ETHUSDT-trades-2025-01.zip">jan</a>'
    csv_text = (
        "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\\n"
        "42,3000.5,0.25,750.125,1735689600010866,true,true\\n"
    )
    archive_bytes = make_zip(csv_text)
    client = MockClient({
        directory: listing,
        archive_url: archive_bytes,
        archive_url + ".CHECKSUM": checksum_text(archive_bytes, archive_url.rsplit("/", 1)[-1]),
    })
    adapter = BinanceArchiveAdapter("spot", client=client, request_pause=0)
    assert adapter.list_monthly_trade_archives(
        "ETHUSDT", start_month="2025-01", end_month="2025-01"
    ) == [archive_url]
    record = adapter.fetch_trade_archive(archive_url)[0]
    assert record.data_type == "trades"
    assert record.record_key == "42"
    assert record.timestamp_ms == 1735689600010
    assert record.payload["quote_or_base_quantity"] == "750.125"
    assert record.payload["is_buyer_maker"] is True
    adapter.close()
