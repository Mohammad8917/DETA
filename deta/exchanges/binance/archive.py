"""Official Binance public bulk archive ingestion for historical native klines."""

from __future__ import annotations

import csv
import hashlib
import io
import re
import time
import zipfile
from datetime import date
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

import httpx

from deta.domain import DataRecord


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self.hrefs.append(href)


class BinanceArchiveAdapter:
    """Discover and parse official monthly kline ZIP archives from data.binance.vision."""

    base_url = "https://data.binance.vision"
    market_paths = {
        "spot": ("spot", "spot"),
        "usd-m": ("futures/um", "binance_usd_m_futures"),
        "coin-m": ("futures/cm", "binance_coin_m_futures"),
    }
    candle_intervals = (
        "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h",
        "12h", "1d", "3d", "1w", "1M",
    )

    def __init__(
        self,
        market: str,
        *,
        timeout: float = 60.0,
        request_pause: float = 0.1,
        client: httpx.Client | None = None,
    ) -> None:
        if market not in self.market_paths:
            raise ValueError(f"Unsupported Binance archive market: {market}")
        self.market = market
        self.archive_market_path, self.market_type = self.market_paths[market]
        self._client = client or httpx.Client(
            timeout=timeout, follow_redirects=True, headers={"User-Agent": "DETA/0.1"}
        )
        self._owns_client = client is None
        self.request_pause = max(0.0, request_pause)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _get(self, url: str, *, allow_not_found: bool = False) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                response = self._client.get(url)
                if response.status_code in (418, 429):
                    time.sleep(float(response.headers.get("Retry-After", min(60, 2**attempt))))
                    continue
                if response.status_code >= 500:
                    last_error = RuntimeError(f"HTTP {response.status_code} for {url}")
                    time.sleep(min(30.0, 2**attempt))
                    continue
                if response.status_code == 404 and allow_not_found:
                    return response
                if 400 <= response.status_code < 500:
                    raise RuntimeError(f"HTTP {response.status_code} for {url}")
                response.raise_for_status()
                if self.request_pause:
                    time.sleep(self.request_pause)
                return response
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == 5:
                    break
                time.sleep(min(30.0, 2**attempt))
        raise RuntimeError(f"Binance archive request failed for {url}: {last_error}") from last_error

    def list_monthly_archives(
        self, symbol: str, interval: str, start_month: str | None = None,
        end_month: str | None = None,
    ) -> list[str]:
        """Return source-listed monthly ZIP URLs, optionally restricted to YYYY-MM bounds."""
        if interval not in self.candle_intervals:
            raise ValueError(f"Unsupported Binance kline interval: {interval}")
        if not symbol or "/" in symbol or "\\" in symbol:
            raise ValueError("Invalid symbol for archive path")
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise ValueError("start_month must not be later than end_month")

        archive_interval = "1mo" if interval == "1M" else interval
        directory = (
            f"{self.base_url}/data/{self.archive_market_path}/monthly/klines/"
            f"{symbol}/{archive_interval}/"
        )
        response = self._get(directory, allow_not_found=True)
        if response.status_code == 404:
            return []
        parser = _Links()
        parser.feed(response.text)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-{re.escape(archive_interval)}-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in parser.hrefs:
            name = href.rstrip("/").rsplit("/", 1)[-1]
            match = pattern.fullmatch(name)
            if not match:
                continue
            month = self._parse_month(match.group(1))
            if start and month < start:
                continue
            if end and month > end:
                continue
            found.append((month, urljoin(directory, href)))
        return [url for _, url in sorted(set(found), key=lambda item: item[0])]

    @staticmethod
    def _parse_month(value: str) -> date:
        if not re.fullmatch(r"\d{4}-\d{2}", value):
            raise ValueError("Month must use YYYY-MM format")
        try:
            return date.fromisoformat(f"{value}-01")
        except ValueError as exc:
            raise ValueError(f"Invalid month: {value}") from exc

    def list_monthly_agg_trade_archives(
        self, symbol: str, start_month: str | None = None, end_month: str | None = None,
    ) -> list[str]:
        """Return official monthly aggregate-trade ZIP archives listed for one symbol."""
        if not symbol or "/" in symbol or "\\" in symbol:
            raise ValueError("Invalid symbol for archive path")
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise ValueError("start_month must not be later than end_month")
        directory = (
            f"{self.base_url}/data/{self.archive_market_path}/monthly/aggTrades/{symbol}/"
        )
        response = self._get(directory, allow_not_found=True)
        if response.status_code == 404:
            return []
        parser = _Links()
        parser.feed(response.text)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-aggTrades-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in parser.hrefs:
            name = href.rstrip("/").rsplit("/", 1)[-1]
            match = pattern.fullmatch(name)
            if not match:
                continue
            month = self._parse_month(match.group(1))
            if start and month < start:
                continue
            if end and month > end:
                continue
            found.append((month, urljoin(directory, href)))
        return [url for _, url in sorted(set(found), key=lambda item: item[0])]

    def list_monthly_trade_archives(
        self, symbol: str, start_month: str | None = None, end_month: str | None = None,
    ) -> list[str]:
        """Return official monthly individual-trade ZIP archives listed for one symbol."""
        if not symbol or "/" in symbol or "\\" in symbol:
            raise ValueError("Invalid symbol for archive path")
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise ValueError("start_month must not be later than end_month")
        directory = (
            f"{self.base_url}/data/{self.archive_market_path}/monthly/trades/{symbol}/"
        )
        response = self._get(directory, allow_not_found=True)
        if response.status_code == 404:
            return []
        parser = _Links()
        parser.feed(response.text)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-trades-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in parser.hrefs:
            name = href.rstrip("/").rsplit("/", 1)[-1]
            match = pattern.fullmatch(name)
            if not match:
                continue
            month = self._parse_month(match.group(1))
            if start and month < start:
                continue
            if end and month > end:
                continue
            found.append((month, urljoin(directory, href)))
        return [url for _, url in sorted(set(found), key=lambda item: item[0])]

    def fetch_trade_archive(self, url: str) -> list[DataRecord]:
        """Download, checksum-verify, and normalize one monthly individual-trade ZIP."""
        archive_bytes = self._get_verified_archive(url)
        symbol = self._trade_archive_symbol(url)
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(csv_names) != 1:
                    raise ValueError(
                        f"Expected exactly one CSV in archive {url}, found {len(csv_names)}"
                    )
                with archive.open(csv_names[0]) as binary:
                    text_stream = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
                    reader = csv.reader(text_stream)
                    records: list[DataRecord] = []
                    for row in reader:
                        if not row:
                            continue
                        if row[0].strip().lower() in {"id", "trade_id"}:
                            continue
                        if len(row) < 6:
                            raise ValueError(
                                f"Malformed trade row in {url}: expected at least 6 columns"
                            )
                        try:
                            trade_id = int(row[0])
                            source_timestamp = int(row[4])
                        except ValueError as exc:
                            raise ValueError(f"Invalid numeric fields in {url}") from exc
                        timestamp, timestamp_unit = self._normalize_timestamp(source_timestamp)
                        payload: dict[str, Any] = {
                            "trade_id": trade_id,
                            "price": row[1],
                            "quantity": row[2],
                            "quote_or_base_quantity": row[3],
                            "timestamp_ms": timestamp,
                            "source_timestamp": source_timestamp,
                            "source_timestamp_unit": timestamp_unit,
                            "is_buyer_maker": row[5].strip().lower() in {"true", "1"},
                            "is_best_match": (
                                row[6].strip().lower() in {"true", "1"} if len(row) > 6 else None
                            ),
                            "source": "binance_official_bulk_archive",
                            "archive_url": url,
                        }
                        records.append(DataRecord(
                            "binance", self.market_type, "trades", symbol,
                            timestamp, str(trade_id), payload,
                        ))
                    return records
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid ZIP archive: {url}") from exc

    @staticmethod
    def _trade_archive_symbol(url: str) -> str:
        name = url.rstrip("/").rsplit("/", 1)[-1]
        match = re.fullmatch(r"(.+)-trades-\d{4}-\d{2}\.zip", name)
        if not match:
            raise ValueError(f"Cannot infer symbol from trade archive filename: {name}")
        return match.group(1)

    def fetch_agg_trade_archive(self, url: str) -> list[DataRecord]:
        """Download, checksum-verify, and normalize one monthly aggregate-trade ZIP."""
        archive_bytes = self._get_verified_archive(url)
        symbol = self._agg_trade_archive_symbol(url)
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(csv_names) != 1:
                    raise ValueError(
                        f"Expected exactly one CSV in archive {url}, found {len(csv_names)}"
                    )
                with archive.open(csv_names[0]) as binary:
                    text_stream = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
                    reader = csv.reader(text_stream)
                    records: list[DataRecord] = []
                    for row in reader:
                        if not row:
                            continue
                        if row[0].strip().lower() in {"agg_trade_id", "aggregate_trade_id"}:
                            continue
                        if len(row) < 7:
                            raise ValueError(
                                f"Malformed aggregate-trade row in {url}: expected at least 7 columns"
                            )
                        try:
                            trade_id = int(row[0])
                            first_trade_id = int(row[3])
                            last_trade_id = int(row[4])
                            source_timestamp = int(row[5])
                        except ValueError as exc:
                            raise ValueError(f"Invalid numeric fields in {url}") from exc
                        timestamp, timestamp_unit = self._normalize_timestamp(source_timestamp)
                        payload: dict[str, Any] = {
                            "aggregate_trade_id": trade_id,
                            "price": row[1],
                            "quantity": row[2],
                            "first_trade_id": first_trade_id,
                            "last_trade_id": last_trade_id,
                            "timestamp_ms": timestamp,
                            "source_timestamp": source_timestamp,
                            "source_timestamp_unit": timestamp_unit,
                            "is_buyer_maker": row[6].strip().lower() in {"true", "1"},
                            "is_best_match": (
                                row[7].strip().lower() in {"true", "1"} if len(row) > 7 else None
                            ),
                            "source": "binance_official_bulk_archive",
                            "archive_url": url,
                        }
                        records.append(DataRecord(
                            "binance", self.market_type, "agg_trades", symbol,
                            timestamp, str(trade_id), payload,
                        ))
                    return records
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid ZIP archive: {url}") from exc

    @staticmethod
    def _agg_trade_archive_symbol(url: str) -> str:
        name = url.rstrip("/").rsplit("/", 1)[-1]
        match = re.fullmatch(r"(.+)-aggTrades-\d{4}-\d{2}\.zip", name)
        if not match:
            raise ValueError(f"Cannot infer symbol from aggregate-trade archive filename: {name}")
        return match.group(1)

    def fetch_kline_archive(self, url: str) -> list[DataRecord]:
        """Download, checksum-verify, and parse one monthly native kline ZIP."""
        archive_bytes = self._get_verified_archive(url)
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(csv_names) != 1:
                    raise ValueError(
                        f"Expected exactly one CSV in archive {url}, found {len(csv_names)}"
                    )
                with archive.open(csv_names[0]) as binary:
                    text_stream = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
                    reader = csv.reader(text_stream)
                    records: list[DataRecord] = []
                    for row in reader:
                        if not row:
                            continue
                        if row[0].strip().lower() in {"open_time", "open time"}:
                            continue
                        if len(row) < 12:
                            raise ValueError(
                                f"Malformed kline row in {url}: expected at least 12 columns"
                            )
                        try:
                            source_open_time = int(row[0])
                            source_close_time = int(row[6])
                            trade_count = int(row[8])
                        except ValueError as exc:
                            raise ValueError(f"Invalid numeric fields in {url}") from exc
                        open_time, timestamp_unit = self._normalize_timestamp(source_open_time)
                        close_time, _ = self._normalize_timestamp(source_close_time)
                        symbol, interval = self._archive_identity(url)
                        payload: dict[str, Any] = {
                            "interval": interval,
                            "open_time_ms": open_time,
                            "source_open_time": source_open_time,
                            "source_close_time": source_close_time,
                            "source_timestamp_unit": timestamp_unit,
                            "open": row[1],
                            "high": row[2],
                            "low": row[3],
                            "close": row[4],
                            "volume": row[5],
                            "close_time_ms": close_time,
                            "quote_volume_or_contract_volume": row[7],
                            "trade_count": trade_count,
                            "taker_buy_volume": row[9],
                            "taker_buy_quote_volume": row[10],
                            "source": "binance_official_bulk_archive",
                            "archive_url": url,
                        }
                        records.append(DataRecord(
                            "binance", self.market_type, f"klines_{interval}", symbol,
                            open_time, str(open_time), payload,
                        ))
                    return records
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid ZIP archive: {url}") from exc

    def _get_verified_archive(self, url: str) -> bytes:
        """Require the official SHA-256 sidecar before parsing any archive contents."""
        response = self._get(url)
        checksum_response = self._get(f"{url}.CHECKSUM")
        match = re.search(r"\b([0-9a-fA-F]{64})\b", checksum_response.text)
        if match is None:
            raise ValueError(f"Missing or malformed SHA-256 checksum for archive: {url}")
        actual = hashlib.sha256(response.content).hexdigest()
        expected = match.group(1).lower()
        if actual != expected:
            raise ValueError(
                f"SHA-256 mismatch for archive {url}: expected {expected}, got {actual}"
            )
        return response.content

    @staticmethod
    def _normalize_timestamp(source_timestamp: int) -> tuple[int, str]:
        """Normalize Binance's post-2025 Spot microsecond timestamps to stored milliseconds."""
        if source_timestamp < 0:
            raise ValueError("Source timestamp must be non-negative")
        if source_timestamp >= 100_000_000_000_000:
            return source_timestamp // 1000, "microseconds"
        return source_timestamp, "milliseconds"

    @staticmethod
    def _archive_identity(url: str) -> tuple[str, str]:
        name = url.rstrip("/").rsplit("/", 1)[-1]
        match = re.fullmatch(r"(.+)-([^-]+)-(\d{4}-\d{2})\.zip", name)
        if not match:
            raise ValueError(f"Cannot infer symbol and interval from archive filename: {name}")
        interval = "1M" if match.group(2) == "1mo" else match.group(2)
        return match.group(1), interval
