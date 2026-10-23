"""Official Binance public bulk archive ingestion for historical native klines."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from itertools import chain
import time
import zipfile
from datetime import date
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit
from xml.etree import ElementTree

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
    price_kline_types = {
        "markPriceKlines": "markPrice",
        "indexPriceKlines": "indexPrice",
        "premiumIndexKlines": "premiumIndex",
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
        self.last_verified_checksum_sha256: str | None = None
        self.last_verified_size_bytes: int | None = None

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

    def _list_directory_hrefs(self, directory: str) -> list[str]:
        """List entries from HTML when available, falling back to S3 ListObjectsV2.

        The public file-browser can render entries through JavaScript and expose no
        archive links to an HTML parser. The S3 listing API is the fallback source.
        """
        response = self._get(directory, allow_not_found=True)
        if response.status_code != 404:
            parser = _Links()
            parser.feed(response.text)
            useful = [
                href for href in parser.hrefs
                if href not in {"../", "./", "/"}
                and (href.endswith("/") or ".zip" in href.lower())
            ]
            if useful:
                return useful

        prefix = urlsplit(directory).path.lstrip("/")
        if not prefix.endswith("/"):
            prefix += "/"
        endpoint = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
        entries: set[str] = set()
        continuation: str | None = None
        while True:
            params = {"list-type": "2", "prefix": prefix, "delimiter": "/"}
            if continuation:
                params["continuation-token"] = continuation
            listing_url = f"{endpoint}?{urlencode(params)}"
            listing_response = self._get(listing_url, allow_not_found=True)
            if listing_response.status_code == 404:
                return []
            try:
                root = ElementTree.fromstring(listing_response.content)
            except ElementTree.ParseError as exc:
                raise RuntimeError(f"Invalid S3 archive listing XML for prefix {prefix}") from exc

            def child_text(element: ElementTree.Element, wanted: str) -> str | None:
                for child in element.iter():
                    if child.tag.rsplit("}", 1)[-1] == wanted:
                        return child.text
                return None

            for element in root.iter():
                tag = element.tag.rsplit("}", 1)[-1]
                if tag == "Contents":
                    key = child_text(element, "Key")
                    if key and key.startswith(prefix):
                        entries.add(key[len(prefix):])
                elif tag == "CommonPrefixes":
                    child_prefix = child_text(element, "Prefix")
                    if child_prefix and child_prefix.startswith(prefix):
                        entries.add(child_prefix[len(prefix):])

            truncated = child_text(root, "IsTruncated")
            continuation = child_text(root, "NextContinuationToken")
            if truncated != "true" or not continuation:
                break
        return sorted(entries)

    def list_archive_symbols(self, dataset: str, frequency: str = "monthly") -> list[str]:
        """Discover symbols from official monthly/daily archive indexes, including delisted markets."""
        folders = {
            "klines": "klines", "aggTrades": "aggTrades", "trades": "trades",
            **{key: key for key in self.price_kline_types},
            "fundingRate": "fundingRate",
        }
        if dataset not in folders:
            raise ValueError(f"Unsupported Binance archive dataset index: {dataset}")
        if frequency not in {"monthly", "daily"}:
            raise ValueError(f"Unsupported archive frequency: {frequency}")
        directory = (
            f"{self.base_url}/data/{self.archive_market_path}/{frequency}/{folders[dataset]}/"
        )
        hrefs = self._list_directory_hrefs(directory)
        symbols: set[str] = set()
        for href in hrefs:
            if not href.endswith("/"):
                continue
            name = href.rstrip("/")
            # Only accept immediate child directories; do not mistake nested paths for symbols.
            if "/" in name:
                continue
            if name and re.fullmatch(r"[A-Za-z0-9_]+", name):
                symbols.add(name)
        return sorted(symbols)

    def list_daily_archives(
        self, dataset: str, symbol: str, interval: str | None = None,
        start_month: str | None = None, end_month: str | None = None,
    ) -> list[str]:
        """Return official daily archives for a dataset, optionally bounded by month."""
        allowed = {"klines", "aggTrades", "trades", *self.price_kline_types}
        if dataset not in allowed:
            raise ValueError(f"Unsupported Binance daily archive dataset: {dataset}")
        if not symbol or "/" in symbol or "\\" in symbol:
            raise ValueError("Invalid symbol for archive path")
        if dataset == "klines" or dataset in self.price_kline_types:
            if interval not in self.candle_intervals:
                raise ValueError(f"Unsupported kline interval: {interval}")
            archive_interval = "1mo" if interval == "1M" else interval
            if dataset == "klines":
                directory = (
                    f"{self.base_url}/data/{self.archive_market_path}/daily/klines/"
                    f"{symbol}/{archive_interval}/"
                )
                filename_prefix = f"{symbol}-{archive_interval}"
            else:
                if self.market == "spot":
                    raise ValueError("Futures price kline archives are not available for Spot")
                directory = (
                    f"{self.base_url}/data/{self.archive_market_path}/daily/{dataset}/"
                    f"{symbol}/{archive_interval}/"
                )
                filename_prefix = f"{symbol}-{self.price_kline_types[dataset]}-{archive_interval}"
            pattern = re.compile(
                rf"^{re.escape(filename_prefix)}-(\d{{4}}-\d{{2}}-\d{{2}})\.zip$"
            )
        else:
            directory = (
                f"{self.base_url}/data/{self.archive_market_path}/daily/{dataset}/{symbol}/"
            )
            pattern = re.compile(
                rf"^{re.escape(symbol)}-{re.escape(dataset)}-(\d{{4}}-\d{{2}}-\d{{2}})\.zip$"
            )
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise ValueError("start_month must not be later than end_month")
        hrefs = self._list_directory_hrefs(directory)
        found: list[tuple[date, str]] = []
        for href in hrefs:
            name = href.rstrip("/").rsplit("/", 1)[-1]
            match = pattern.fullmatch(name)
            if not match:
                continue
            try:
                day = date.fromisoformat(match.group(1))
            except ValueError as exc:
                raise ValueError(f"Invalid daily archive date in filename: {name}") from exc
            month = day.replace(day=1)
            if start and month < start:
                continue
            if end and month > end:
                continue
            found.append((day, urljoin(directory, href)))
        return [url for _, url in sorted(set(found), key=lambda item: item[0])]

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
        hrefs = self._list_directory_hrefs(directory)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-{re.escape(archive_interval)}-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in hrefs:
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

    def list_monthly_funding_rate_archives(
        self, symbol: str, start_month: str | None = None,
        end_month: str | None = None,
    ) -> list[str]:
        """List official monthly Futures funding-rate archives for a contract."""
        if self.market == "spot":
            raise ValueError("Funding-rate archives are not available for Spot")
        if not symbol or "/" in symbol or "\\" in symbol:
            raise ValueError("Invalid symbol for archive path")
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise ValueError("start_month must not be later than end_month")
        directory = (
            f"{self.base_url}/data/{self.archive_market_path}/monthly/fundingRate/{symbol}/"
        )
        hrefs = self._list_directory_hrefs(directory)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-fundingRate-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in hrefs:
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

    def fetch_funding_rate_archive(self, url: str) -> list[DataRecord]:
        """Download, checksum-verify, and parse one official Futures funding-rate ZIP."""
        if self.market == "spot":
            raise ValueError("Funding-rate archives are not available for Spot")
        archive_bytes = self._get_verified_archive(url)
        symbol = url.rstrip("/").rsplit("/", 1)[-1].split("-fundingRate-", 1)[0]
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(csv_names) != 1:
                    raise ValueError(
                        f"Expected exactly one CSV in archive {url}, found {len(csv_names)}"
                    )
                with archive.open(csv_names[0]) as binary:
                    reader = csv.reader(io.TextIOWrapper(binary, encoding="utf-8-sig", newline=""))
                    rows = iter(reader)
                    first_row = next(rows, None)
                    if first_row is None:
                        return []
                    header: dict[str, int] | None = None
                    normalized = [value.strip().lower().replace(" ", "_") for value in first_row]
                    if any(value in {"calc_time", "funding_time", "fundingtime"} for value in normalized):
                        header = {value: index for index, value in enumerate(normalized)}
                        data_rows = rows
                    else:
                        data_rows = chain([first_row], rows)
                    records: list[DataRecord] = []
                    for row in data_rows:
                        if not row:
                            continue
                        try:
                            if header is not None:
                                time_index = next(
                                    header[key] for key in ("calc_time", "funding_time", "fundingtime")
                                    if key in header
                                )
                                rate_index = next(
                                    header[key] for key in ("last_funding_rate", "funding_rate", "fundingrate")
                                    if key in header
                                )
                                timestamp = int(row[time_index])
                                rate = row[rate_index]
                                interval_index = header.get("funding_interval_hours")
                                interval_hours = row[interval_index] if interval_index is not None else None
                                symbol_index = header.get("symbol")
                                if symbol_index is not None and row[symbol_index]:
                                    symbol = row[symbol_index]
                            elif row[0].strip().upper() == symbol.upper() and len(row) >= 4:
                                timestamp = int(row[1])
                                interval_hours = row[2]
                                rate = row[3]
                            elif len(row) >= 3:
                                timestamp = int(row[0])
                                interval_hours = row[1]
                                rate = row[2]
                            else:
                                raise ValueError("Expected at least three funding-rate columns")
                        except (ValueError, StopIteration, IndexError) as exc:
                            raise ValueError(f"Malformed funding-rate row in {url}: {row}") from exc
                        payload: dict[str, Any] = {
                            "funding_time_ms": timestamp,
                            "funding_interval_hours": interval_hours,
                            "funding_rate": rate,
                            "last_funding_rate": rate,
                            "source": "binance_official_bulk_archive",
                            "archive_url": url,
                        }
                        records.append(DataRecord(
                            "binance", self.market_type, "funding_rate", symbol,
                            timestamp, str(timestamp), payload,
                        ))
                    return records
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid ZIP archive: {url}") from exc

    def list_monthly_price_kline_archives(
        self, symbol: str, interval: str, price_type: str,
        start_month: str | None = None, end_month: str | None = None,
    ) -> list[str]:
        """List official monthly Futures mark/index/premium-price candle archives."""
        if self.market == "spot":
            raise ValueError("Futures price kline archives are not available for Spot")
        if price_type not in {"markPrice", "indexPrice", "premiumIndex"}:
            raise ValueError(f"Unsupported Futures price kline type: {price_type}")
        if interval not in self.candle_intervals:
            raise ValueError(f"Unsupported kline interval: {interval}")
        folder = f"{price_type}Klines"
        archive_interval = "1mo" if interval == "1M" else interval
        directory = (
            f"{self.base_url}/data/{self.archive_market_path}/monthly/{folder}/"
            f"{symbol}/{archive_interval}/"
        )
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise ValueError("start_month must not be later than end_month")
        hrefs = self._list_directory_hrefs(directory)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-{re.escape(price_type)}-{re.escape(archive_interval)}-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in hrefs:
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

    def fetch_price_kline_archive(self, url: str, price_type: str) -> list[DataRecord]:
        """Download and checksum-verify a Futures mark/index/premium-price candle archive."""
        if self.market == "spot":
            raise ValueError("Futures price kline archives are not available for Spot")
        type_names = {
            "markPrice": "mark_price",
            "indexPrice": "index_price",
            "premiumIndex": "premium_index",
        }
        if price_type not in type_names:
            raise ValueError(f"Unsupported Futures price kline type: {price_type}")
        archive_bytes = self._get_verified_archive(url)
        parts = url.split("?", 1)[0].rstrip("/").split("/")
        if len(parts) < 3:
            raise ValueError(f"Cannot infer symbol and interval from archive URL: {url}")
        symbol, archive_interval = parts[-3], parts[-2]
        interval = "1M" if archive_interval == "1mo" else archive_interval
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(csv_names) != 1:
                    raise ValueError(
                        f"Expected exactly one CSV in archive {url}, found {len(csv_names)}"
                    )
                with archive.open(csv_names[0]) as binary:
                    reader = csv.reader(io.TextIOWrapper(binary, encoding="utf-8-sig", newline=""))
                    records: list[DataRecord] = []
                    for row in reader:
                        if not row:
                            continue
                        if row[0].strip().lower() in {"open_time", "open time"}:
                            continue
                        if len(row) < 7:
                            raise ValueError(
                                f"Malformed price kline row in {url}: expected at least 7 columns"
                            )
                        try:
                            source_open_time = int(row[0])
                            source_close_time = int(row[6])
                        except ValueError as exc:
                            raise ValueError(f"Invalid numeric fields in {url}") from exc
                        open_time, timestamp_unit = self._normalize_timestamp(source_open_time)
                        close_time, _ = self._normalize_timestamp(source_close_time)
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
                            "close_time_ms": close_time,
                            "raw_columns": row,
                            "source": "binance_official_bulk_archive",
                            "archive_url": url,
                            "price_type": price_type,
                        }
                        records.append(DataRecord(
                            "binance", self.market_type,
                            f"{type_names[price_type]}_klines_{interval}", symbol,
                            open_time, str(open_time), payload,
                        ))
                    return records
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid ZIP archive: {url}") from exc

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
        hrefs = self._list_directory_hrefs(directory)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-aggTrades-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in hrefs:
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
        hrefs = self._list_directory_hrefs(directory)
        pattern = re.compile(
            rf"^{re.escape(symbol)}-trades-(\d{{4}}-\d{{2}})\.zip$"
        )
        found: list[tuple[date, str]] = []
        for href in hrefs:
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
                        header = row[0].strip().lower().replace(" ", "_")
                        if header in {"id", "trade_id"}:
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
        match = re.fullmatch(r"(.+)-trades-\d{4}-\d{2}(?:-\d{2})?\.zip", name)
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
                        header = row[0].strip().lower().replace(" ", "_")
                        if header in {"agg_trade_id", "agg_tradeid", "aggregate_trade_id"}:
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
        match = re.fullmatch(r"(.+)-aggTrades-\d{4}-\d{2}(?:-\d{2})?\.zip", name)
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
        self.last_verified_checksum_sha256 = actual
        self.last_verified_size_bytes = len(response.content)
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
        match = re.fullmatch(r"(.+)-([^-]+)-(\d{4}-\d{2}(?:-\d{2})?)\.zip", name)
        if not match:
            raise ValueError(f"Cannot infer symbol and interval from archive filename: {name}")
        interval = "1M" if match.group(2) == "1mo" else match.group(2)
        return match.group(1), interval
