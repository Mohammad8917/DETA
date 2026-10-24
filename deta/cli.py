"""Command-line entry points for DETA historical collection."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import re
from pathlib import Path
from typing import Literal, Sequence

from deta.exchanges.binance import BinanceSpotAdapter
from deta.exchanges.binance.archive import BinanceArchiveAdapter
from deta.exchanges.binance.derivatives import BinanceDerivativesAdapter
from deta.storage import SQLiteStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="deta", description="Historical public exchange data collector")
    parser.add_argument("--db", type=Path, default=Path("data/deta.sqlite3"), help="SQLite database path")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="Refresh Binance Spot symbol metadata")
    candles = commands.add_parser("backfill-klines", help="Backfill Binance Spot candles through REST")
    candles.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    candles.add_argument("--symbols", nargs="*", help="Optional symbols; default is current trading symbols")
    candles.add_argument("--include-inactive", action="store_true")
    trades = commands.add_parser("backfill-agg-trades", help="Backfill Binance Spot aggregate trades through REST")
    trades.add_argument("--symbols", nargs="*", help="Optional symbols; default is current trading symbols")
    trades.add_argument("--start-ms", type=int, default=0, help="Initial timestamp in Unix milliseconds")
    trades.add_argument("--end-ms", type=int, help="Optional inclusive ending timestamp in Unix milliseconds")
    trades.add_argument("--include-inactive", action="store_true")
    derivative_discover = commands.add_parser("discover-derivatives", help="Refresh Binance Futures contract metadata")
    derivative_discover.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    derivative_candles = commands.add_parser("backfill-derivative-klines", help="Backfill Binance Futures candles through REST")
    derivative_candles.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    derivative_candles.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    derivative_candles.add_argument("--symbols", nargs="*", help="Optional contract symbols")
    derivative_candles.add_argument("--include-inactive", action="store_true")
    funding = commands.add_parser(
        "backfill-funding-rates", help="Backfill historical Binance Futures funding rates"
    )
    funding.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    funding.add_argument("--symbols", nargs="*", help="Optional contract symbols; default is all locally known")
    funding.add_argument("--start-ms", type=int, default=0, help="Initial Unix timestamp in milliseconds")
    funding.add_argument("--end-ms", type=int, help="Optional inclusive ending Unix timestamp in milliseconds")
    price_archive = commands.add_parser(
        "backfill-archive-price-klines",
        help="Import official Binance Futures mark/index/premium-price candle archives",
    )
    price_archive.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    price_archive.add_argument("--price-type", choices=("markPrice", "indexPrice", "premiumIndex"), required=True)
    price_archive.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    price_archive.add_argument("--symbols", nargs="*", help="Optional contracts; otherwise discover from archive indexes")
    price_archive.add_argument("--start-month", help="Optional inclusive YYYY-MM archive month")
    price_archive.add_argument("--end-month", help="Optional inclusive YYYY-MM archive month")
    funding_archive = commands.add_parser(
        "backfill-archive-funding-rates",
        help="Import official monthly Binance Futures funding-rate archives",
    )
    funding_archive.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    funding_archive.add_argument("--symbols", nargs="*", help="Optional contracts; otherwise discover from archive indexes")
    funding_archive.add_argument("--start-month", help="Optional inclusive YYYY-MM archive month")
    funding_archive.add_argument("--end-month", help="Optional inclusive YYYY-MM archive month")
    archive = commands.add_parser(
        "backfill-archive-klines",
        help="Import Binance official monthly historical kline ZIP archives",
    )
    archive.add_argument("--market", choices=("spot", "usd-m", "coin-m"), required=True)
    archive.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    archive.add_argument("--symbols", nargs="*", help="Optional symbols/contracts; otherwise use all locally known")
    archive.add_argument("--start-month", help="Optional inclusive YYYY-MM archive month")
    archive.add_argument("--end-month", help="Optional inclusive YYYY-MM archive month")
    archive_trades = commands.add_parser(
        "backfill-archive-agg-trades",
        help="Import Binance official monthly aggregate-trade ZIP archives",
    )
    archive_trades.add_argument("--market", choices=("spot", "usd-m", "coin-m"), required=True)
    archive_trades.add_argument("--symbols", nargs="*", help="Optional symbols/contracts; otherwise use all locally known")
    archive_trades.add_argument("--start-month", help="Optional inclusive YYYY-MM archive month")
    archive_trades.add_argument("--end-month", help="Optional inclusive YYYY-MM archive month")
    raw_trades = commands.add_parser(
        "backfill-archive-trades",
        help="Import Binance official monthly individual-trade ZIP archives",
    )
    raw_trades.add_argument("--market", choices=("spot", "usd-m", "coin-m"), required=True)
    raw_trades.add_argument("--symbols", nargs="*", help="Optional symbols/contracts; otherwise use all locally known")
    raw_trades.add_argument("--start-month", help="Optional inclusive YYYY-MM archive month")
    raw_trades.add_argument("--end-month", help="Optional inclusive YYYY-MM archive month")
    manifest = commands.add_parser(
        "archive-manifest", help="Audit persisted official archive verification/import states"
    )
    manifest.add_argument("--market-type", choices=("spot", "usd_m", "coin_m"))
    manifest.add_argument("--data-type", help="Optional exact dataset type filter")
    manifest.add_argument("--symbol", help="Optional exact symbol/contract filter")
    reconcile = commands.add_parser(
        "reconcile-archive-klines",
        help="Compare source-listed Binance kline archives with the local verification manifest for one bounded dataset",
    )
    reconcile.add_argument("--market", choices=("spot", "usd-m", "coin-m"), required=True)
    reconcile.add_argument("--symbol", required=True, help="One symbol/contract")
    reconcile.add_argument(
        "--interval", required=True,
        choices=("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"),
    )
    reconcile.add_argument("--start-month", required=True, help="Inclusive YYYY-MM")
    reconcile.add_argument("--end-month", required=True, help="Inclusive YYYY-MM")
    gap_audit = commands.add_parser(
        "audit-kline-gaps",
        help="Audit stored candle timestamp continuity in an explicit bounded UTC-millisecond range",
    )
    gap_audit.add_argument("--market-type", choices=("spot", "usd_m", "coin_m"), required=True)
    gap_audit.add_argument("--symbol", required=True)
    gap_audit.add_argument(
        "--interval", choices=("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d"), required=True,
    )
    gap_audit.add_argument("--start-ms", type=int, required=True, help="Inclusive UTC Unix timestamp in milliseconds; must align to interval")
    gap_audit.add_argument("--end-ms", type=int, required=True, help="Inclusive UTC Unix timestamp in milliseconds; must align to interval")
    commands.add_parser("status", help="Show locally stored dataset coverage/status")
    return parser


def _select_symbols(
    store: SQLiteStore,
    adapter: BinanceSpotAdapter,
    requested: list[str] | None,
    include_inactive: bool,
) -> list[dict[str, object]]:
    discovered = adapter.discover_symbols()
    store.save_symbols(discovered)
    symbols = store.get_symbols("binance", "spot")
    wanted = set(requested) if requested else None
    if wanted is not None:
        known = {str(item["symbol"]) for item in symbols}
        unknown = wanted - known
        if unknown:
            logging.getLogger("deta").warning(
                "Requested symbols absent from latest exchange metadata: %s", sorted(unknown)
            )
        symbols = [item for item in symbols if item["symbol"] in wanted]
    if not include_inactive:
        symbols = [
            item for item in symbols
            if item.get("status") == "TRADING" and item.get("is_currently_listed", True)
        ]
    return symbols


def _monotonic_cursor(
    store: SQLiteStore,
    exchange_id: str,
    market_type: str,
    data_type: str,
    symbol: str,
    candidate: int,
) -> str:
    """Prevent archive imports from moving a dataset checkpoint backward."""
    existing = store.get_checkpoint(exchange_id, market_type, data_type, symbol)
    if existing is not None and existing.isdigit():
        candidate = max(candidate, int(existing))
    return str(candidate)


def _archive_period(url: str) -> str:
    """Return the exact UTC month or day encoded in an official archive filename."""
    name = url.rstrip("/").rsplit("/", 1)[-1]
    match = re.search(r"-(\d{4}-\d{2}(?:-\d{2})?)\.zip$", name)
    if match is None:
        raise ValueError(f"Cannot determine archive period from filename: {name}")
    period = match.group(1)
    # Reject impossible calendar dates instead of persisting misleading provenance.
    datetime.strptime(period, "%Y-%m-%d" if len(period) == 10 else "%Y-%m")
    return period


def _archive_month(url: str) -> str:
    """Return the YYYY-MM grouping key for either monthly or daily archives."""
    return _archive_period(url)[:7]


def _backfill_archive_klines(
    store: SQLiteStore,
    market: str,
    requested_symbols: list[str] | None,
    intervals: list[str],
    start_month: str | None,
    end_month: str | None,
) -> int:
    log = logging.getLogger("deta")
    if market == "spot":
        source = BinanceSpotAdapter()
        exchange_id, market_type = "binance", "spot"
    else:
        source = BinanceDerivativesAdapter(market)
        exchange_id, market_type = "binance", source.market_type
    archive = BinanceArchiveAdapter(market)
    try:
        try:
            discovered = source.discover_symbols()
            if discovered:
                store.save_symbols(discovered)
            else:
                log.warning("Current market discovery returned no symbols; continuing with archive indexes")
        except Exception as exc:
            log.warning("Current market discovery failed; continuing with archive indexes: %s", exc)
        stored_symbols = store.get_symbols(exchange_id, market_type)
        if requested_symbols:
            known = {str(item["symbol"]) for item in stored_symbols}
            # Explicit symbols are allowed even if delisted or absent from current exchange metadata.
            selected = [{"symbol": symbol} for symbol in requested_symbols]
            for item in stored_symbols:
                if str(item["symbol"]) in known and str(item["symbol"]) in requested_symbols:
                    selected = [entry for entry in selected if str(entry["symbol"]) != str(item["symbol"])]
                    selected.append(item)
        else:
            # The official archive index, not current exchange metadata, is the primary source
            # for historical/delisted symbols. Keep locally known symbols as a fallback union.
            known = {str(item["symbol"]): item for item in stored_symbols}
            archived = set(archive.list_archive_symbols("klines", "monthly"))
            archived.update(archive.list_archive_symbols("klines", "daily"))
            selected = [known.get(symbol, {"symbol": symbol})
                        for symbol in sorted(set(known) | archived)]

        failed = archives_verified = archives_imported = pages_saved = rows_saved = 0
        for item in selected:
            symbol = str(item["symbol"])
            for interval in intervals:
                data_type = f"klines_{interval}"
                current_archive_url: str | None = None
                try:
                    monthly_urls = archive.list_monthly_archives(
                        symbol, interval, start_month=start_month, end_month=end_month
                    )
                    daily_urls = archive.list_daily_archives(
                        "klines", symbol, interval, start_month, end_month
                    )
                    monthly_months = {_archive_month(url) for url in monthly_urls}
                    urls = sorted(monthly_urls + [
                        url for url in daily_urls if _archive_month(url) not in monthly_months
                    ])
                    if not urls:
                        existing_cursor = store.get_checkpoint(exchange_id, market_type, data_type, symbol)
                        store.update_checkpoint(
                            exchange_id, market_type, data_type, symbol, existing_cursor, "partial",
                            "No matching official monthly archives were listed for the requested range; coverage is unknown",
                        )
                        continue
                    for url in urls:
                        current_archive_url = url
                        store.upsert_archive_manifest(
                            archive_url=url, exchange_id=exchange_id, market_type=market_type,
                            data_type=data_type, symbol=symbol, archive_period=_archive_period(url),
                            status="discovered",
                        )
                        records = archive.fetch_kline_archive(url)
                        digest = archive.last_verified_checksum_sha256
                        size_bytes = archive.last_verified_size_bytes
                        if digest is None or size_bytes is None:
                            raise RuntimeError(
                                f"Archive parser returned without verified checksum metadata: {url}"
                            )
                        archives_verified += 1
                        store.update_archive_manifest(
                            url, status="verified", checksum_sha256=digest,
                            size_bytes=size_bytes, row_count=len(records), error="",
                        )
                        if not records:
                            existing_cursor = store.get_checkpoint(exchange_id, market_type, data_type, symbol)
                            store.update_checkpoint(
                                exchange_id, market_type, data_type, symbol, existing_cursor, "partial",
                                f"Official archive contained no kline rows: {url}",
                            )
                            continue
                        last = max(record.timestamp_ms for record in records)
                        cursor = _monotonic_cursor(
                            store, exchange_id, market_type, data_type, symbol, last + 1
                        )
                        inserted = store.save_archive_batch(
                            url, records, cursor=cursor, status="partial",
                            detail="Official monthly/daily archives imported with per-archive record provenance; full source coverage and gaps are not yet independently verified",
                        )
                        store.update_archive_manifest(
                            url, status="imported", imported_at=datetime.now(timezone.utc).isoformat(), row_count=len(records), error="",
                        )
                        archives_imported += 1
                        pages_saved += 1
                        rows_saved += inserted
                    log.info("%s %s: processed %s official monthly archives", symbol, interval, len(urls))
                except Exception as exc:
                    failed += 1
                    if current_archive_url is not None:
                        try:
                            store.update_archive_manifest(
                                current_archive_url, status="failed",
                                error=f"{type(exc).__name__}: {exc}",
                            )
                        except KeyError:
                            pass
                    checkpoint = store.get_checkpoint(exchange_id, market_type, data_type, symbol)
                    store.update_checkpoint(
                        exchange_id, market_type, data_type, symbol, checkpoint, "failed",
                        f"{type(exc).__name__}: {exc}",
                    )
                    log.exception("Failed official archive dataset %s %s", symbol, interval)
        print(json.dumps({
            "exchange": exchange_id,
            "market_type": market_type,
            "symbols_attempted": len(selected),
            "archives_verified": archives_verified,
            "archives_imported": archives_imported,
            "batches_saved": pages_saved,
            "new_rows_saved": rows_saved,
            "datasets_failed": failed,
            "note": "Archive ingestion does not itself prove continuous historical coverage; run reconciliation after archive import.",
        }, indent=2))
        return 1 if failed else 0
    finally:
        source.close()
        archive.close()



def _backfill_archive_funding_rates(
    store: SQLiteStore,
    market: str,
    requested_symbols: list[str] | None,
    start_month: str | None,
    end_month: str | None,
) -> int:
    log = logging.getLogger("deta")
    source = BinanceDerivativesAdapter(market)
    archive = BinanceArchiveAdapter(market)
    try:
        try:
            discovered = source.discover_symbols()
            if discovered:
                store.save_symbols(discovered)
        except Exception as exc:
            log.warning("Current contract discovery failed; continuing with archive indexes: %s", exc)
        stored_symbols = store.get_symbols("binance", source.market_type)
        if requested_symbols:
            known = {str(item["symbol"]): item for item in stored_symbols}
            selected = [known.get(symbol, {"symbol": symbol}) for symbol in requested_symbols]
        else:
            archived = set(archive.list_archive_symbols("fundingRate", "monthly"))
            known = {str(item["symbol"]): item for item in stored_symbols}
            selected = [known.get(symbol, {"symbol": symbol})
                        for symbol in sorted(set(known) | archived)]

        failed = archives_seen = batches_saved = rows_saved = 0
        for item in selected:
            symbol = str(item["symbol"])
            data_type = "funding_rate"
            current_archive_url = None
            try:
                urls = archive.list_monthly_funding_rate_archives(
                    symbol, start_month, end_month
                )
                if not urls:
                    cursor = store.get_checkpoint(
                        "binance", source.market_type, data_type, symbol
                    )
                    store.update_checkpoint(
                        "binance", source.market_type, data_type, symbol, cursor, "partial",
                        "No official monthly funding-rate archives listed; coverage is unknown",
                    )
                    continue
                for url in urls:
                    current_archive_url = url
                    store.upsert_archive_manifest(
                        archive_url=url, exchange_id="binance", market_type=source.market_type,
                        data_type=data_type, symbol=symbol, archive_period=_archive_period(url),
                        status="discovered",
                    )
                    records = archive.fetch_funding_rate_archive(url)
                    checksum = archive.last_verified_checksum_sha256
                    size_bytes = archive.last_verified_size_bytes
                    if checksum is None or size_bytes is None:
                        raise RuntimeError("Archive adapter did not expose verified checksum metadata")
                    store.update_archive_manifest(
                        url, status="verified", checksum_sha256=checksum,
                        size_bytes=size_bytes, row_count=len(records), error="",
                    )
                    archives_seen += 1
                    if not records:
                        continue
                    candidate_cursor = max(record.timestamp_ms for record in records) + 1
                    cursor = _monotonic_cursor(
                        store, "binance", source.market_type, data_type, symbol, candidate_cursor
                    )
                    inserted = store.save_archive_batch(
                        url, records, cursor=str(cursor), status="partial",
                        detail="Official funding-rate archives imported with per-archive provenance; source coverage and gaps are not independently verified",
                    )
                    store.update_archive_manifest(
                        url, status="imported", row_count=len(records),
                        imported_at=datetime.now(timezone.utc).isoformat(), error="",
                    )
                    batches_saved += 1
                    rows_saved += inserted
            except Exception as exc:
                failed += 1
                if current_archive_url is not None:
                    try:
                        store.update_archive_manifest(
                            current_archive_url, status="failed",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    except KeyError:
                        pass
                cursor = store.get_checkpoint("binance", source.market_type, data_type, symbol)
                store.update_checkpoint(
                    "binance", source.market_type, data_type, symbol, cursor, "failed",
                    f"{type(exc).__name__}: {exc}",
                )
                log.exception("Failed funding-rate archive dataset %s", symbol)
        print(json.dumps({
            "exchange": "binance",
            "market_type": source.market_type,
            "dataset": "funding_rate",
            "contracts_attempted": len(selected),
            "archives_imported": archives_seen,
            "batches_saved": batches_saved,
            "new_rows_saved": rows_saved,
            "datasets_failed": failed,
            "note": "Archive import is checksum-verified but does not itself prove complete coverage.",
        }, indent=2))
        return 1 if failed else 0
    finally:
        source.close()
        archive.close()


def _backfill_archive_price_klines(
    store: SQLiteStore,
    market: str,
    price_type: str,
    requested_symbols: list[str] | None,
    intervals: list[str],
    start_month: str | None,
    end_month: str | None,
) -> int:
    log = logging.getLogger("deta")
    source = BinanceDerivativesAdapter(market)
    archive = BinanceArchiveAdapter(market)
    type_names = {
        "markPrice": "mark_price",
        "indexPrice": "index_price",
        "premiumIndex": "premium_index",
    }
    try:
        try:
            discovered = source.discover_symbols()
            if discovered:
                store.save_symbols(discovered)
        except Exception as exc:
            log.warning("Current contract discovery failed; continuing with archive indexes: %s", exc)
        stored_symbols = store.get_symbols("binance", source.market_type)
        if requested_symbols:
            known = {str(item["symbol"]): item for item in stored_symbols}
            selected = [known.get(symbol, {"symbol": symbol}) for symbol in requested_symbols]
        else:
            folder = f"{price_type}Klines"
            archived = set(archive.list_archive_symbols(folder, "monthly"))
            archived.update(archive.list_archive_symbols(folder, "daily"))
            known = {str(item["symbol"]): item for item in stored_symbols}
            selected = [known.get(symbol, {"symbol": symbol})
                        for symbol in sorted(set(known) | archived)]

        failed = archives_seen = batches_saved = rows_saved = 0
        for item in selected:
            symbol = str(item["symbol"])
            for interval in intervals:
                data_type = f"{type_names[price_type]}_klines_{interval}"
                current_archive_url = None
                try:
                    monthly_urls = archive.list_monthly_price_kline_archives(
                        symbol, interval, price_type, start_month, end_month
                    )
                    daily_urls = archive.list_daily_archives(
                        f"{price_type}Klines", symbol, interval, start_month, end_month
                    )
                    monthly_months = {_archive_month(url) for url in monthly_urls}
                    urls = sorted(monthly_urls + [
                        url for url in daily_urls if _archive_month(url) not in monthly_months
                    ])
                    if not urls:
                        cursor = store.get_checkpoint(
                            "binance", source.market_type, data_type, symbol
                        )
                        store.update_checkpoint(
                            "binance", source.market_type, data_type, symbol, cursor, "partial",
                            "No matching official price-kline archives listed; coverage is unknown",
                        )
                        continue
                    for url in urls:
                        current_archive_url = url
                        store.upsert_archive_manifest(
                            archive_url=url, exchange_id="binance",
                            market_type=source.market_type, data_type=data_type,
                            symbol=symbol, archive_period=_archive_month(url),
                            status="discovered",
                        )
                        records = archive.fetch_price_kline_archive(url, price_type)
                        checksum = archive.last_verified_checksum_sha256
                        size_bytes = archive.last_verified_size_bytes
                        if checksum is None or size_bytes is None:
                            raise RuntimeError("Archive adapter did not expose verified checksum metadata")
                        store.update_archive_manifest(
                            url, status="verified", checksum_sha256=checksum,
                            size_bytes=size_bytes, row_count=len(records), error="",
                        )
                        archives_seen += 1
                        if not records:
                            continue
                        last = max(record.timestamp_ms for record in records)
                        cursor = _monotonic_cursor(
                            store, "binance", source.market_type, data_type, symbol, last + 1
                        )
                        inserted = store.save_archive_batch(
                            url, records, cursor=cursor, status="partial",
                            detail=f"Official {price_type} archives imported with per-archive provenance; full coverage and gaps are not independently verified",
                        )
                        store.update_archive_manifest(
                            url, status="imported", row_count=len(records),
                            imported_at=datetime.now(timezone.utc).isoformat(), error="",
                        )
                        batches_saved += 1
                        rows_saved += inserted
                except Exception as exc:
                    failed += 1
                    if current_archive_url is not None:
                        try:
                            store.update_archive_manifest(
                                current_archive_url, status="failed",
                                error=f"{type(exc).__name__}: {exc}",
                            )
                        except KeyError:
                            pass
                    cursor = store.get_checkpoint(
                        "binance", source.market_type, data_type, symbol
                    )
                    store.update_checkpoint(
                        "binance", source.market_type, data_type, symbol, cursor, "failed",
                        f"{type(exc).__name__}: {exc}",
                    )
                    log.exception("Failed %s archive dataset %s %s", price_type, symbol, interval)
        print(json.dumps({
            "exchange": "binance",
            "market_type": source.market_type,
            "price_type": price_type,
            "contracts_attempted": len(selected),
            "archives_imported": archives_seen,
            "batches_saved": batches_saved,
            "new_rows_saved": rows_saved,
            "datasets_failed": failed,
            "note": "Price-kline archive ingestion does not itself prove full historical coverage.",
        }, indent=2))
        return 1 if failed else 0
    finally:
        source.close()
        archive.close()


def _backfill_archive_trade_dataset(
    store: SQLiteStore,
    market: str,
    requested_symbols: list[str] | None,
    start_month: str | None,
    end_month: str | None,
    dataset: Literal["trades", "agg_trades"],
) -> int:
    log = logging.getLogger("deta")
    if market == "spot":
        source = BinanceSpotAdapter()
        market_type = "spot"
    else:
        source = BinanceDerivativesAdapter(market)
        market_type = source.market_type
    archive = BinanceArchiveAdapter(market)
    try:
        try:
            discovered = source.discover_symbols()
            if discovered:
                store.save_symbols(discovered)
            else:
                log.warning("Current market discovery returned no symbols; continuing with archive indexes")
        except Exception as exc:
            log.warning("Current market discovery failed; continuing with archive indexes: %s", exc)
        stored_symbols = store.get_symbols("binance", market_type)
        if requested_symbols:
            by_symbol = {str(item["symbol"]): item for item in stored_symbols}
            selected = [by_symbol.get(symbol, {"symbol": symbol}) for symbol in requested_symbols]
        else:
            archive_dataset = "aggTrades" if dataset == "agg_trades" else "trades"
            known = {str(item["symbol"]): item for item in stored_symbols}
            archived = set(archive.list_archive_symbols(archive_dataset, "monthly"))
            archived.update(archive.list_archive_symbols(archive_dataset, "daily"))
            selected = [known.get(symbol, {"symbol": symbol})
                        for symbol in sorted(set(known) | archived)]

        failed = archives_seen = batches_saved = rows_saved = 0
        for item in selected:
            symbol = str(item["symbol"])
            current_archive_url = None
            try:
                archive_dataset = "aggTrades" if dataset == "agg_trades" else "trades"
                if dataset == "agg_trades":
                    monthly_urls = archive.list_monthly_agg_trade_archives(
                        symbol, start_month=start_month, end_month=end_month
                    )
                    fetch_archive = archive.fetch_agg_trade_archive
                    description = "aggregate-trade"
                else:
                    monthly_urls = archive.list_monthly_trade_archives(
                        symbol, start_month=start_month, end_month=end_month
                    )
                    fetch_archive = archive.fetch_trade_archive
                    description = "individual-trade"
                daily_urls = archive.list_daily_archives(
                    archive_dataset, symbol, start_month=start_month, end_month=end_month
                )
                monthly_months = {_archive_month(url) for url in monthly_urls}
                urls = sorted(monthly_urls + [
                    url for url in daily_urls if _archive_month(url) not in monthly_months
                ])
                if not urls:
                    cursor = store.get_checkpoint("binance", market_type, dataset, symbol)
                    store.update_checkpoint(
                        "binance", market_type, dataset, symbol, cursor, "partial",
                        f"No matching official monthly {description} archives were listed; coverage is unknown",
                    )
                    continue
                for url in urls:
                    current_archive_url = url
                    store.upsert_archive_manifest(
                        archive_url=url, exchange_id="binance", market_type=market_type,
                        data_type=dataset, symbol=symbol, archive_period=_archive_month(url),
                        status="discovered",
                    )
                    records = fetch_archive(url)
                    checksum = archive.last_verified_checksum_sha256
                    size_bytes = archive.last_verified_size_bytes
                    if checksum is None or size_bytes is None:
                        raise RuntimeError("Archive adapter did not expose verified checksum metadata")
                    store.update_archive_manifest(
                        url, status="verified", checksum_sha256=checksum,
                        size_bytes=size_bytes, row_count=len(records), error="",
                    )
                    archives_seen += 1
                    if not records:
                        continue
                    candidate_cursor = max(int(record.record_key) for record in records) + 1
                    cursor = _monotonic_cursor(
                        store, "binance", market_type, dataset, symbol, candidate_cursor
                    )
                    inserted = store.save_archive_batch(
                        url, records, cursor=cursor, status="partial",
                        detail=f"Official {description} archives imported with per-archive provenance; source coverage and gaps are not yet independently verified",
                    )
                    store.update_archive_manifest(
                        url, status="imported", row_count=len(records),
                        imported_at=datetime.now(timezone.utc).isoformat(), error="",
                    )
                    batches_saved += 1
                    rows_saved += inserted
                log.info("%s %s: imported %s official archives", symbol, description, len(urls))
            except Exception as exc:
                failed += 1
                if current_archive_url is not None:
                    try:
                        store.update_archive_manifest(
                            current_archive_url, status="failed",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    except KeyError:
                        pass
                cursor = store.get_checkpoint("binance", market_type, dataset, symbol)
                store.update_checkpoint(
                    "binance", market_type, dataset, symbol, cursor, "failed",
                    f"{type(exc).__name__}: {exc}",
                )
                log.exception("Failed %s archive dataset %s", dataset, symbol)
        print(json.dumps({
            "exchange": "binance",
            "market_type": market_type,
            "dataset": dataset,
            "symbols_attempted": len(selected),
            "archives_imported": archives_seen,
            "batches_saved": batches_saved,
            "new_rows_saved": rows_saved,
            "datasets_failed": failed,
            "note": "Archive import does not prove complete coverage; checksum verification and gap reconciliation remain separate verification steps.",
        }, indent=2))
        return 1 if failed else 0
    finally:
        source.close()
        archive.close()


def _backfill_archive_agg_trades(
    store: SQLiteStore,
    market: str,
    requested_symbols: list[str] | None,
    start_month: str | None,
    end_month: str | None,
) -> int:
    return _backfill_archive_trade_dataset(
        store, market, requested_symbols, start_month, end_month, "agg_trades"
    )


def _backfill_archive_trades(
    store: SQLiteStore,
    market: str,
    requested_symbols: list[str] | None,
    start_month: str | None,
    end_month: str | None,
) -> int:
    return _backfill_archive_trade_dataset(
        store, market, requested_symbols, start_month, end_month, "trades"
    )



def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("deta")
    with SQLiteStore(args.db) as store:
        if args.command == "status":
            print(json.dumps(store.dataset_summary(), indent=2, ensure_ascii=False))
            return 0
        if args.command == "audit-kline-gaps":
            interval_ms = {
                "1m": 60_000, "3m": 180_000, "5m": 300_000,
                "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
                "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
                "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
                "3d": 259_200_000,
            }[args.interval]
            if args.start_ms < 0 or args.end_ms < args.start_ms:
                log.error("Invalid audit range: require 0 <= start-ms <= end-ms")
                return 2
            if args.start_ms % interval_ms or args.end_ms % interval_ms:
                log.error("Range endpoints must align to UTC interval boundaries for this interval")
                return 2
            expected_count = ((args.end_ms - args.start_ms) // interval_ms) + 1
            if expected_count > 2_000_000:
                log.error("Refusing audit range above 2,000,000 expected candles; split it into smaller ranges")
                return 2
            data_type = f"klines_{args.interval}"
            observed = store.record_timestamps(
                exchange_id="binance", market_type=args.market_type,
                data_type=data_type, symbol=args.symbol,
                start_ms=args.start_ms, end_ms=args.end_ms,
            )
            observed_set = set(observed)
            expected = range(args.start_ms, args.end_ms + 1, interval_ms)
            missing = [timestamp for timestamp in expected if timestamp not in observed_set]
            unexpected = [timestamp for timestamp in observed if (timestamp - args.start_ms) % interval_ms != 0]
            print(json.dumps({
                "exchange": "binance", "market_type": args.market_type,
                "symbol": args.symbol, "data_type": data_type,
                "requested_range_ms": {"start_inclusive": args.start_ms, "end_inclusive": args.end_ms},
                "interval_ms": interval_ms, "expected_candles": expected_count,
                "stored_distinct_timestamps": len(observed),
                "missing_candle_count": len(missing), "first_missing_timestamps_ms": missing[:100],
                "unexpected_off_grid_timestamps": unexpected[:100],
                "first_stored_timestamp_ms": observed[0] if observed else None,
                "last_stored_timestamp_ms": observed[-1] if observed else None,
                "coverage_claim": "This checks timestamp continuity only within the explicit range and stored database. It does not prove source/archive completeness or validate candle payload contents.",
            }, indent=2, ensure_ascii=False))
            return 1 if missing or unexpected else 0
        if args.command == "archive-manifest":
            entries = store.archive_manifest_summary(
                exchange_id="binance",
                market_type=args.market_type,
                data_type=args.data_type,
                symbol=args.symbol,
            )
            counts: dict[str, int] = {}
            for entry in entries:
                status = str(entry["status"])
                counts[status] = counts.get(status, 0) + 1
            print(json.dumps({
                "exchange": "binance",
                "filters": {
                    "market_type": args.market_type,
                    "data_type": args.data_type,
                    "symbol": args.symbol,
                },
                "archive_count": len(entries),
                "status_counts": counts,
                "archives": entries,
                "coverage_claim": "Manifest state is audit evidence, not proof of exhaustive source coverage.",
            }, indent=2, ensure_ascii=False))
            return 0
        if args.command == "reconcile-archive-klines":
            archive = BinanceArchiveAdapter(args.market)
            try:
                monthly = archive.list_monthly_archives(
                    args.symbol, args.interval, args.start_month, args.end_month
                )
                daily = archive.list_daily_archives(
                    "klines", args.symbol, args.interval, args.start_month, args.end_month
                )
                monthly_periods = {_archive_month(url) for url in monthly}
                source_urls = sorted(set(monthly + [
                    url for url in daily if _archive_month(url) not in monthly_periods
                ]))
                market_type = archive.market_type
                data_type = f"klines_{args.interval}"
                manifest = store.archive_manifest_summary(
                    exchange_id="binance", market_type=market_type,
                    data_type=data_type, symbol=args.symbol,
                )
                by_url = {str(entry["archive_url"]): entry for entry in manifest}
                listed = set(source_urls)
                # Manifest periods may represent either a whole month (YYYY-MM) or a
                # daily archive (YYYY-MM-DD). Compare by month so daily entries are not
                # accidentally excluded by lexicographic comparison against YYYY-MM bounds.
                scoped_manifest = [
                    entry for entry in manifest
                    if args.start_month <= str(entry["archive_period"])[:7] <= args.end_month
                ]
                missing_manifest = [url for url in source_urls if url not in by_url]
                verified_not_imported = [url for url in source_urls
                    if url in by_url and by_url[url]["status"] == "verified"]
                failed = [url for url in source_urls
                    if url in by_url and by_url[url]["status"] == "failed"]
                imported = [url for url in source_urls
                    if url in by_url and by_url[url]["status"] == "imported"]
                discovered_or_downloaded = [url for url in source_urls
                    if url in by_url and by_url[url]["status"] in {"discovered", "downloaded"}]
                missing_checksum = [url for url in source_urls
                    if url in by_url and (
                        not isinstance(by_url[url]["checksum_sha256"], str)
                        or len(str(by_url[url]["checksum_sha256"])) != 64
                        or any(char not in "0123456789abcdefABCDEF"
                               for char in str(by_url[url]["checksum_sha256"]))
                        or not isinstance(by_url[url]["size_bytes"], int)
                        or by_url[url]["size_bytes"] <= 0
                    )]
                missing_import_timestamp = [
                    url for url in imported
                    if not isinstance(by_url[url]["imported_at"], str)
                    or not by_url[url]["imported_at"].strip()
                ]
                # Verify the exact archive-to-record provenance, not just timestamps
                # that might have originated from REST or another overlapping archive.
                imported_row_count_mismatches: list[dict[str, object]] = []
                for url in imported:
                    entry = by_url[url]
                    provenance = store.archive_provenance_summary(url)
                    manifest_count = entry["row_count"]
                    if (
                        not isinstance(manifest_count, int)
                        or manifest_count <= 0
                        or provenance["provenance_rows"] != manifest_count
                        or provenance["stored_record_rows"] != manifest_count
                        or provenance["timestamp_mismatches"] != 0
                        or provenance["payload_hash_mismatches"] != 0
                    ):
                        imported_row_count_mismatches.append({
                            "archive_url": url,
                            "archive_period": entry["archive_period"],
                            "manifest_row_count": manifest_count,
                            "archive_provenance_rows": provenance["provenance_rows"],
                            "stored_record_rows_with_provenance": provenance["stored_record_rows"],
                            "timestamp_mismatches": provenance["timestamp_mismatches"],
                            "payload_hash_mismatches": provenance["payload_hash_mismatches"],
                        })
                manifest_not_listed = [
                    str(entry["archive_url"]) for entry in scoped_manifest
                    if str(entry["archive_url"]) not in listed
                ]
                print(json.dumps({
                    "exchange": "binance", "market": args.market,
                    "market_type": market_type, "symbol": args.symbol,
                    "data_type": data_type,
                    "requested_months": {"start": args.start_month, "end": args.end_month},
                    "source_listed_archive_count": len(source_urls),
                    "manifest_entries_in_scope": len(scoped_manifest),
                    "missing_from_manifest": missing_manifest,
                    "verified_but_not_imported": verified_not_imported,
                    "failed_archives": failed,
                    "imported_archives": imported,
                    "discovered_or_downloaded": discovered_or_downloaded,
                    "missing_checksum_or_size": missing_checksum,
                    "missing_import_timestamp": missing_import_timestamp,
                    "imported_row_count_mismatches": imported_row_count_mismatches,
                    "manifest_entries_not_in_current_source_listing": manifest_not_listed,
                    "coverage_claim": (
                        "This reconciles the current official monthly/daily source listing against the local manifest only. "
                        "It cannot prove that the source listing itself is exhaustive or that imported rows cover every expected timestamp."
                    ),
                }, indent=2, ensure_ascii=False))
                # Fail closed for direct CLI callers as well as the live-smoke workflow.
                # A non-empty source listing is not sufficient if the manifest disagrees.
                reconciliation_passed = (
                    bool(source_urls)
                    and not missing_manifest
                    and not verified_not_imported
                    and not failed
                    and not discovered_or_downloaded
                    and not missing_checksum
                    and not missing_import_timestamp
                    and not imported_row_count_mismatches
                    and not manifest_not_listed
                    and len(imported) == len(source_urls)
                )
                return 0 if reconciliation_passed else 1
            finally:
                archive.close()
        if args.command == "backfill-archive-klines":
            return _backfill_archive_klines(
                store, args.market, args.symbols, args.intervals, args.start_month, args.end_month
            )
        if args.command == "backfill-archive-agg-trades":
            return _backfill_archive_agg_trades(
                store, args.market, args.symbols, args.start_month, args.end_month
            )
        if args.command == "backfill-archive-trades":
            return _backfill_archive_trades(
                store, args.market, args.symbols, args.start_month, args.end_month
            )
        if args.command == "backfill-archive-funding-rates":
            return _backfill_archive_funding_rates(
                store, args.market, args.symbols, args.start_month, args.end_month
            )
        if args.command == "backfill-archive-price-klines":
            return _backfill_archive_price_klines(
                store, args.market, args.price_type, args.symbols, args.intervals,
                args.start_month, args.end_month,
            )
        if args.command == "backfill-funding-rates":
            if args.start_ms < 0 or (
                args.end_ms is not None and args.end_ms < args.start_ms
            ):
                log.error("Invalid funding-rate time range")
                return 2
            adapter = BinanceDerivativesAdapter(args.market)
            try:
                store.save_symbols(adapter.discover_symbols())
                symbols = store.get_symbols("binance", adapter.market_type)
                if args.symbols:
                    wanted = set(args.symbols)
                    known = {str(item["symbol"]): item for item in symbols}
                    symbols = [known.get(symbol, {"symbol": symbol}) for symbol in sorted(wanted)]
                failed = pages_saved = rows_saved = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    data_type = "funding_rate"
                    checkpoint = store.get_checkpoint(
                        "binance", adapter.market_type, data_type, symbol
                    )
                    cursor = max(args.start_ms, int(checkpoint)) if checkpoint is not None else args.start_ms
                    pages_for_symbol = 0
                    try:
                        for page in adapter.iter_funding_rates(symbol, cursor, args.end_ms):
                            last = max(record.timestamp_ms for record in page)
                            inserted = store.save_batch(
                                page, cursor=str(last + 1), status="partial",
                                detail="Funding-rate history saved; full source coverage not independently verified",
                            )
                            pages_saved += 1
                            pages_for_symbol += 1
                            rows_saved += inserted
                        if pages_for_symbol == 0:
                            store.update_checkpoint(
                                "binance", adapter.market_type, data_type, symbol,
                                checkpoint, "partial",
                                "No rows returned for requested range; availability is unverified",
                            )
                    except Exception as exc:
                        failed += 1
                        store.update_checkpoint(
                            "binance", adapter.market_type, data_type, symbol,
                            str(cursor) if cursor else checkpoint, "failed",
                            f"{type(exc).__name__}: {exc}",
                        )
                        log.exception("Failed funding-rate dataset %s", symbol)
                print(json.dumps({
                    "exchange": "binance",
                    "market_type": adapter.market_type,
                    "contracts_attempted": len(symbols),
                    "pages_saved": pages_saved,
                    "new_rows_saved": rows_saved,
                    "datasets_failed": failed,
                    "note": "Successful completion does not prove full historical coverage.",
                }, indent=2))
                return 1 if failed else 0
            finally:
                adapter.close()
        if args.command in ("discover-derivatives", "backfill-derivative-klines"):
            adapter = BinanceDerivativesAdapter(args.market)
            try:
                if args.command == "discover-derivatives":
                    symbols = adapter.discover_symbols()
                    store.save_symbols(symbols)
                    print(json.dumps({
                        "exchange": "binance", "market_type": adapter.market_type,
                        "contracts_discovered": len(symbols),
                        "trading_contracts": sum(s.get("status") == "TRADING" for s in symbols),
                        "contract_types": sorted({str(s.get("contract_type")) for s in symbols}),
                    }, indent=2))
                    return 0
                discovered = adapter.discover_symbols()
                store.save_symbols(discovered)
                symbols = store.get_symbols("binance", adapter.market_type)
                wanted = set(args.symbols) if args.symbols else None
                if wanted is not None:
                    symbols = [item for item in symbols if item["symbol"] in wanted]
                if not args.include_inactive:
                    symbols = [
                        item for item in symbols
                        if item.get("status") == "TRADING" and item.get("is_currently_listed", True)
                    ]
                failed = pages_saved = rows_saved = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    for interval in args.intervals:
                        data_type = f"klines_{interval}"
                        checkpoint = store.get_checkpoint(
                            "binance", adapter.market_type, data_type, symbol
                        )
                        cursor = int(checkpoint) if checkpoint is not None else 0
                        try:
                            for page in adapter.iter_klines(symbol, interval, cursor):
                                last = max(record.timestamp_ms for record in page)
                                inserted = store.save_batch(
                                    page, cursor=str(last + 1), status="partial",
                                    detail="Native Futures candles saved; historical completeness not independently verified",
                                )
                                pages_saved += 1
                                rows_saved += inserted
                                cursor = last + 1
                            if cursor == 0:
                                store.update_checkpoint(
                                    "binance", adapter.market_type, data_type, symbol,
                                    checkpoint, "partial", "No rows returned; earliest availability not proven",
                                )
                        except Exception as exc:
                            failed += 1
                            store.update_checkpoint(
                                "binance", adapter.market_type, data_type, symbol,
                                str(cursor) if cursor else checkpoint, "failed",
                                f"{type(exc).__name__}: {exc}",
                            )
                            log.exception("Failed Futures dataset %s %s", symbol, interval)
                print(json.dumps({
                    "exchange": "binance", "market_type": adapter.market_type,
                    "contracts_attempted": len(symbols), "pages_saved": pages_saved,
                    "new_rows_saved": rows_saved, "datasets_failed": failed,
                    "note": "Successful completion does not prove full historical coverage.",
                }, indent=2))
                return 1 if failed else 0
            finally:
                adapter.close()
        adapter = BinanceSpotAdapter()
        try:
            if args.command == "discover":
                symbols = adapter.discover_symbols()
                store.save_symbols(symbols)
                print(json.dumps({
                    "exchange": "binance", "market_type": "spot",
                    "symbols_discovered": len(symbols),
                    "trading_symbols": sum(s.get("status") == "TRADING" for s in symbols),
                }, indent=2))
                return 0
            if args.command == "backfill-klines":
                symbols = _select_symbols(store, adapter, args.symbols, args.include_inactive)
                failed = pages_saved = rows_saved = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    for interval in args.intervals:
                        data_type = f"klines_{interval}"
                        checkpoint = store.get_checkpoint("binance", "spot", data_type, symbol)
                        cursor = int(checkpoint) if checkpoint is not None else 0
                        try:
                            for page in adapter.iter_klines(symbol, interval, cursor):
                                last = max(record.timestamp_ms for record in page)
                                inserted = store.save_batch(
                                    page, cursor=str(last + 1), status="partial",
                                    detail="Paged data saved; historical coverage has not yet been independently verified",
                                )
                                pages_saved += 1
                                rows_saved += inserted
                                cursor = last + 1
                            if cursor == 0:
                                store.update_checkpoint(
                                    "binance", "spot", data_type, symbol, checkpoint,
                                    "partial", "No rows returned; earliest availability not proven",
                                )
                            log.info("%s %s: backfill pass finished", symbol, interval)
                        except Exception as exc:
                            failed += 1
                            store.update_checkpoint(
                                "binance", "spot", data_type, symbol,
                                str(cursor) if cursor else checkpoint, "failed",
                                f"{type(exc).__name__}: {exc}",
                            )
                            log.exception("Failed dataset %s %s", symbol, interval)
                print(json.dumps({
                    "exchange": "binance", "market_type": "spot",
                    "symbols_attempted": len(symbols), "pages_saved": pages_saved,
                    "new_rows_saved": rows_saved, "datasets_failed": failed,
                    "note": "A successful run is not proof of complete historical coverage.",
                }, indent=2))
                return 1 if failed else 0
            if args.command == "backfill-agg-trades":
                if args.start_ms < 0 or (args.end_ms is not None and args.end_ms < args.start_ms):
                    log.error("Invalid aggregate-trade time range")
                    return 2
                symbols = _select_symbols(store, adapter, args.symbols, args.include_inactive)
                failed = pages_saved = rows_saved = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    data_type = "agg_trades"
                    checkpoint = store.get_checkpoint("binance", "spot", data_type, symbol)
                    cursor_id = int(checkpoint) if checkpoint is not None else None
                    symbol_pages = 0
                    try:
                        for page in adapter.iter_aggregate_trades(
                            symbol, args.start_ms, args.end_ms, cursor_id
                        ):
                            last_id = max(int(record.record_key) for record in page)
                            cursor_id = last_id + 1
                            inserted = store.save_batch(
                                page, cursor=str(cursor_id), status="partial",
                                detail="Aggregate trades paged and persisted; historical coverage has not been independently verified",
                            )
                            pages_saved += 1
                            symbol_pages += 1
                            rows_saved += inserted
                        if symbol_pages == 0 and checkpoint is None:
                            store.update_checkpoint(
                                "binance", "spot", data_type, symbol, None,
                                "partial", "No rows returned; earliest availability not proven",
                            )
                        log.info("%s aggregate trades: backfill pass finished", symbol)
                    except Exception as exc:
                        failed += 1
                        store.update_checkpoint(
                            "binance", "spot", data_type, symbol,
                            str(cursor_id) if cursor_id is not None else checkpoint,
                            "failed", f"{type(exc).__name__}: {exc}",
                        )
                        log.exception("Failed aggregate-trade dataset %s", symbol)
                print(json.dumps({
                    "exchange": "binance", "market_type": "spot",
                    "symbols_attempted": len(symbols), "pages_saved": pages_saved,
                    "new_rows_saved": rows_saved, "datasets_failed": failed,
                    "note": "A successful run is not proof of complete historical coverage.",
                }, indent=2))
                return 1 if failed else 0
        finally:
            adapter.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
