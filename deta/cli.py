"""Command-line entry points for DETA historical collection."""

from __future__ import annotations

import argparse
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


def _archive_month(url: str) -> str:
    name = url.rstrip("/").rsplit("/", 1)[-1]
    match = re.search(r"-(\d{4}-\d{2})(?:-\d{2})?\.zip$", name)
    if match is None:
        raise ValueError(f"Cannot determine archive month from filename: {name}")
    return match.group(1)


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
        discovered = source.discover_symbols()
        store.save_symbols(discovered)
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

        failed = archives_seen = pages_saved = rows_saved = 0
        for item in selected:
            symbol = str(item["symbol"])
            for interval in intervals:
                data_type = f"klines_{interval}"
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
                        records = archive.fetch_kline_archive(url)
                        archives_seen += 1
                        if not records:
                            existing_cursor = store.get_checkpoint(exchange_id, market_type, data_type, symbol)
                            store.update_checkpoint(
                                exchange_id, market_type, data_type, symbol, existing_cursor, "partial",
                                f"Official archive contained no kline rows: {url}",
                            )
                            continue
                        last = max(record.timestamp_ms for record in records)
                        inserted = store.save_batch(
                            records, cursor=str(last + 1), status="partial",
                            detail="Official monthly archives imported; full source coverage and gaps are not yet independently verified",
                        )
                        pages_saved += 1
                        rows_saved += inserted
                    log.info("%s %s: processed %s official monthly archives", symbol, interval, len(urls))
                except Exception as exc:
                    failed += 1
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
            "archives_imported": archives_seen,
            "batches_saved": pages_saved,
            "new_rows_saved": rows_saved,
            "datasets_failed": failed,
            "note": "Archive ingestion does not itself prove continuous historical coverage; run reconciliation after archive import.",
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
        store.save_symbols(source.discover_symbols())
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
                    records = fetch_archive(url)
                    archives_seen += 1
                    if not records:
                        continue
                    if dataset == "agg_trades":
                        cursor = str(max(int(record.record_key) for record in records) + 1)
                    else:
                        cursor = str(max(int(record.record_key) for record in records) + 1)
                    inserted = store.save_batch(
                        records, cursor=cursor, status="partial",
                        detail=f"Official {description} archives imported; source coverage and gaps are not yet independently verified",
                    )
                    batches_saved += 1
                    rows_saved += inserted
                log.info("%s %s: imported %s official archives", symbol, description, len(urls))
            except Exception as exc:
                failed += 1
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
