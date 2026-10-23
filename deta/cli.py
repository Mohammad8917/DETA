"""Command-line entry points for DETA."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Sequence

from deta.exchanges.binance import BinanceSpotAdapter
from deta.exchanges.binance.derivatives import BinanceDerivativesAdapter
from deta.storage import SQLiteStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="deta", description="Public exchange market-data collector")
    parser.add_argument("--db", type=Path, default=Path("data/deta.sqlite3"), help="SQLite database path")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="Refresh Binance Spot symbol metadata")
    candles = commands.add_parser("backfill-klines", help="Backfill Binance Spot candles")
    candles.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    candles.add_argument("--symbols", nargs="*", help="Optional symbols; default is all current trading symbols")
    candles.add_argument("--include-inactive", action="store_true")
    trades = commands.add_parser("backfill-agg-trades", help="Backfill Binance Spot aggregate trades")
    trades.add_argument("--symbols", nargs="*", help="Optional symbols; default is all current trading symbols")
    trades.add_argument("--start-ms", type=int, default=0, help="Initial timestamp in Unix milliseconds")
    trades.add_argument("--end-ms", type=int, help="Optional inclusive ending timestamp in Unix milliseconds")
    trades.add_argument("--include-inactive", action="store_true")
    derivative_discover = commands.add_parser("discover-derivatives", help="Refresh Binance futures contract metadata")
    derivative_discover.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    derivative_candles = commands.add_parser("backfill-derivative-klines", help="Backfill Binance futures candles")
    derivative_candles.add_argument("--market", choices=("usd-m", "coin-m"), required=True)
    derivative_candles.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    derivative_candles.add_argument("--symbols", nargs="*", help="Optional contract symbols")
    derivative_candles.add_argument("--include-inactive", action="store_true")
    commands.add_parser("status", help="Show locally stored dataset coverage/status")
    return parser


def _select_symbols(store: SQLiteStore, adapter: BinanceSpotAdapter,
                    requested: list[str] | None, include_inactive: bool) -> list[dict[str, object]]:
    discovered = adapter.discover_symbols()
    store.save_symbols(discovered)
    symbols = store.get_symbols("binance", "spot")
    wanted = set(requested) if requested else None
    if wanted is not None:
        known = {str(item["symbol"]) for item in symbols}
        unknown = wanted - known
        if unknown:
            logging.getLogger("deta").warning(
                "Requested symbols absent from latest exchange metadata: %s", sorted(unknown))
        symbols = [item for item in symbols if item["symbol"] in wanted]
    if not include_inactive:
        symbols = [item for item in symbols if item.get("status") == "TRADING"
                   and item.get("is_currently_listed", True)]
    return symbols


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("deta")
    with SQLiteStore(args.db) as store:
        if args.command == "status":
            print(json.dumps(store.dataset_summary(), indent=2, ensure_ascii=False))
            return 0
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
                    symbols = [item for item in symbols if item.get("status") == "TRADING"
                               and item.get("is_currently_listed", True)]
                failed = pages_saved = rows_saved = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    for interval in args.intervals:
                        data_type = f"klines_{interval}"
                        checkpoint = store.get_checkpoint("binance", adapter.market_type, data_type, symbol)
                        cursor = int(checkpoint) if checkpoint is not None else 0
                        try:
                            for page in adapter.iter_klines(symbol, interval, cursor):
                                last = max(record.timestamp_ms for record in page)
                                inserted = store.save_batch(
                                    page, cursor=str(last + 1), status="partial",
                                    detail="Native futures candles saved; historical completeness not independently verified",
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
                            log.exception("Failed futures dataset %s %s", symbol, interval)
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
                print(json.dumps({"exchange": "binance", "market_type": "spot",
                    "symbols_discovered": len(symbols),
                    "trading_symbols": sum(s.get("status") == "TRADING" for s in symbols)}, indent=2))
                return 0
            if args.command == "backfill-klines":
                symbols = _select_symbols(store, adapter, args.symbols, args.include_inactive)
                failed = 0
                pages_saved = 0
                rows_saved = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    for interval in args.intervals:
                        data_type = f"klines_{interval}"
                        checkpoint = store.get_checkpoint("binance", "spot", data_type, symbol)
                        cursor = int(checkpoint) if checkpoint is not None else 0
                        try:
                            for page in adapter.iter_klines(symbol, interval, cursor):
                                last = max(record.timestamp_ms for record in page)
                                inserted = store.save_batch(page, cursor=str(last + 1), status="partial",
                                    detail="Paged data saved; historical coverage has not yet been independently verified")
                                pages_saved += 1
                                rows_saved += inserted
                                cursor = last + 1
                            if cursor == 0:
                                store.update_checkpoint("binance", "spot", data_type, symbol,
                                    checkpoint, "partial", "No rows returned; earliest availability not proven")
                            log.info("%s %s: backfill pass finished", symbol, interval)
                        except Exception as exc:
                            failed += 1
                            store.update_checkpoint("binance", "spot", data_type, symbol,
                                str(cursor) if cursor else checkpoint, "failed", f"{type(exc).__name__}: {exc}")
                            log.exception("Failed dataset %s %s", symbol, interval)
                print(json.dumps({"exchange": "binance", "market_type": "spot",
                    "symbols_attempted": len(symbols), "pages_saved": pages_saved,
                    "new_rows_saved": rows_saved, "datasets_failed": failed,
                    "note": "A successful run is not proof of complete historical coverage."}, indent=2))
                return 1 if failed else 0
            if args.command == "backfill-agg-trades":
                if args.start_ms < 0 or (args.end_ms is not None and args.end_ms < args.start_ms):
                    log.error("Invalid aggregate-trade time range")
                    return 2
                symbols = _select_symbols(store, adapter, args.symbols, args.include_inactive)
                failed = 0
                pages_saved = 0
                rows_saved = 0
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
                            inserted = store.save_batch(page, cursor=str(cursor_id), status="partial",
                                detail="Aggregate trades paged and persisted; historical coverage has not been independently verified")
                            pages_saved += 1
                            symbol_pages += 1
                            rows_saved += inserted
                        if symbol_pages == 0 and checkpoint is None:
                            store.update_checkpoint("binance", "spot", data_type, symbol, None,
                                "partial", "No rows returned; earliest availability not proven")
                        log.info("%s aggregate trades: backfill pass finished", symbol)
                    except Exception as exc:
                        failed += 1
                        store.update_checkpoint("binance", "spot", data_type, symbol,
                            str(cursor_id) if cursor_id is not None else checkpoint,
                            "failed", f"{type(exc).__name__}: {exc}")
                        log.exception("Failed aggregate-trade dataset %s", symbol)
                print(json.dumps({"exchange": "binance", "market_type": "spot",
                    "symbols_attempted": len(symbols), "pages_saved": pages_saved,
                    "new_rows_saved": rows_saved, "datasets_failed": failed,
                    "note": "A successful run is not proof of complete historical coverage."}, indent=2))
                return 1 if failed else 0
        finally:
            adapter.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
