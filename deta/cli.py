"""Command-line entry points for DETA."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Sequence

from deta.exchanges.binance import BinanceSpotAdapter
from deta.storage import SQLiteStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="deta", description="Public exchange market-data collector")
    parser.add_argument("--db", type=Path, default=Path("data/deta.sqlite3"),
                        help="SQLite database path")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="Discover and persist Binance Spot symbols")
    candles = commands.add_parser("backfill-klines", help="Backfill Binance Spot candles")
    candles.add_argument("--intervals", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "1d"])
    candles.add_argument("--symbols", nargs="*", help="Optional symbols; default is all discovered symbols")
    candles.add_argument("--include-inactive", action="store_true")
    commands.add_parser("status", help="Show locally stored dataset coverage/status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("deta")
    with SQLiteStore(args.db) as store:
        if args.command == "status":
            print(json.dumps(store.dataset_summary(), indent=2, ensure_ascii=False))
            return 0
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
                symbols = store.get_symbols("binance", "spot")
                if not symbols:
                    symbols = adapter.discover_symbols()
                    store.save_symbols(symbols)
                wanted = set(args.symbols) if args.symbols else None
                if wanted is not None:
                    known = {str(s["symbol"]) for s in symbols}
                    unknown = wanted - known
                    if unknown:
                        log.warning("Requested symbols absent from current metadata: %s", sorted(unknown))
                    symbols = [s for s in symbols if s["symbol"] in wanted]
                if not args.include_inactive:
                    symbols = [s for s in symbols if s.get("status") == "TRADING"]
                total = 0
                failed = 0
                for item in symbols:
                    symbol = str(item["symbol"])
                    for interval in args.intervals:
                        data_type = f"klines_{interval}"
                        checkpoint = store.get_checkpoint("binance", "spot", data_type, symbol)
                        start_ms = int(checkpoint) if checkpoint is not None else 0
                        try:
                            records = adapter.fetch_klines(symbol, interval, start_ms)
                            if records:
                                last = max(r.timestamp_ms for r in records)
                                store.save_batch(records, cursor=str(last + 1), status="partial",
                                    detail="This run paginated to an empty/short page; full historical coverage is not independently proven")
                                total += len(records)
                            else:
                                store.update_checkpoint("binance", "spot", data_type, symbol,
                                    checkpoint, "partial", "No rows returned; earliest availability not proven")
                            log.info("%s %s: %d rows", symbol, interval, len(records))
                        except Exception as exc:
                            failed += 1
                            store.update_checkpoint("binance", "spot", data_type, symbol,
                                checkpoint, "failed", f"{type(exc).__name__}: {exc}")
                            log.exception("Failed dataset %s %s", symbol, interval)
                print(json.dumps({"exchange": "binance", "market_type": "spot",
                    "symbols_attempted": len(symbols), "rows_received": total,
                    "datasets_failed": failed,
                    "note": "A successful run is not proof of complete historical coverage."}, indent=2))
                return 1 if failed else 0
        finally:
            adapter.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
