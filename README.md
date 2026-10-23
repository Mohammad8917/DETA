# DETA — Public Exchange Data Archive

DETA aims to collect publicly accessible cryptocurrency market data from the oldest history each source actually exposes through ongoing updates. It must never fabricate records or claim complete coverage without evidence.

## Current implementation status

Implemented initial slices:
- Binance Spot: public symbol discovery, native klines, and page-streamed aggregate trades.
- Binance USDⓈ-M Futures: public contract discovery and native historical klines.
- Binance COIN-M Futures: public contract discovery and native historical klines, including contract metadata that distinguishes perpetual and delivery contract types.
- SQLite persistence with per-dataset checkpoints.

This is not yet a complete archive, ten-exchange collector, continuous live collector, or automated trading system. Futures trades and other derivatives datasets are not implemented by the futures adapter yet.

## Requirements

- Python 3.13 or newer
- Network access to public exchange APIs
- Sufficient local disk space; exhaustive trade history can become very large

Install:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Discover Binance Spot symbols:

```powershell
python -m deta.cli discover
```

Backfill Spot candles:

```powershell
python -m deta.cli backfill-klines --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSDT ETHUSDT
```

Backfill Spot aggregate trades:

```powershell
python -m deta.cli backfill-agg-trades --symbols BTCUSDT ETHUSDT
```

Discover Binance futures contracts (USDⓈ-M and COIN-M):

```powershell
python -m deta.cli discover-derivatives --market usd-m
python -m deta.cli discover-derivatives --market coin-m
```

Backfill futures candles:

```powershell
python -m deta.cli backfill-derivative-klines --market usd-m --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSDT ETHUSDT
python -m deta.cli backfill-derivative-klines --market coin-m --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSD_PERP
```

Check stored dataset state:

```powershell
python -m deta.cli status
```

Use `--db PATH` before the command to select another SQLite database. Start with a small symbol list before broad backfills.

## Scope and limitations

The target scope includes Spot, perpetual futures, and non-perpetual/delivery futures; historical and live public data; and a separate automated-trading capability. Automated trading is **not implemented yet** and no order is placed by the current code. A real trading engine must be a separately tested module with paper mode by default, explicit opt-in for live mode, strict risk limits, and credentials supplied through local environment/secret storage—not committed to Git.

Public endpoints expose different datasets and history windows. “All historical data without exception” cannot be guaranteed literally: DETA can only collect data that the source publishes and still makes accessible. Historical order books, snapshots, liquidation records, and delisted-contract data must be marked unavailable if no source archive exists. The project should aim for exhaustive coverage of every verified public dataset and record explicit gaps rather than invent data.

SQLite is the current initial store, not a proven high-throughput format for a multi-terabyte archive. This implementation is not yet a continuous live collector or a complete ten-exchange system.

See `docs/ARCHITECTURE.md` for the implementation contracts.
