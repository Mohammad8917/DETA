# DETA — Historical Exchange Market Data Collection Platform

DETA is a platform for collecting, storing, validating, and completing historical market data published by cryptocurrency exchanges through their public APIs, official archives, and other verifiable public sources.

Its purpose is historical data coverage and integrity. DETA does not analyze markets, generate trading signals, predict prices, or execute trades.

## Current implementation status

Implemented initial slices:
- Binance Spot: public symbol discovery, native klines, and aggregate trades.
- Binance USDⓈ-M Futures: public contract discovery and native historical klines.
- Binance COIN-M Futures: public contract discovery and native historical klines, including perpetual and delivery contract metadata.
- SQLite persistence with per-dataset checkpoints.

The current implementation is an initial subset, not a complete historical archive. Official monthly and daily kline, aggregate-trade, and individual-trade ZIP archive discovery/import are implemented for Binance Spot, USDⓈ-M, and COIN-M. Daily archives are used for months where no monthly archive is listed, helping cover recent data before monthly publication. Archive SHA-256 sidecars are required and verified before parsing, and Spot archive timestamps expressed in microseconds are normalized to the storage millisecond unit while preserving the original source timestamp. Individual-trade archive ingestion is implemented. Funding-rate history is available through the Futures public REST adapter and official monthly funding-rate archives. Historical Futures mark-price, index-price, and premium-index candle archive import is also implemented. Comprehensive gap reconciliation and additional exchanges remain pending. Passing unit tests does not prove complete real-world historical coverage.

## Scope

The target is historical public data from these exchange market categories:
- Spot
- Perpetual Futures
- Delivery / expiry Futures

For each supported exchange, market, symbol/contract, and dataset, DETA should discover source capabilities and retrieve all historical records that are publicly published and remain accessible. The dataset inventory should include all applicable historical data types that the exchange actually exposes, not only candles.

Candidate data categories include:
- Native OHLCV candles for supported intervals
- Individual and aggregate trades
- Historical ticker, price, volume, and book-ticker records where available
- Funding rates, mark/index-price candles, open interest, basis, and derivatives statistics where available
- Contract specifications and discoverable historical symbol/contract metadata
- Historical order-book data or liquidation records only where a verifiable source/archive exists
- Official exchange bulk archives and downloadable historical files

Availability varies by exchange and dataset. A data type must not be marked supported without source evidence. Data that was never published or is no longer accessible cannot be reconstructed reliably and must be reported as a coverage limitation, never fabricated.

## Data integrity and coverage

The collector is expected to support:
- Resumable historical downloads after interruptions
- Durable checkpoints advanced only after records are persisted and validated
- Duplicate-safe pagination and persistence
- Source-aware rate limits, retries, and backoff
- Detection and reconciliation of missing historical ranges
- Preservation of exchange, market, symbol/contract, dataset type, source timestamps, identifiers, and provenance
- Dataset-level coverage reports that distinguish complete, partial, unavailable, and failed coverage based on evidence

A successful download command or process exit is not proof that the historical dataset is complete. Completeness must be evaluated against a verified source-supported range and available archive/API evidence.

## Requirements

- Python 3.13 or newer
- Network access to public exchange APIs and archives
- Sufficient local disk space; exhaustive trade history can become very large

Install on Windows:

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

Discover Binance Futures contracts:

```powershell
python -m deta.cli discover-derivatives --market usd-m
python -m deta.cli discover-derivatives --market coin-m
```

Backfill Futures candles:

```powershell
python -m deta.cli backfill-derivative-klines --market usd-m --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSDT ETHUSDT
python -m deta.cli backfill-derivative-klines --market coin-m --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSD_PERP
```

Backfill historical Futures funding rates:

```powershell
python -m deta.cli backfill-funding-rates --market usd-m --symbols BTCUSDT ETHUSDT
python -m deta.cli backfill-funding-rates --market coin-m --symbols BTCUSD_PERP
python -m deta.cli backfill-archive-funding-rates --market usd-m --symbols BTCUSDT
python -m deta.cli backfill-archive-funding-rates --market coin-m --symbols BTCUSD_PERP
```

Import Futures mark/index/premium-price candle archives:

```powershell
python -m deta.cli backfill-archive-price-klines --market usd-m --price-type markPrice --symbols BTCUSDT --intervals 1m 5m 1h 1d
python -m deta.cli backfill-archive-price-klines --market usd-m --price-type indexPrice --symbols BTCUSDT --intervals 1m 5m 1h 1d
python -m deta.cli backfill-archive-price-klines --market coin-m --price-type premiumIndex --symbols BTCUSD_PERP --intervals 1m 5m 1h 1d
```

Import all listed monthly kline archives for selected symbols/contracts (this can take substantial time and disk space):

```powershell
python -m deta.cli backfill-archive-klines --market spot --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSDT ETHUSDT
python -m deta.cli backfill-archive-klines --market usd-m --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSDT ETHUSDT
python -m deta.cli backfill-archive-klines --market coin-m --intervals 1m 5m 15m 1h 4h 1d --symbols BTCUSD_PERP
```

Use `--start-month YYYY-MM --end-month YYYY-MM` to limit archive months. If month bounds are omitted, the command imports all monthly archives listed by the source for the selected symbol/interval. For historical aggregate trades, use `backfill-archive-agg-trades` with the same `--market`, `--symbols`, and month-bound options. For individual trades, use `backfill-archive-trades`; its archive schema varies by Spot/USDⓈ-M/COIN-M, and DETA preserves the source-specific quantity field. Example: `python -m deta.cli backfill-archive-agg-trades --market spot --symbols BTCUSDT --start-month 2020-01 --end-month 2020-12`. Without `--symbols`, these commands enumerate symbol directories from the official archive index and union them with locally known symbols/contracts, helping include delisted historical symbols that current exchange metadata omits. Archive imports are idempotent at the record key and require a matching SHA-256 sidecar. Comprehensive coverage verification and gap repair remain pending; archive-index symbol discovery is implemented but still requires real-source coverage validation.

Check stored dataset state:

```powershell
python -m deta.cli status
```

Use `--db PATH` before the command to select another SQLite database. Start with a small symbol list before broad backfills.

## Storage note

SQLite is the current initial store, not a proven high-throughput format for a multi-terabyte archive. Storage technology and partitioning must be evaluated with realistic volume, throughput, recovery, and disk-capacity tests before claiming large-scale readiness.

## Architectural reference

See `docs/ARCHITECTURE.md` for the historical collection contracts, source-adapter expectations, data inventory, completeness semantics, and implementation sequence.
