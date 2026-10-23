# DETA — Public Exchange Data Archive

DETA is intended to discover and collect publicly available cryptocurrency market data, from the oldest history each source actually exposes through ongoing updates. It must never fabricate records or claim full coverage without evidence.

## Current implementation status

The first runnable slice supports **Binance Spot** symbol discovery, historical native klines, and page-streamed aggregate trades through Binance's public REST API. Data is persisted locally in SQLite with per-dataset checkpoints. Other exchanges and additional data types are not implemented yet. This is **not** a complete ten-exchange collector.

## Requirements

- Python 3.13 or newer
- Network access to public exchange APIs
- Sufficient local disk space; exhaustive trade history can become very large

Install for development:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Refresh Binance Spot symbol metadata:

```powershell
python -m deta.cli discover
```

Backfill candles for all currently trading discovered Spot symbols at selected intervals:

```powershell
python -m deta.cli backfill-klines --intervals 1m 5m 15m 1h 4h 1d
```

Backfill aggregate trades for selected symbols (or all current trading symbols if omitted):

```powershell
python -m deta.cli backfill-agg-trades --symbols BTCUSDT ETHUSDT
```

The aggregate-trade command starts from the earliest timestamp requested by `--start-ms` (default 0), resumes from saved aggregate trade IDs, and can be bounded using `--end-ms`. A checkpoint proves progress, not that the exchange exposed every historical trade.

Check saved coverage and per-dataset state:

```powershell
python -m deta.cli status
```

Use `--db PATH` before the command to select another SQLite database. Use a small `--symbols` list before attempting broad backfills.

## Important limitations

- Public endpoints expose different datasets and different history windows. DETA can only collect data that an exchange makes accessible; no software can recover data the source never published or no longer serves.
- A successful process exit is not proof of historical completeness. Dataset status remains partial/failed unless coverage is independently established; there is no false guarantee that all old data exists.
- The collector refreshes current symbol metadata before each backfill. Symbols missing from the latest discovery are retained as historical metadata but marked not currently listed, so they are not silently treated as active symbols.
- This first slice is **not yet a continuous live collector**, and does not yet support ten exchanges, derivatives, historical order books, funding/open-interest/liquidation archives, or delisted-symbol trade archives.
- Historical ticker snapshots and historical order books generally cannot be reconstructed from current snapshots.
- SQLite is an initial local store, not the final high-throughput format for a multi-terabyte archive.
- No private account credentials, wallet access, or order-placement functionality is used.

See `docs/ARCHITECTURE.md` for the contracts and implementation sequence.
