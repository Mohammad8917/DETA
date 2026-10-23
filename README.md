# DETA — Public Exchange Data Archive

DETA is intended to discover and collect publicly available cryptocurrency market data, from the oldest history each source actually exposes through ongoing updates. It must never fabricate records or claim full coverage without evidence.

## Current implementation status

The first runnable slice supports **Binance Spot** public symbol discovery and historical native klines through Binance's public REST API, with local SQLite persistence and per-dataset checkpoints. The adapter also contains an aggregate-trades retrieval method, but it is not yet wired into the CLI or covered by live integration tests. Other exchanges and additional public data types are not implemented yet. This is **not** a complete ten-exchange collector.

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

Discover current Binance Spot symbols:

```powershell
python -m deta.cli discover
```

Backfill candles for every currently trading discovered Spot symbol at selected intervals:

```powershell
python -m deta.cli backfill-klines --intervals 1m 5m 15m 1h 4h 1d
```

Check saved coverage and per-dataset state:

```powershell
python -m deta.cli status
```

Use `--db PATH` before the command to select another SQLite database. Use `--symbols BTCUSDT ETHUSDT` to test a small subset first.

## Important limitations

- Public endpoints expose different datasets and different history windows. DETA can only collect data that an exchange makes accessible; no software can recover data the source never published or no longer serves.
- A successful process exit is not proof of historical completeness. Status remains partial/failed unless coverage is established; there is no false guarantee that all old data exists.
- This first slice is **not yet a continuous live collector**, does not yet backfill all trade history, and does not yet support ten exchanges, derivatives, historical order books, funding/open-interest/liquidation archives, or delisted-symbol archives.
- Historical ticker snapshots and historical order books generally cannot be reconstructed from current snapshots.
- SQLite is an initial local store, not the final high-throughput format for a multi-terabyte archive.
- No private account credentials, wallet access, or order-placement functionality is used.

See `docs/ARCHITECTURE.md` for the contracts and implementation sequence.
