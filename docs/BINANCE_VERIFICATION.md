# Binance Verification and Lock Ledger

This file is the source of truth for which Binance DETA behaviours are frozen and which remain unproven. It does not claim that the full Binance historical dataset is complete.

## Lock policy

- A behaviour may be marked **LOCKED** only after its contract is explicit, tests cover success and failure cases, and the relevant CI run is green.
- Locked behaviour must not be redesigned, renamed, removed, or silently changed. A change requires an explicit request or a documented correctness/security defect, regression tests, and a separate review of the lock.
- A green unit-test run is not evidence of live-source compatibility or complete historical coverage.
- Never label a dataset complete merely because an import command succeeded or a checkpoint advanced.
- Keep DETA limited to historical public-data collection, validation, storage, reconciliation, and coverage reporting. No market analysis, signals, or trading execution.

## Current evidence snapshot

Repository revision assessed for this evidence snapshot: `b074f4ee0aa159747e35484d8a220354bea07d87` (2026-10-24).

The following recent GitHub Actions runs completed successfully:
- `40053280269` — verifies that archive counters distinguish checksum-verified archives from archives actually imported.
- `40053269891` — separates verified and imported archive counters in the importer.
- `40052871433` — verifies that an empty but checksum-verified archive remains `verified`, with zero rows and no import timestamp.
- `40052419835` — verification-ledger update and manifest audit changes.

These are unit-test/CI evidence only. The manual live workflow has not been executed; these runs are not live Binance end-to-end evidence and do not prove additional source coverage.

## Locked contracts — current scope only

The following contracts are frozen at the **unit-tested implementation level**. This does not imply exhaustive historical coverage.

1. **Market identity remains distinct**
   - Spot, USDⓈ-M Futures, and COIN-M Futures must not be conflated.
   - Perpetual and delivery contracts must retain their distinct source metadata where available.
2. **Record identity and durable checkpoint contract**
   - Stored records use dataset identity and a record key for duplicate-safe inserts.
   - A non-empty batch and its checkpoint are committed in the same SQLite transaction.
   - Archive re-import must not move a numeric checkpoint backwards.
3. **Archive integrity**
   - An archive is not parsed unless its SHA-256 checksum sidecar verifies.
   - A checksum mismatch is an error, not a warning or successful import.
4. **Timestamp provenance**
   - Spot archive timestamps from 2025 onward may be microseconds; normalization to storage milliseconds must preserve the original timestamp and unit in payload provenance.
5. **Dataset separation**
   - Native klines, aggregate trades, individual trades, funding rates, and Futures mark/index/premium-price klines remain distinct dataset types.
   - Mark/index/premium-price candles must not be mislabeled as native OHLCV candles or assigned invented volume.
6. **Honest status**
   - Import success must not be reported as proof of continuous history.
   - Missing, unsupported, failed, partial, and verified-complete coverage must remain distinguishable.

These contracts may be changed only with an explicit, reviewed change and updated tests. A source-specific correction must preserve the contract unless evidence shows the contract itself is wrong.

## Newly verified at unit-test level

- **S3 archive-listing fallback:** the regression test for falling back from an HTML directory page with no usable archive links to an S3 `ListObjectsV2` response passed in CI at `439dfdf`. This verifies the mocked fallback path and symbol-prefix parsing only; it does not prove live endpoint availability, full pagination behaviour against Binance, or exhaustive historical symbol discovery.
- **Manifest lifecycle — unit-test level:** deterministic success-path tests cover native klines, funding-rate archives, individual trades, and Futures price-klines. The funding-rate lifecycle test initially failed because its fake adapter did not accept the market argument; the test fixture was corrected in commit `06b6037`, and the current CI run passed. These tests prove mocked lifecycle behaviour only, not live Binance coverage. Current code is connected; independent full coverage remains unproven.
- **Manifest audit CLI — unit-test level:** `archive-manifest` reports persisted manifest rows/status counts with optional market/data-type/symbol filters and explicitly warns that manifest state is not proof of exhaustive source coverage. Tests and the workflow artifact update passed CI. The manual live workflow definition has been checked against the CLI parser, but it has not been dispatched in this evidence snapshot.
- **Verified-vs-imported accounting — unit-test level:** `archives_verified` increments after checksum-verified archive parsing; `archives_imported` increments only after non-empty records are persisted and the manifest is marked imported. An empty archive remains `verified` and does not inflate the imported counter. Commits `d1eb7994` and `b074f4ee`; CI runs `40053269891` and `40053280269` passed.

## Implemented but NOT yet locked as proven

The code contains initial implementations for:
- Binance Spot, USDⓈ-M Futures, and COIN-M Futures historical klines.
- Official archive parsing for klines, aggregate trades, individual trades, Futures funding-rate archives, and Futures mark/index/premium-price kline archives.
- Monthly and daily archive selection, with daily archives used when no monthly archive is listed for that month.
- Public Futures funding-rate REST retrieval.
- Archive symbol discovery intended to include historical/delisted symbols.

These implementations are not yet proven end-to-end against the live source across all relevant market/dataset combinations.

## Explicit blockers before Binance can be fully locked

1. **Live archive-index verification:** run the current index-discovery and archive-listing code against the real official source. Unit fixtures currently model HTML anchor listings; this must not be mistaken for proof that the live listing endpoint exposes the same anchors. If the source listing format differs, correct discovery before relying on it.
2. **Persistent archive manifest — implementation underway:** SQLite manifest storage exists, and unit-tested lifecycle integration now covers native klines, funding-rate archives, individual trades, and Futures mark/index/premium-price klines. Each path records discovered/verified/imported/failed state with checksum, size, row count, import timestamp, and errors. This is not yet proof that every edge case or source family is covered; empty verified archives currently remain verified rather than imported, and restart/resume behaviour still needs dedicated tests.
3. **Source inventory and coverage report:** enumerate the source-supported datasets for Spot, USDⓈ-M, and COIN-M separately, and report unsupported/unavailable categories rather than assuming support.
4. **Gap reconciliation:** compare archive inventory and imported records, detect missing/failed archive files and suspicious timestamp/identifier gaps, and distinguish true missing data from legitimate periods without trades.
5. **Real integration tests:** verify at least representative historical and recent files for each supported archive family, including checksum verification, CSV schema, timestamp units, and persistence.
6. **Recovery and scale tests:** demonstrate interruption/restart correctness and measure storage/disk use on representative data volumes. SQLite has not been proven for exhaustive multi-terabyte raw-trade history.
7. **Final lock review:** freeze each independently proven dataset family only after its evidence, tests, and coverage limitations are recorded here.

## Coverage labels

- **IMPLEMENTED:** code path exists; completeness not implied.
- **UNIT-VERIFIED:** deterministic tests pass; live source behaviour not implied.
- **LIVE-VERIFIED:** representative real source requests and files have been checked.
- **COVERAGE-VERIFIED:** archive/source inventory and stored data have been reconciled over an explicitly stated range.
- **LOCKED:** contract and evidence have been reviewed and frozen.
- **PARTIAL / UNAVAILABLE / FAILED:** must be reported accurately; never upgraded to complete without evidence.

Update this ledger in the same change that upgrades any dataset family to LIVE-VERIFIED, COVERAGE-VERIFIED, or LOCKED.
