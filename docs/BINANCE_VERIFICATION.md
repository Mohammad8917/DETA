# Binance Verification and Lock Ledger

This file is the source of truth for which Binance DETA behaviours are frozen and which remain unproven. It does not claim that the full Binance historical dataset is complete.

## Lock policy

- A behaviour may be marked **LOCKED** only after its contract is explicit, tests cover success and failure cases, and the relevant CI run is green.
- Locked behaviour must not be redesigned, renamed, removed, or silently changed. A change requires an explicit request or a documented correctness/security defect, regression tests, and a separate review of the lock.
- A green unit-test run is not evidence of live-source compatibility or complete historical coverage.
- Never label a dataset complete merely because an import command succeeded or a checkpoint advanced.
- Keep DETA limited to historical public-data collection, validation, storage, reconciliation, and coverage reporting. No market analysis, signals, or trading execution.

## Latest verification update — 2026-10-24

- Revision that introduced the live-smoke reconciliation gate: `7c4d46a30407b63dc9ac395870dc1377ad301c98`.
- Added a fail-closed gate to `.github/workflows/binance-live-smoke.yml`: after importing the requested one-month Spot/USD-M/COIN-M native 1-minute kline scope, the workflow compares the official source listing to the persisted archive Manifest and fails if source-listed archives are absent from the Manifest, verified-but-not-imported, failed, still discovered/downloaded, missing checksum/size, or inconsistent with the imported count. It also rejects an empty source listing.
- Unit-test workflow run [40076813519](https://github.com/Mohammad8917/DETA/actions/runs/40076813519) completed successfully for this revision.
- **Important limitation:** run 40076813519 was triggered by a push and ran the unit-test workflow. It did **not** execute `binance-live-smoke.yml`. The live smoke workflow remains undispatched, so there is no new live-source evidence and no two-year/all-symbol coverage claim.
- This gate is intentionally narrow: it checks archive listing/Manifest reconciliation for one market, symbol, interval, and month. It does not prove source inventory exhaustiveness, row-level candle correctness, timestamp continuity, or coverage of trades/funding/price-index datasets.

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

## Bounded source-to-manifest reconciliation — unit-verified

- The `reconcile-archive-klines` CLI compares the currently listed official monthly/daily kline archive URLs for one market, symbol, interval, and inclusive month range against persisted Manifest rows.
- It separately reports source-listed URLs missing from the Manifest, rows marked verified but not imported, failed/discovered/imported entries, missing checksum/size metadata, and scoped Manifest URLs absent from the current source listing.
- A deterministic mocked regression test covers a two-month range with one verified-but-not-imported archive and one source-listed archive missing from the Manifest.
- Commit `39203844` fixes a parser/test-adapter coupling issue; CI run [40062644650](https://github.com/Mohammad8917/DETA/actions/runs/40062644650) passed.
- **Evidence level: UNIT-VERIFIED only.** The workflow above was triggered by a push, not by the manual live smoke workflow. No live source-to-Manifest reconciliation has yet been recorded. This report compares archive URLs only; it does not validate stored candle timestamp continuity or prove that the source listing is exhaustive.

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

## Bounded stored-kline timestamp gap audit — unit-verified

- The `audit-kline-gaps` CLI checks distinct stored timestamps for one Binance market, symbol, interval, and explicitly bounded inclusive UTC-millisecond range. It computes expected candle-open timestamps, reports missing timestamps and off-grid observations, and rejects invalid or excessively large ranges.
- Storage lookup is scoped to the requested dataset and range and returns timestamps in ascending order. Supported market identifiers match the persisted DETA values: `spot`, `usd_m`, and `coin_m`.
- Regression tests cover a missing 1-minute candle and rejection of a non-aligned range. The initial validation test failed because it inspected stderr rather than pytest's captured logger; commit `19c3552e` corrected the test to assert captured logging. CI run [40070504672](https://github.com/Mohammad8917/DETA/actions/runs/40070504672) passed.
- **Evidence level: UNIT-VERIFIED only.** This audit proves timestamp continuity only for records already stored in the selected database and bounded range. It does not establish that Binance supplied every expected candle, validate OHLCV payload correctness, or prove historical archive completeness. No live audit result is claimed here.

## Closed-candle REST filtering — unit-verified (2026-10-24)

- Spot and USDⓈ-M/COIN-M Futures REST kline iterators filter rows whose exchange-provided close timestamp has not yet passed at iterator start. This prevents the currently forming candle from being emitted as finalized historical data.
- Regression tests cover exclusion of a future/open candle in both Spot and Futures adapters.
- Commits: `49aaf186bf65cda63750ad55be29ddd46f655127` (Spot adapter), `510ec729cff1e0f8d448567cb38a14150cf6485a` (derivatives adapter), `d2274df46ec2ce5559f0ef46c0e070696462fb9f` (Spot regression test), and `669f2505192a79294189e35be6b25f3a2ce78826` (Futures regression test).
- GitHub Actions runs [40073462170](https://github.com/Mohammad8917/DETA/actions/runs/40073462170), [40073464925](https://github.com/Mohammad8917/DETA/actions/runs/40073464925), [40073477994](https://github.com/Mohammad8917/DETA/actions/runs/40073477994), and [40073481018](https://github.com/Mohammad8917/DETA/actions/runs/40073481018) all completed successfully.
- **Evidence level: UNIT-VERIFIED only.** The tests do not validate Binance server-clock skew, all interval boundaries, pagination/resume edge cases, or completeness of any stored historical range. The implementation captures local wall-clock time once at iterator start; a live integration check and checkpoint-resume regression remain outstanding.

## Required full-dataset coverage accounting — not yet complete

The user's scope is the complete set of supported historical data families and their results, not candles alone. Progress must be tracked separately for each market and dataset family, without inferring coverage from a green CI run or from another dataset's status:

- Spot: native klines, aggregate trades, and individual trades.
- USDⓈ-M Futures: native klines, aggregate trades, individual trades, funding rates, mark-price klines, index-price klines, and premium-index klines.
- COIN-M Futures: the same families only where the official source actually provides them; unsupported or unavailable combinations must be reported explicitly, not marked complete.
- For every applicable family, the coverage report must distinguish source inventory, archives listed, archives checksum-verified, archives imported, records stored, first/last timestamp or trade ID, missing/failed archives, timestamp/identifier gaps, checkpoint/resume status, and evidence level. For trade datasets, gaps must not be treated as missing data solely because no trades occurred during a time interval.
- A complete two-year result is not established until the relevant source inventory and local persisted data have been reconciled for the explicitly stated symbols/contracts, intervals, and date range. Current CI is unit-test evidence and does not constitute that reconciliation.
## Archive-to-database provenance gate — implemented, execution pending (2026-10-24)

- Archive-backed imports for native klines, funding rates, price klines, aggregate trades, and individual trades now use one transactional storage path that writes records, per-archive record provenance, and checkpoint state together.
- Before importing, the storage layer requires a registered archive in a verified/imported state, valid SHA-256 and positive byte-size metadata, an exact match between parsed record count and manifest row count, matching dataset identity, and timestamps within the declared UTC month or day.
- Daily archive dates are retained as YYYY-MM-DD; they are not collapsed into a monthly label for per-archive period validation.
- Reconciliation checks provenance row count, existence of every referenced stored record, stored timestamps, and canonical payload fingerprints. A manifest entry marked imported without corresponding records, or with altered payload/timestamp data, fails closed.
- A changed archive checksum invalidates the prior import timestamp and deletes provenance for the old content version. Re-importing checksum-identical content remains idempotent.
- Regression tests were added for missing records, altered payloads/timestamps, row-count mismatch, out-of-period daily records, checksum-change invalidation, and daily period parsing.
- Evidence level: IMPLEMENTED; execution pending. These changes have been fetched back from GitHub and inspected, but the available commit-status/workflow-run queries did not return a CI result. Do not label these tests UNIT-VERIFIED until a real test run succeeds.
- This gate proves the stored records match the records parsed from each imported archive. It does not by itself prove the source inventory is exhaustive, OHLCV values are semantically correct, or the complete two-year scope is covered.
