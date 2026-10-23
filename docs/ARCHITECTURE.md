# DETA Architecture and Contracts

## 1. Runtime layers

- `deta/domain.py`: framework-independent records, market identity, statuses, and invariants.
- `deta/application/` (planned): orchestration for discovery, backfill planning, resumable jobs, live collection, retries, reconciliation, and coverage reporting.
- `deta/exchanges/<exchange_id>/`: one adapter package per exchange. It owns market discovery, public capabilities, pagination, rate-limit policy, and source-specific normalization.
- `deta/storage.py`: durable raw records, checkpoints, symbol metadata, and dataset reports.
- `deta/cli.py`: human entry points for discovery, backfill, resume, and status.
- `tests/`: domain, adapter, storage, and fault-injection tests.

Dependencies point inward: adapters and storage implement interfaces consumed by application logic; domain code must not import network clients or CLI code.

## 2. Market coverage

Market types are distinct storage identities and must never be merged:
- Spot.
- USDⓈ-M futures, including perpetual and dated/delivery contracts.
- COIN-M futures, including perpetual and dated/delivery contracts.
- Additional exchanges' spot, linear derivatives, inverse derivatives, options, and other public markets only where the exchange exposes them.

A contract's type, settlement/margin asset, delivery date, listing state, and native symbol must be retained. “Perpetual” and “delivery/non-perpetual” contracts must not be conflated.

## 3. Exchange adapter contract

Every adapter must expose:
1. stable exchange ID and public documentation references;
2. market-type and symbol discovery without a curated symbol allowlist;
3. an explicit capability report per data type;
4. earliest/latest historical availability where provable;
5. paginated historical retrieval with deterministic cursors and duplicate-safe boundaries;
6. public live-stream or polling support where available;
7. source-aware rate limiting, retries, and backoff;
8. normalization that preserves source timestamps, identifiers, and provenance;
9. explicit distinctions among unsupported, unavailable, empty, partial, complete, and failed.

No adapter may silently report success after skipping a page, symbol, interval, or data type. Unsupported capabilities must be recorded rather than represented by fake data.

## 4. Required data inventory

Investigate and implement every publicly available data category for each exchange/market separately:
- all current and historically discoverable symbols/contracts and metadata;
- native candles for every supported interval;
- individual trades and aggregate trades;
- ticker/price/volume snapshots and book-ticker snapshots;
- current order-book snapshots and public incremental order-book streams;
- historical order-book snapshots only when an actual archive exists;
- funding rates, mark/index price candles, open interest, basis, and derivatives statistics;
- public liquidation streams/history where exposed;
- exchange status and public market events;
- source archives/bulk downloads when available.

Native data and derived data must be labelled distinctly. Derived candles may be produced from complete trades, but cannot replace native candles without provenance. Missing historical order-book states cannot be reconstructed from candles or trades.

## 5. Automated trading is a separate subsystem

The trading subsystem is not part of the current collector and must not be represented as implemented until it exists and passes tests. Required safety contracts before live execution:
- paper/simulation mode by default;
- no live order placement unless the operator explicitly enables it;
- credentials only through local environment variables or a secret manager, never in source or logs;
- exchange-specific precision, tick size, lot size, minimum notional, leverage, margin mode, and contract rules validated before an order;
- pre-trade risk checks, position/exposure limits, daily loss limits, stale-data checks, idempotent client order IDs, and duplicate-order prevention;
- order/position reconciliation after timeouts and restarts;
- kill switch and auditable order lifecycle;
- separate permissions and tests for Spot, USDⓈ-M, and COIN-M.

## 6. Storage and completeness

Logical partition key: `exchange_id / market_type / data_type / symbol / time_partition`.

Checkpoint advancement occurs only after durable persistence and validation. Dataset manifests should record source, schema version, earliest/latest observed timestamps, expected/actual rows where estimable, missing ranges, retries, status, and known limitations. A successful process exit is not proof of historical completeness.

“Complete” means complete over a verified source-supported range. If a source never exposed a dataset, no longer serves it, or has undocumented gaps, the project must report that limitation; it must never claim literal recovery of unavailable information.

## 7. Implementation sequence

1. Domain/storage contracts and deterministic tests.
2. Spot historical data with checkpoint/reconciliation.
3. USDⓈ-M and COIN-M metadata and historical data.
4. Additional public datasets and live streams.
5. Additional exchange adapters, one at a time with evidence and tests.
6. Paper trading, then gated live trading only after independent safety tests.
7. Long-run, fault-injection, recovery, and storage-capacity tests.
