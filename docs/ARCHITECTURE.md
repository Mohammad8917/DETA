# DETA Architecture and Contracts

## 1. Runtime layers

- `deta/domain/`: framework-independent models and invariants: exchange identity, market identity, dataset status, time ranges, coverage, and provenance.
- `deta/application/`: orchestration: discovery, backfill planning, resumable jobs, live collection, retries, reconciliation, and coverage reporting.
- `deta/exchanges/<exchange_id>/`: one adapter package per exchange. It owns that exchange's public endpoint behavior, market discovery, supported dataset capabilities, pagination, rate-limit policy, and source-specific normalization.
- `deta/storage/`: durable raw and normalized writers, atomic checkpoint/manifest updates, and partition layout.
- `deta/cli/`: human entry points for inspect, plan, backfill, resume, verify, and continuous collection.
- `tests/`: domain tests, application tests, adapter contract tests, storage tests, and opt-in network integration tests.
- `docs/`: contracts, supported-source evidence, operations, and known limitations.

Dependencies point inward: exchange adapters and storage implement interfaces consumed by the application; domain code must not import exchange SDKs, storage engines, CLI code, or network clients.

## 2. Exchange adapter contract

Every adapter must expose, through a common typed interface:

1. stable exchange ID and public documentation/source references;
2. public market discovery, including spot and every other market type actually supported;
3. per-market symbol discovery without a curated symbol allowlist;
4. an explicit capability report for each data type;
5. historical earliest/latest availability where the source can establish it;
6. paginated historical retrieval with deterministic cursors and duplicate-safe boundaries;
7. public live-stream or polling support when available;
8. source-aware rate limiting, retry classification, and backoff;
9. normalization that preserves source timestamps, identifiers, and provenance;
10. clear distinction among unsupported, unavailable, empty, partial, complete, and failed results.

No adapter may silently report success after skipping a page, symbol, interval, or data type. Unsupported capabilities must be recorded rather than represented by fake data.

## 3. Collection workflow

1. Discover configured exchanges and public capabilities.
2. Enumerate all discoverable market types and symbols; retain delisted symbols when the source exposes historical metadata or archives.
3. Probe data availability independently for each exchange / market / symbol / data type.
4. Create resumable jobs using the oldest proven available point, not a global hard-coded start date.
5. Fetch pages with source-specific limits and persist data before advancing checkpoints.
6. Reconcile expected ranges, timestamps, page boundaries, duplicates, and source gaps.
7. Store a machine-readable manifest for every dataset and a global coverage report.
8. Resume incomplete work after process restart.
9. After backfill, continue collecting new public data where supported; periodic discovery detects new and changed symbols.
10. Run periodic verification and retry recoverable gaps without overwriting verified data blindly.

## 4. Storage partitioning

Logical partition key:

`exchange_id / market_type / data_type / symbol / time_partition`

A representative layout is:

`data/<exchange_id>/<market_type>/<data_type>/<symbol>/<year>/<month>/...`

Raw source payloads and normalized datasets must be distinguishable. Dataset manifests record source, schema/version, first and last observed timestamps, row count, download/checkpoint state, gaps, validation result, and known limitations. File format and partition granularity are selected based on workload and validated size/performance tests, not guessed in advance.

## 5. Data types to investigate

Capabilities are discovered and verified per exchange. Candidate public datasets include:

- market and symbol metadata, status changes, contract specifications;
- public trades and aggregated trades;
- OHLCV candles for all intervals available or safely derivable from complete source trades;
- ticker and public price/volume snapshots;
- live order-book updates and current depth snapshots;
- historical order-book snapshots, only where an archive actually exists;
- funding rates, mark/index prices, open interest, and futures statistics;
- public liquidation streams/history where exposed;
- exchange status and public market events where documented.

Derived candles must be labelled as derived and must not be presented as exchange-native candles. Missing historical order-book states cannot be reconstructed from candles or trades.

## 6. Operational guarantees

- Checkpoint advancement occurs only after durable persistence and validation.
- Retries are bounded per attempt and resumable across process restarts.
- HTTP/network errors, rate limits, malformed payloads, and unsupported endpoints are classified separately.
- Concurrency is bounded globally and per exchange.
- Secrets are not needed for public endpoints and must not be committed.
- Storage exhaustion and disk errors stop affected jobs safely and visibly.
- Continuous collection requires a continuously running host or an explicitly configured scheduler.
- Logs and reports include enough context to diagnose a failure without logging credentials.

## 7. Completion criteria

A dataset is not considered complete merely because a command exited successfully. Completion requires a source-supported range, exhausted pagination or archive coverage, successful persistence, validation, and an explicit manifest status. Where source coverage cannot be proven, the status remains partial or unknown.

## 8. Implementation order

1. Repository tooling, configuration schema, typed domain contracts, and deterministic tests.
2. Storage layout, atomic manifests/checkpoints, and resume behavior.
3. One reference adapter with verified public endpoints and adapter contract tests.
4. Historical backfill, reconciliation, and coverage reporting.
5. Live collection and periodic rediscovery.
6. Add and validate remaining adapters one at a time.
7. Performance, fault-injection, long-run, and disk-capacity tests.

No exchange is considered operational until its adapter tests and documented capability evidence pass.
