# DETA — Historical Exchange Data Architecture

## 1. Mission and boundary

DETA is responsible for collecting, storing, validating, reconciling, and reporting historical market data published by exchanges through public APIs, official bulk archives, and other verifiable public sources.

The project is a historical data collection platform. Market analysis, forecasting, trading-signal generation, order placement, and automated trading are outside its scope and must not be added as DETA capabilities.

## 2. Runtime responsibilities

- `deta/domain.py`: framework-independent records, market identity, dataset status, and invariants.
- `deta/application/` (planned): historical discovery, backfill planning, resumable jobs, reconciliation, and coverage reporting.
- `deta/exchanges/<exchange_id>/`: exchange-specific public source discovery, capabilities, pagination, rate-limit policy, archive access, and source-aware normalization.
- `deta/storage.py`: durable historical records, checkpoints, symbol/contract metadata, and dataset reports.
- `deta/cli.py`: discovery, historical backfill, status, and verification entry points.
- `tests/`: domain, adapter, storage, pagination, recovery, and fault-injection tests.

Dependencies should point inward: adapters and storage implement contracts consumed by application logic; domain code must not depend on network clients or CLI code.

## 3. Market coverage

Market types must remain distinct storage identities:
- Spot.
- USDⓈ-M Futures, including perpetual and dated/delivery contracts.
- COIN-M Futures, including perpetual and dated/delivery contracts.
- Additional exchange market types only when public historical datasets are verified and implemented.

Preserve each source's native symbol and contract identifiers, contract type, settlement/margin asset, delivery date, listing state, and other relevant metadata where provided. Perpetual and delivery contracts must not be conflated.

## 4. Exchange-source adapter contract

Each exchange adapter should provide:
1. Stable exchange identity and official source/documentation references.
2. Discovery of available market types, symbols, contracts, and relevant historical metadata without relying solely on a manually curated symbol list.
3. A capability inventory for each historical dataset type.
4. Earliest/latest available history where the source makes this discoverable or where it can be verified.
5. Paginated historical retrieval with deterministic cursors and duplicate-safe boundaries.
6. Official bulk-archive discovery and ingestion where available.
7. Source-aware rate limiting, bounded retries, and backoff.
8. Normalization that preserves native timestamps, source identifiers, and provenance.
9. Explicit distinctions among supported, unsupported, unavailable, empty, partial, complete, and failed results.

An adapter must not silently report success after skipping a page, symbol, interval, archive file, or dataset. Unsupported or unavailable capabilities must be reported rather than represented by fabricated data.

## 5. Historical data inventory

Investigate and implement each publicly available historical data category separately for each exchange and market:
- Current and historically discoverable symbol/contract metadata.
- Native OHLCV candles for every interval exposed by the source.
- Individual trades and aggregate trades.
- Historical ticker, price, volume, and book-ticker records where provided.
- Funding rates, mark/index-price candles, open interest, basis, and derivatives statistics where provided.
- Historical order-book snapshots, incremental history, and liquidation records only where an actual accessible archive/source exists.
- Other verifiable public historical datasets exposed by the exchange.
- Official bulk downloads and archive files.

Do not describe a dataset as supported merely because another exchange offers it. Native source data and derived data must be labelled separately. Derived candles may be generated from complete underlying trades when explicitly required, but must not be represented as exchange-native candles. Historical order-book states cannot be reconstructed from candles or trades.

## 6. Historical collection workflow

1. Discover supported exchanges, market types, datasets, symbols, and contracts.
2. Establish source evidence and limitations for each dataset.
3. Determine the oldest verifiable available point independently for each source/market/symbol/dataset.
4. Plan resumable jobs across API history and official archives.
5. Retrieve data using source-specific limits and pagination.
6. Persist and validate records before advancing checkpoints.
7. Reconcile available source ranges, archive coverage, page boundaries, timestamps, duplicates, and detectable gaps.
8. Store machine-readable manifests and dataset coverage reports.
9. Resume incomplete work after process restarts.
10. Repeat verification and retry recoverable gaps without silently overwriting verified data.

## 7. Storage and provenance

Logical partition key:

`exchange_id / market_type / data_type / symbol_or_contract / time_partition`

Storage should preserve the distinction between raw source payloads and normalized records where applicable. Dataset manifests should record:
- Source and retrieval method.
- Schema/version information.
- Exchange, market, symbol/contract, and dataset identity.
- Earliest/latest observed timestamps.
- Row counts where measurable.
- Download and checkpoint state.
- Missing ranges and verification results.
- Retries, errors, and known source limitations.
- Evidence supporting the assigned coverage status.

Storage format, partition granularity, and throughput claims must be selected and validated through realistic workload and recovery tests, not assumptions.

## 8. Completeness semantics

A dataset is not complete merely because a command exits successfully or a checkpoint reaches a recent timestamp.

Completeness must be assessed over a verified source-supported range using available API and archive evidence, pagination exhaustion, persistence checks, validation, and gap reconciliation. If the source never published a dataset, no longer serves it, or has unresolvable historical gaps, that limitation must be recorded. DETA must never fabricate unavailable history or claim universal 100% coverage without evidence.

## 9. Operational reliability

- Checkpoints advance only after durable persistence and validation.
- Retries are bounded per attempt and resumable across process restarts.
- Network errors, rate limits, malformed responses, missing archive files, and unsupported datasets are classified separately.
- Concurrency is bounded according to source limits and local capacity.
- Public historical collection should not require trading permissions.
- Disk exhaustion and storage failures must stop affected work safely and visibly.
- Logs and reports must include enough context to diagnose failures without exposing secrets.
- Long-running and large-scale readiness must be supported by performance, fault-injection, recovery, and disk-capacity tests.

## 10. Implementation sequence

1. Domain and storage contracts with deterministic tests.
2. Verify and strengthen Spot historical collection, checkpointing, and reconciliation.
3. Expand USDⓈ-M and COIN-M historical coverage beyond native candles.
4. Add official archive ingestion and dataset-level gap/coverage reporting.
5. Implement additional public historical data types one by one with source evidence and tests.
6. Add additional exchange adapters individually, with capability reports and real-source verification.
7. Run long-duration, interruption/recovery, completeness, throughput, and storage-capacity tests.

No exchange or dataset should be described as fully operational until its actual source coverage and behavior have been verified.
