# DETA — Exchange Data Archive

DETA is a public-market-data collection system designed to discover and collect the broadest practical set of publicly accessible data from 10 cryptocurrency exchanges, with exchange-specific storage and resumable historical backfills.

## Project objective

- Discover all symbols and market types exposed by each configured exchange.
- Collect the longest history actually available for each supported public data type; there is no fixed start-date cutoff.
- Continue collecting new data after historical backfills, where the runtime environment is kept running.
- Keep each exchange, market type, data type, and symbol logically separated in storage.
- Track progress, failures, gaps, coverage, and source limitations explicitly.
- Resume safely after interruption without silently treating missing data as complete.

## Important limits

“Collect everything” means maximizing coverage of data that is publicly exposed and technically retrievable. It cannot guarantee recovery of information an exchange never published, has deleted, restricts, or does not provide through its public endpoints. Historical order-book snapshots and liquidation history, for example, may not be available for every exchange or symbol. Each dataset must report its actual coverage and limitations.

Only public market-data interfaces are in scope. Private account access, API keys, wallets, order placement, and automated trading are out of scope.

## Design principles

1. **Exchange isolation:** each exchange adapter has its own directory and tests.
2. **Data-type isolation:** trades, candles, order books, funding, open interest, liquidations, metadata, and other supported datasets remain distinguishable.
3. **No hidden truncation:** no hard-coded symbol allowlist or arbitrary historical start date.
4. **Resumability:** checkpoints and manifests make progress durable and restartable.
5. **Rate-limit respect:** use public documented endpoints, backoff, bounded concurrency, and exchange-specific limits.
6. **Evidence-based status:** discovered, unsupported, unavailable, partial, complete, and failed are distinct states.
7. **Data integrity:** preserve raw source records where possible; normalized data must retain provenance and timestamps.
8. **Testability:** unit tests, adapter contract tests, and integration tests must verify behavior rather than merely import modules.

## Planned exchange adapters

The first implementation milestone will verify public endpoints, historical archive availability, supported markets, rate limits, and licensing/terms before enabling each adapter. The ten exchanges are a configurable set; an adapter must not be marked supported until its behavior is implemented and tested.

## Runtime

GitHub is used for source control, documentation, and automated tests. Large historical downloads and continuous collection should run on a machine or server with sufficient storage and stable connectivity; GitHub Actions is not intended to be the long-running data warehouse.

## Status

Initial repository scope and engineering constraints are documented. Collection functionality is not claimed to exist until implemented and tested.
