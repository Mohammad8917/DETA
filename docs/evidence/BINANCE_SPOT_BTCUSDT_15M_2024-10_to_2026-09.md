# Bounded live Binance two-year coverage proof

Date recorded: 2026-10-25

## Evidence

- GitHub Actions run: https://github.com/Mohammad8917/DETA/actions/runs/40138793698
- Workflow: `.github/workflows/binance-two-year-coverage.yml`
- Run revision: `65fd92fe5259ce2ac219353af29f7836cc5dc6f2`
- Scope: Binance Spot / BTCUSDT / native 15-minute klines / inclusive UTC months 2024-10 through 2026-09 (24 calendar months).

## Results reported by the successful run

- Official source-listed archives: 24
- Imported archives: 24
- Expected candle-open timestamps: 70,080
- Stored timestamp rows: 70,080
- Distinct timestamps: 70,080
- Missing timestamps: 0
- Duplicate timestamps: 0
- Off-grid timestamps: 0
- Invalid OHLCV payloads: 0
- Manifest entries in imported state: 24
- Imported provenance audit: passed; zero failures
- Artifact: https://github.com/Mohammad8917/DETA/actions/runs/40138793698/artifacts/12577203350

## Report-field correction

The reconciliation summary in this historical run emitted `interval: null`, even though the workflow input and candle audit used `15m`. PR #2 added `interval: args.interval` to the CLI report and a regression assertion for `interval` and `data_type`; PR #2 was merged as commit `6716d3f4fb486b9c0499695345294a4d38bdc2d6`. This corrects future reports and does not modify the historical artifact.

PR: https://github.com/Mohammad8917/DETA/pull/2

## Evidence boundary

This is bounded live source-to-Manifest reconciliation and stored-data audit evidence for Spot/BTCUSDT/15m over the exact 24-month period above. It does not establish coverage for other symbols, intervals, USD-M or COIN-M markets, aggregate trades, individual trades, funding rates, mark/index/premium-price datasets, or source inventory beyond what the official listing returned during the run. Do not infer full-project completeness from this result.

The workflow artifact is retained for a limited period; download it if long-term evidence retention is required.
