# Contributing to DETA

Thank you for helping improve DETA, a historical public exchange-data collection project.

## Project boundaries

- Keep DETA focused on collecting, validating, persisting, and reporting historical public market data.
- Do not add trading execution, wallet access, market predictions, or signal generation.
- Do not claim a dataset is complete unless the claim is backed by verifiable source inventory and coverage evidence.
- Do not label a source or dataset as supported without a reproducible test or source verification.
- Preserve exchange, market type, symbol/contract, dataset type, source timestamps, and provenance.
- Avoid unrelated refactors and preserve established contracts unless a change is required and explicitly documented.

## Development setup

Use Python 3.13 or newer:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
pytest
ruff check .
mypy
```

Run the relevant tests before submitting changes. If a change relies on an external exchange endpoint or archive, add mocked regression tests and clearly distinguish those tests from successful live-source verification.

## Evidence and reporting

In pull requests and bug reports, include:
- The exact command or test used.
- Expected versus observed behavior.
- Exchange, market, symbol, dataset type, and relevant time range where applicable.
- Whether the evidence is unit-tested, live-source-verified, or coverage-verified.
- Sanitized logs and reproducible steps; never include credentials or private information.

## Scope of contributions

Prefer focused changes with tests over broad redesigns. Document limitations and failure modes. A passing test suite proves only the behaviors exercised by those tests; it does not prove complete historical coverage, uninterrupted availability of third-party sources, or multi-terabyte performance.
