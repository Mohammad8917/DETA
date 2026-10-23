"""Framework-independent data contracts used by DETA."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class DatasetStatus(StrEnum):
    UNKNOWN = "unknown"
    PARTIAL = "partial"
    COMPLETE = "complete"
    UNSUPPORTED = "unsupported"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MarketSymbol:
    exchange_id: str
    market_type: str
    symbol: str
    base_asset: str | None = None
    quote_asset: str | None = None
    status: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DataRecord:
    exchange_id: str
    market_type: str
    data_type: str
    symbol: str
    timestamp_ms: int
    record_key: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DatasetReport:
    exchange_id: str
    market_type: str
    data_type: str
    symbol: str
    status: DatasetStatus
    rows_written: int
    first_timestamp_ms: int | None
    last_timestamp_ms: int | None
    detail: str
