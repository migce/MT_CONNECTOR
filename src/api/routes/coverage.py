"""
REST endpoint: ``/api/v1/coverage``

Data coverage statistics - for each symbol x timeframe and ticks:
earliest bar, latest bar, total count, and sync_state metadata.
"""

from __future__ import annotations

import asyncio
from datetime import datetime  # noqa: TC003 - Pydantic resolves this type at runtime
from typing import Annotated, Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from src.config import get_settings
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["coverage"])

_full_coverage_task: asyncio.Task[
    tuple[list[dict[str, Any]], list[dict[str, Any]]]
] | None = None
_full_coverage_lock = asyncio.Lock()


# ---------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------

class TimeframeCoverage(BaseModel):
    timeframe: str
    first_bar: datetime | None = None
    last_bar: datetime | None = None
    total_bars: int = 0
    last_synced_at: datetime | None = None

    model_config = {"from_attributes": True}


class TickCoverage(BaseModel):
    first_tick: datetime | None = None
    last_tick: datetime | None = None
    total_ticks: int = 0
    last_synced_at: datetime | None = None

    model_config = {"from_attributes": True}


class SymbolCoverage(BaseModel):
    symbol: str
    candles: list[TimeframeCoverage] = Field(default_factory=list)
    ticks: TickCoverage = Field(default_factory=TickCoverage)


class CoverageSummary(BaseModel):
    note: str = (
        "Coverage shows data currently loaded in the database. "
        "Additional historical data can be fetched on-demand via "
        "/api/v1/candles/{symbol}?from=...&to=... or "
        "POST /api/v1/backfill for explicit range preload."
    )
    total_candle_rows: int = 0
    total_tick_rows: int = 0
    symbols: list[SymbolCoverage] = Field(default_factory=list)
    configured_symbols: list[str] = Field(default_factory=list)
    available_symbols: list[str] = Field(default_factory=list)


async def _load_full_coverage_stats() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Coalesce concurrent full scans without serving stale row counts."""
    global _full_coverage_task

    async with _full_coverage_lock:
        task = _full_coverage_task
        if task is None:
            async def _query() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
                # Keep the two large aggregates sequential. Running them
                # concurrently increases peak DB/cache pressure.
                candle_stats = await repo.query_candle_coverage()
                tick_stats = await repo.query_tick_coverage()
                return candle_stats, tick_stats

            task = asyncio.create_task(_query())
            _full_coverage_task = task

    try:
        return await asyncio.shield(task)
    finally:
        if task.done():
            async with _full_coverage_lock:
                if _full_coverage_task is task:
                    _full_coverage_task = None


def _reset_coverage_cache() -> None:
    """Reset single-flight state in tests."""
    global _full_coverage_task
    if _full_coverage_task is not None and not _full_coverage_task.done():
        _full_coverage_task.cancel()
    _full_coverage_task = None


# ---------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------

@router.get(
    "/coverage",
    response_model=CoverageSummary,
    summary="Data coverage statistics",
    description=(
        "Returns per-symbol, per-timeframe data availability: "
        "first/last bar timestamps, total row counts, and sync state. "
        "Use this to understand what historical data is loaded."
    ),
)
async def get_coverage(
    include_counts: bool = True,
    include_ticks: bool = True,
    timeframes: Annotated[list[str] | None, Query()] = None,
) -> CoverageSummary:
    normalized_timeframes = (
        sorted({value.strip().upper() for value in timeframes if value.strip()})
        if timeframes
        else None
    )
    if include_counts:
        candle_stats, tick_stats = await _load_full_coverage_stats()
        if normalized_timeframes is not None:
            allowed = set(normalized_timeframes)
            candle_stats = [row for row in candle_stats if str(row["timeframe"]).upper() in allowed]
        if not include_ticks:
            tick_stats = []
    else:
        candle_stats = await repo.query_candle_bounds(normalized_timeframes)
        tick_stats = await repo.query_tick_bounds() if include_ticks else []

    sync_states = await repo.query_all_sync_states()
    settings = get_settings()

    # Index sync states for fast lookup
    sync_map: dict[tuple[str, str], datetime] = {}
    for s in sync_states:
        sync_map[(s["symbol"], s["data_type"])] = s["last_synced_at"]

    def _latest_timestamp(
        aggregate_value: datetime | None,
        synced_value: datetime | None,
    ) -> datetime | None:
        if aggregate_value is None:
            return synced_value
        if synced_value is None:
            return aggregate_value
        return max(aggregate_value, synced_value)

    # Group candle stats by symbol
    symbols_map: dict[str, SymbolCoverage] = {}
    total_candles = 0
    total_ticks = 0

    for row in candle_stats:
        sym = row["symbol"]
        if sym not in symbols_map:
            symbols_map[sym] = SymbolCoverage(symbol=sym)

        count = row["total"]
        total_candles += count
        symbols_map[sym].candles.append(
            TimeframeCoverage(
                timeframe=row["timeframe"],
                first_bar=row["first_bar"],
                last_bar=_latest_timestamp(
                    row["last_bar"],
                    sync_map.get((sym, row["timeframe"])),
                ),
                total_bars=count,
                last_synced_at=sync_map.get((sym, row["timeframe"])),
            )
        )

    for row in tick_stats:
        sym = row["symbol"]
        if sym not in symbols_map:
            symbols_map[sym] = SymbolCoverage(symbol=sym)

        count = row["total"]
        total_ticks += count
        symbols_map[sym].ticks = TickCoverage(
            first_tick=row["first_tick"],
            last_tick=_latest_timestamp(
                row["last_tick"],
                sync_map.get((sym, "tick")),
            ),
            total_ticks=count,
            last_synced_at=sync_map.get((sym, "tick")),
        )

    # Ensure all configured (tracked) symbols appear (even if no data yet)
    for sym in settings.symbols:
        if sym not in symbols_map:
            symbols_map[sym] = SymbolCoverage(symbol=sym)

    # Also include all MT5-available symbols that have data in DB
    from src.api.symbol_registry import get_all_mt5_symbols
    all_mt5 = get_all_mt5_symbols()

    note = CoverageSummary.model_fields["note"].default
    if not include_counts:
        note += " Row counts were intentionally omitted from this lightweight bounds response."

    return CoverageSummary(
        note=note,
        total_candle_rows=total_candles,
        total_tick_rows=total_ticks,
        symbols=sorted(symbols_map.values(), key=lambda s: s.symbol),
        configured_symbols=sorted(settings.symbols),
        available_symbols=sorted(all_mt5.keys()) if all_mt5 else sorted(settings.symbols),
    )
