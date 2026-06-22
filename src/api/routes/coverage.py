"""
REST endpoint: ``/api/v1/coverage``

Data coverage statistics — for each symbol × timeframe and ticks:
earliest bar, latest bar, total count, and sync_state metadata.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from src.config import get_settings
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["coverage"])


# ---------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------

class TimeframeCoverage(BaseModel):
    timeframe: str
    first_bar: Optional[datetime] = None
    last_bar: Optional[datetime] = None
    total_bars: int = 0
    last_synced_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class TickCoverage(BaseModel):
    first_tick: Optional[datetime] = None
    last_tick: Optional[datetime] = None
    total_ticks: int = 0
    last_synced_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class SymbolCoverage(BaseModel):
    symbol: str
    candles: list[TimeframeCoverage] = []
    ticks: TickCoverage = TickCoverage()


class CoverageSummary(BaseModel):
    note: str = (
        "Coverage shows data currently loaded in the database. "
        "Additional historical data can be fetched on-demand via "
        "/api/v1/candles/{symbol}?from=...&to=... or "
        "POST /api/v1/backfill for explicit range preload."
    )
    total_candle_rows: int = 0
    total_tick_rows: int = 0
    symbols: list[SymbolCoverage] = []
    configured_symbols: list[str] = []
    available_symbols: list[str] = []


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
async def get_coverage() -> CoverageSummary:
    candle_stats = await repo.query_candle_coverage()
    tick_stats = await repo.query_tick_coverage()
    sync_states = await repo.query_all_sync_states()
    settings = get_settings()

    # Index sync states for fast lookup
    sync_map: dict[tuple[str, str], datetime] = {}
    for s in sync_states:
        sync_map[(s["symbol"], s["data_type"])] = s["last_synced_at"]

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
                last_bar=row["last_bar"],
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
            last_tick=row["last_tick"],
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

    return CoverageSummary(
        total_candle_rows=total_candles,
        total_tick_rows=total_ticks,
        symbols=sorted(symbols_map.values(), key=lambda s: s.symbol),
        configured_symbols=sorted(settings.symbols),
        available_symbols=sorted(all_mt5.keys()) if all_mt5 else sorted(settings.symbols),
    )
