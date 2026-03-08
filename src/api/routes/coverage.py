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
    total_candle_rows: int = 0
    total_tick_rows: int = 0
    symbols: list[SymbolCoverage] = []


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

    return CoverageSummary(
        total_candle_rows=total_candles,
        total_tick_rows=total_ticks,
        symbols=sorted(symbols_map.values(), key=lambda s: s.symbol),
    )
