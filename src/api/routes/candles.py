"""
REST endpoint: ``/api/v1/candles/{symbol}``

Query historical OHLCV candle data.

If the requested ``from`` date is before the data we have stored,
an on-demand backfill request is sent to the MT5 poller via Redis.
The handler waits (up to 60 s) for the data to be downloaded and then
returns the full result.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import CandleResponse
from src.api.services.backfill_helper import maybe_backfill_candles
from src.config import Timeframe

router = APIRouter(prefix="/api/v1", tags=["candles"])


@router.get(
    "/candles/{symbol}",
    response_model=list[CandleResponse],
    summary="Get historical candles",
    description=(
        "Retrieve OHLCV candle bars for a given symbol and timeframe. "
        "Results are ordered by time ascending. "
        "If the requested range is not yet in the database, the system "
        "automatically fetches it from MetaTrader 5 (may take a few seconds "
        "on first request)."
    ),
)
async def get_candles(
    symbol: str,
    timeframe: str = Query(
        default="M1",
        description="Candle timeframe: M1, M5, M15, H1, H4, D1",
    ),
    from_dt: Optional[datetime] = Query(
        default=None,
        alias="from",
        description="Start datetime (ISO 8601). Inclusive.",
    ),
    to_dt: Optional[datetime] = Query(
        default=None,
        alias="to",
        description="End datetime (ISO 8601). Inclusive.",
    ),
    limit: int = Query(
        default=1000,
        ge=1,
        le=50000,
        description="Maximum number of candles to return.",
    ),
) -> list[CandleResponse]:
    # Validate timeframe
    try:
        Timeframe(timeframe.upper())
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. "
                   f"Allowed: {[t.value for t in Timeframe]}",
        )

    rows = await maybe_backfill_candles(
        symbol=symbol.upper(),
        timeframe=timeframe.upper(),
        dt_from=from_dt,
        dt_to=to_dt,
        limit=limit,
    )
    return [CandleResponse(**r) for r in rows]
