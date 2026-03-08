"""
REST endpoint: ``/api/v1/candles/{symbol}``

Query historical OHLCV candle data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import CandleResponse
from src.config import Timeframe
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["candles"])


@router.get(
    "/candles/{symbol}",
    response_model=list[CandleResponse],
    summary="Get historical candles",
    description=(
        "Retrieve OHLCV candle bars for a given symbol and timeframe. "
        "Results are ordered by time ascending."
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

    rows = await repo.query_candles(
        symbol=symbol.upper(),
        timeframe=timeframe.upper(),
        dt_from=from_dt,
        dt_to=to_dt,
        limit=limit,
    )
    return [CandleResponse(**r) for r in rows]
