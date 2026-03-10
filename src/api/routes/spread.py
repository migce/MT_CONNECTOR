"""
REST endpoint: ``/api/v1/spread/{symbol}``

Query spread history — from candle metadata (fast) or raw tick data
(granular, supports time-bucket aggregation).

Three modes controlled by the ``source`` query parameter:

- **candles** (default): spread column from the ``candles`` table.
  Fast, returns integer spread in broker points.
- **ticks**: raw ``ask − bid`` from the ``ticks`` table.
  Maximum granularity, returns spread in price units.
- **ticks_agg**: same as *ticks* but aggregated into time buckets via
  TimescaleDB ``time_bucket``. Returns avg / min / max per bucket.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Union

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import PaginatedResponse, SpreadAggPoint, SpreadPoint
from src.api.services.validation import validate_symbol
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["spread"])


class SpreadSource(str, Enum):
    candles = "candles"
    ticks = "ticks"
    ticks_agg = "ticks_agg"


_VALID_BUCKETS = {"1 min", "5 min", "15 min", "30 min", "1 hour", "4 hours", "1 day"}


@router.get(
    "/spread/{symbol}",
    response_model=PaginatedResponse[Union[SpreadAggPoint, SpreadPoint]],
    summary="Get spread history",
    description=(
        "Retrieve spread history for a given symbol.\n\n"
        "**source=candles** (default) — integer spread in broker points from OHLCV "
        "candle data. Specify `timeframe` (M1, M5, …).\n\n"
        "**source=ticks** — raw `ask − bid` spread from tick data. "
        "Maximum precision.\n\n"
        "**source=ticks_agg** — tick-level spread aggregated into time "
        "buckets (avg/min/max). Use `bucket` to set the interval "
        "(e.g. `1 hour`, `15 min`, `1 day`)."
    ),
)
async def get_spread(
    symbol: str,
    source: SpreadSource = Query(
        default=SpreadSource.candles,
        description="Data source: candles | ticks | ticks_agg",
    ),
    timeframe: str = Query(
        default="M1",
        description="Candle timeframe (only for source=candles): M1, M5, M15, H1, H4, D1",
    ),
    bucket: str = Query(
        default="1 hour",
        description=(
            "Time-bucket interval (only for source=ticks_agg). "
            "Allowed: 1 min, 5 min, 15 min, 30 min, 1 hour, 4 hours, 1 day"
        ),
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
        default=5000,
        ge=1,
        le=50000,
        description="Maximum number of data points to return.",
    ),
) -> PaginatedResponse[Union[SpreadAggPoint, SpreadPoint]]:
    symbol = validate_symbol(symbol)

    if source == SpreadSource.candles:
        rows = await repo.query_spread_from_candles(
            symbol=symbol,
            timeframe=timeframe.upper(),
            dt_from=from_dt,
            dt_to=to_dt,
            limit=limit + 1,
        )
        has_more = len(rows) > limit
        next_from = rows[limit]["time"].isoformat() if has_more else None
        rows = rows[:limit]
        data = [SpreadPoint(time=r["time"], spread=r["spread"]) for r in rows]

    elif source == SpreadSource.ticks:
        rows = await repo.query_spread_from_ticks(
            symbol=symbol,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=limit + 1,
        )
        has_more = len(rows) > limit
        next_from = rows[limit]["time"].isoformat() if has_more else None
        rows = rows[:limit]
        data = [SpreadPoint(time=r["time"], spread=r["spread_raw"]) for r in rows]

    elif source == SpreadSource.ticks_agg:
        if bucket not in _VALID_BUCKETS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid bucket '{bucket}'. Allowed: {sorted(_VALID_BUCKETS)}",
            )
        rows = await repo.query_spread_aggregated(
            symbol=symbol,
            bucket=bucket,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=limit + 1,
        )
        has_more = len(rows) > limit
        next_from = rows[limit]["time"].isoformat() if has_more else None
        rows = rows[:limit]
        data = [SpreadAggPoint(**r) for r in rows]  # type: ignore[arg-type]

    else:
        raise HTTPException(status_code=400, detail="Unknown source")

    return PaginatedResponse(
        data=data,
        count=len(data),
        has_more=has_more,
        next_from=next_from,
    )
