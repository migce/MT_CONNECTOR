"""
REST endpoint: ``/api/v1/candles/custom/{symbol}``

Serves **non-standard timeframe candles** built on-the-fly from stored data:

* **Time-based** custom TFs (``M2``, ``M3``, ``M7``, ``H2``, ``H6``, ``H12``,
  ``D2``, ``W1``, …) — aggregated from M1 candles via TimescaleDB
  ``time_bucket``.
* **Tick bars** (``T100``, ``T500``, ``T1000``, …) — each bar contains
  exactly *N* ticks, built on-the-fly from the raw ``ticks`` hypertable.

Standard timeframes (M1, M5, M15, H1, H4, D1) are redirected to the
pre-computed candle table for maximum performance.

On-demand backfill: if the source data (M1/H1 candles or ticks) does not
cover the requested range, the system automatically fetches it from MT5.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import CandleResponse
from src.api.services.backfill_helper import (
    maybe_backfill_candles,
    maybe_backfill_ticks,
)
from src.api.services.validation import backfill_limiter, validate_symbol
from src.config import (
    Timeframe,
    is_standard_timeframe,
    parse_custom_timeframe,
)
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["custom-candles"])


def _choose_source_tf(bucket_seconds: int) -> str:
    """
    Pick the coarsest stored timeframe that still fits evenly into the
    requested bucket for faster aggregation.

    Rules:
        bucket >= 3600 s *and* divisible by 3600  → source H1
        otherwise                                 → source M1
    """
    if bucket_seconds >= 3600 and bucket_seconds % 3600 == 0:
        return "H1"
    return "M1"


@router.get(
    "/candles/custom/{symbol}",
    response_model=list[CandleResponse],
    summary="Custom-timeframe candles",
    description=(
        "Build candles for **any** timeframe on-the-fly.\n\n"
        "**Time-based**: `M2`, `M3`, `M7`, `M10`, `M20`, `M30`, "
        "`H2`, `H3`, `H6`, `H8`, `H12`, `D2`, `W1`, … — "
        "any `{unit}{number}` where unit is `M` (minutes), `H` (hours), "
        "`D` (days), `W` (weeks).  Minimum bucket size is 60 s.\n\n"
        "**Tick bars**: `T100`, `T250`, `T500`, `T1000`, … — "
        "each bar contains exactly N ticks.\n\n"
        "Standard timeframes (M1, M5, M15, H1, H4, D1) are served from "
        "the pre-computed table."
    ),
)
async def get_custom_candles(
    symbol: str,
    timeframe: str = Query(
        ...,
        description=(
            "Custom timeframe string.  "
            "Time-based: M2, M3, H2, H6, H12, D2, W1, …  "
            "Tick bars: T100, T500, T1000, …"
        ),
        examples=["M2", "M3", "M10", "H2", "H6", "H12", "T100", "T500"],
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
    price: Literal["bid", "ask", "last", "mid"] = Query(
        default="bid",
        description=(
            "Price field for tick bars: ``bid`` (default), ``ask``, "
            "``last``, or ``mid`` = (bid+ask)/2.  "
            "Ignored for time-based TFs."
        ),
    ),
    include_incomplete: bool = Query(
        default=False,
        description=(
            "For tick bars: include the last (incomplete) bar if it has "
            "fewer than N ticks.  Default: only full bars."
        ),
    ),
) -> list[CandleResponse]:
    symbol = validate_symbol(symbol)
    tf_str = timeframe.strip().upper()
    await backfill_limiter.check(symbol)

    # ------ Standard TF fast-path ------
    if is_standard_timeframe(tf_str):
        rows = await maybe_backfill_candles(
            symbol=symbol,
            timeframe=tf_str,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=limit,
        )
        return [CandleResponse(**r) for r in rows]

    # ------ Parse custom TF ------
    try:
        ctf = parse_custom_timeframe(tf_str)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # ------ Tick bars ------
    if ctf.is_tick_bar:
        if ctf.tick_count < 2:
            raise HTTPException(
                status_code=400,
                detail="Tick bar count must be >= 2.",
            )
        # Ensure source ticks are available
        await maybe_backfill_ticks(
            symbol=symbol,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=1,  # just trigger backfill if needed
        )
        rows = await repo.query_tick_bars(
            symbol=symbol,
            tick_count=ctf.tick_count,
            tf_label=ctf.raw,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=limit,
            price_field=price,
            include_incomplete=include_incomplete,
        )
        return [CandleResponse(**r) for r in rows]

    # ------ Time-based custom TF ------
    if ctf.seconds < 60:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Time-based custom TF must be >= 60 seconds (1 minute). "
                f"Got {ctf.seconds}s.  For sub-minute bars use tick bars (T<n>)."
            ),
        )

    source_tf = _choose_source_tf(ctf.seconds)

    # Ensure source candles (M1 or H1) are available for the range
    await maybe_backfill_candles(
        symbol=symbol,
        timeframe=source_tf,
        dt_from=from_dt,
        dt_to=to_dt,
        limit=1,  # just trigger backfill if needed
    )

    rows = await repo.query_custom_tf_candles(
        symbol=symbol,
        bucket_seconds=ctf.seconds,
        tf_label=ctf.raw,
        dt_from=from_dt,
        dt_to=to_dt,
        limit=limit,
        source_tf=source_tf,
    )
    return [CandleResponse(**r) for r in rows]
