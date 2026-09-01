"""
REST endpoint: ``/api/v1/candles/custom/{symbol}``

Serves **non-standard timeframe candles** built on-the-fly from stored data:

* **Time-based** custom TFs (``M2``, ``M3``, ``M7``, ``H2``, ``H6``, ``H12``,
  ``D2``, ``W1``, …) — aggregated from the coarsest exactly dividing stored
  timeframe via TimescaleDB ``time_bucket``.
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

from src.api.schemas import CandleResponse, PaginatedResponse
from src.api.services.backfill_helper import (
    maybe_backfill_candles,
    maybe_backfill_ticks,
)
from src.api.services.validation import validate_symbol
from src.config import (
    Timeframe,
    custom_timeframe_source,
    is_standard_timeframe,
    parse_custom_timeframe,
)
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["custom-candles"])


@router.get(
    "/candles/custom/{symbol}",
    response_model=PaginatedResponse[CandleResponse],
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
    bars: Optional[int] = Query(
        default=None,
        ge=1,
        le=50000,
        description="Alias for limit — number of bars to return.",
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
) -> PaginatedResponse[CandleResponse]:
    symbol = validate_symbol(symbol)
    tf_str = timeframe.strip().upper()

    # bars overrides limit when provided
    effective_limit = bars if bars is not None else limit

    # For "latest N" queries (no from, optionally capped by to) the DB
    # uses a DESC/ASC subquery.  The "+1 fetch" trick doesn't apply.
    use_latest_n = not from_dt
    fetch_limit = effective_limit if use_latest_n else effective_limit + 1

    # ------ Standard TF fast-path ------
    if is_standard_timeframe(tf_str):
        rows = await maybe_backfill_candles(
            symbol=symbol,
            timeframe=tf_str,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=fetch_limit,
        )
        return _paginate_candles(rows, effective_limit, latest_n=use_latest_n)

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
            limit=fetch_limit,
            price_field=price,
            include_incomplete=include_incomplete,
        )
        return _paginate_candles(rows, effective_limit, latest_n=use_latest_n)

    # ------ Time-based custom TF ------
    if ctf.seconds < 60:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Time-based custom TF must be >= 60 seconds (1 minute). "
                f"Got {ctf.seconds}s.  For sub-minute bars use tick bars (T<n>)."
            ),
        )

    source_tf = custom_timeframe_source(ctf)
    if source_tf is None:  # Defensive: the tick branch returns above.
        raise HTTPException(status_code=400, detail="Tick bars require raw tick history")

    # Ensure the exact-divisor source candles are available for the range.
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
        limit=fetch_limit,
        source_tf=source_tf,
    )
    return _paginate_candles(rows, effective_limit, latest_n=use_latest_n)


def _paginate_candles(
    rows: list[dict],
    limit: int,
    *,
    latest_n: bool = False,
) -> PaginatedResponse[CandleResponse]:
    """Build a PaginatedResponse from raw rows, detecting has_more."""
    if latest_n:
        # No date-range: rows are the latest N in ASC order (no extra row).
        data = [CandleResponse(**r) for r in rows]
        return PaginatedResponse(
            data=data,
            count=len(data),
            has_more=len(rows) >= limit,
        )

    # Range query: one extra row was fetched to detect has_more.
    has_more = len(rows) > limit
    if has_more:
        next_from = rows[limit]["time"].isoformat()
        rows = rows[:limit]
    else:
        next_from = None

    data = [CandleResponse(**r) for r in rows]
    return PaginatedResponse(
        data=data,
        count=len(data),
        has_more=has_more,
        next_from=next_from,
    )
