"""
REST endpoint: ``/api/v1/candles/custom/{symbol}``

Serves **non-standard timeframe candles** built on-the-fly from stored data:

* **Time-based** custom TFs (``M2``, ``M3``, ``M7``, ``H2``, ``H6``, ``H12``,
  ``D2``, ``W1``, …) — aggregated from M1 candles via TimescaleDB
  ``time_bucket``.
* **Tick bars** (``T100``, ``T500``, ``T1000``, …) — each bar contains
  exactly *N* ticks, built on-the-fly from the raw ``ticks`` hypertable.
* **Information bars** (``I100``, ``I500``, ``I1000``, …) — research-only
  adaptive bars whose bounded causal tick weights fill an information budget.
* **Adaptive v2 bars** (``A100``, ``A500``, ``A1000``, …) — research-only
  bars whose target tick count is frozen before each bar opens.
* **A3C-v7 visual presets** (``V7M5``, ``V7M15``, ``V7M30``, ``V7M60``) —
  research-only dual-clock bars calibrated to analogous visual density.

Standard timeframes (M1, M5, M15, H1, H4, D1) are redirected to the
pre-computed candle table for maximum performance.

On-demand backfill: if the source data (M1/H1 candles or ticks) does not
cover the requested range, the system automatically fetches it from MT5.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from time import perf_counter
from typing import Literal, Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import CandleResponse, PaginatedResponse
from src.api.services.backfill_helper import (
    maybe_backfill_candles,
    maybe_backfill_ticks,
)
from src.api.services.validation import validate_symbol
from src.config import (
    get_settings,
    is_standard_timeframe,
    parse_custom_timeframe,
)
from src.db import repository as repo
from src.information_bars import (
    InformationBarConfig,
    build_information_bars,
    information_source_limit,
)
from src.information_bars_a3c_v7 import (
    a3c_v7_source_limit,
    a3c_v7_visual_preset_config,
    build_a3c_v7_bars,
)
from src.information_bars_v2 import (
    InformationBarV2Config,
    build_information_bars_v2,
    information_v2_source_limit,
)

router = APIRouter(prefix="/api/v1", tags=["custom-candles"])
logger = structlog.get_logger(__name__)

_event_gate_loop: asyncio.AbstractEventLoop | None = None
_event_gate: asyncio.Semaphore | None = None


def _event_request_gate() -> asyncio.Semaphore:
    """Return one loop-local gate so heavy snapshots cannot crowd out health."""
    global _event_gate_loop, _event_gate
    loop = asyncio.get_running_loop()
    if _event_gate is None or _event_gate_loop is not loop:
        _event_gate_loop = loop
        _event_gate = asyncio.Semaphore(
            get_settings().custom_candle_max_concurrency
        )
    return _event_gate


async def _build_adaptive_snapshot(
    *,
    symbol: str,
    timeframe: str,
    dt_from: datetime | None,
    dt_to: datetime | None,
    source_limit: int,
    builder,
    config,
    price: str,
    include_incomplete: bool,
) -> tuple[list[dict], list[dict]]:
    """Read a bounded tick prefix and build bars without blocking the event loop."""
    settings = get_settings()
    queued_at = perf_counter()
    async with _event_request_gate():
        started_at = perf_counter()
        ticks = await repo.query_information_bar_ticks(
            symbol=symbol,
            dt_from=dt_from,
            dt_to=dt_to,
            source_limit=source_limit,
            work_mem_mb=settings.custom_candle_work_mem_mb,
        )
        queried_at = perf_counter()
        rows = await asyncio.to_thread(
            builder,
            ticks,
            config,
            price_field=price,
            include_incomplete=include_incomplete,
        )
        completed_at = perf_counter()
    logger.info(
        "custom_candle_snapshot_built",
        symbol=symbol,
        timeframe=timeframe,
        source_limit=source_limit,
        source_tick_count=len(ticks),
        output_bars=len(rows),
        queue_ms=round((started_at - queued_at) * 1000, 1),
        query_ms=round((queried_at - started_at) * 1000, 1),
        build_ms=round((completed_at - queried_at) * 1000, 1),
        total_ms=round((completed_at - queued_at) * 1000, 1),
    )
    return rows, ticks


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
        "**Adaptive information bars**: `I100`, `I250`, `I500`, `I1000`, … — "
        "research-only causal bars that expand directional activity and "
        "compress quiet or two-sided tick noise.\n\n"
        "**Adaptive target-tick v2 bars**: `A100`, `A250`, `A500`, "
        "`A1000`, … — research-only causal bars with a target tick count "
        "frozen before each bar opens.\n\n"
        "**A3C-v7 visual presets**: `V7M5`, `V7M15`, `V7M30`, `V7M60` — "
        "research-only causal bars with density analogous to the named "
        "time period, while neutral movement remains compressed.\n\n"
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
            "Tick bars: T100, T500, T1000, …  "
            "Information bars v1: I100, I500, I1000, …  "
            "Adaptive v2 bars: A100, A500, A1000, …"
            "  A3C-v7 presets: V7M5, V7M15, V7M30, V7M60."
        ),
        examples=["M2", "H2", "T500", "I500", "A500", "V7M15"],
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
            "not reached its fixed or adaptive budget. Default: only full bars."
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

    # ------ A3C-v7 visual-density presets (research-only) ------
    if ctf.is_a3c_v7_bar:
        await maybe_backfill_ticks(
            symbol=symbol,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=1,
        )
        config_v7 = a3c_v7_visual_preset_config(ctf.raw)
        source_limit = a3c_v7_source_limit(
            ctf.raw,
            fetch_limit,
            hard_cap=get_settings().custom_candle_max_source_ticks,
        )
        rows, ticks = await _build_adaptive_snapshot(
            symbol=symbol,
            timeframe=ctf.raw,
            dt_from=from_dt,
            dt_to=to_dt,
            source_limit=source_limit,
            builder=build_a3c_v7_bars,
            config=config_v7,
            price=price,
            include_incomplete=include_incomplete,
        )
        rows = rows[-fetch_limit:] if use_latest_n else rows[:fetch_limit]
        meta = {
            "bar_model": config_v7.metadata(),
            "price": price,
            "strategy_eligible": False,
            "anchor_mode": "bounded_query_prefix",
            "source_tick_count": len(ticks),
            "source_limit": source_limit,
            "source_truncated": len(ticks) >= source_limit,
            "known_limitations": [
                "query_prefix_anchor",
                "completed_minute_state_warmup",
                "same_millisecond_tick_identity",
                "connection_local_live_warmup",
                "no_persistent_live_revision_sequence",
            ],
        }
        return _paginate_candles(
            rows,
            effective_limit,
            latest_n=use_latest_n,
            meta=meta,
        )

    # ------ Adaptive target-tick bars v2 (research-only) ------
    if ctf.is_adaptive_target_bar:
        if ctf.adaptive_target_ticks < 2:
            raise HTTPException(
                status_code=400,
                detail="Adaptive v2 neutral target must be >= 2.",
            )
        await maybe_backfill_ticks(
            symbol=symbol,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=1,
        )
        config_v2 = InformationBarV2Config(
            neutral_ticks=ctf.adaptive_target_ticks
        )
        source_limit = information_v2_source_limit(
            config_v2,
            fetch_limit,
            hard_cap=get_settings().custom_candle_max_source_ticks,
        )
        rows, ticks = await _build_adaptive_snapshot(
            symbol=symbol,
            timeframe=ctf.raw,
            dt_from=from_dt,
            dt_to=to_dt,
            source_limit=source_limit,
            builder=build_information_bars_v2,
            config=config_v2,
            price=price,
            include_incomplete=include_incomplete,
        )
        rows = rows[-fetch_limit:] if use_latest_n else rows[:fetch_limit]
        meta = {
            "bar_model": config_v2.metadata(),
            "price": price,
            "strategy_eligible": False,
            "anchor_mode": "bounded_query_prefix",
            "source_tick_count": len(ticks),
            "source_limit": source_limit,
            "source_truncated": len(ticks) >= source_limit,
            "known_limitations": [
                "query_prefix_anchor",
                "same_millisecond_tick_identity",
                "connection_local_live_warmup",
                "no_persistent_live_revision_sequence",
            ],
        }
        return _paginate_candles(
            rows,
            effective_limit,
            latest_n=use_latest_n,
            meta=meta,
        )

    # ------ Adaptive information bars (research-only) ------
    if ctf.is_information_bar:
        if ctf.information_budget < 2:
            raise HTTPException(
                status_code=400,
                detail="Information bar budget must be >= 2.",
            )
        await maybe_backfill_ticks(
            symbol=symbol,
            dt_from=from_dt,
            dt_to=to_dt,
            limit=1,
        )
        config = InformationBarConfig(budget=ctf.information_budget)
        source_limit = information_source_limit(
            config,
            fetch_limit,
            hard_cap=get_settings().custom_candle_max_source_ticks,
        )
        rows, ticks = await _build_adaptive_snapshot(
            symbol=symbol,
            timeframe=ctf.raw,
            dt_from=from_dt,
            dt_to=to_dt,
            source_limit=source_limit,
            builder=build_information_bars,
            config=config,
            price=price,
            include_incomplete=include_incomplete,
        )
        rows = rows[-fetch_limit:] if use_latest_n else rows[:fetch_limit]
        meta = {
            "bar_model": config.metadata(),
            "price": price,
            "strategy_eligible": False,
            "anchor_mode": "bounded_query_prefix",
            "source_tick_count": len(ticks),
            "source_limit": source_limit,
            "source_truncated": len(ticks) >= source_limit,
            "known_limitations": [
                "query_prefix_anchor",
                "same_millisecond_tick_identity",
                "no_persistent_live_revision_sequence",
            ],
        }
        return _paginate_candles(
            rows,
            effective_limit,
            latest_n=use_latest_n,
            meta=meta,
        )

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
        settings = get_settings()
        requested_source_ticks = ctf.tick_count * fetch_limit
        queued_at = perf_counter()
        async with _event_request_gate():
            started_at = perf_counter()
            rows = await repo.query_tick_bars(
                symbol=symbol,
                tick_count=ctf.tick_count,
                tf_label=ctf.raw,
                dt_from=from_dt,
                dt_to=to_dt,
                limit=fetch_limit,
                price_field=price,
                include_incomplete=include_incomplete,
                max_source_rows=settings.custom_candle_max_source_ticks,
                work_mem_mb=settings.custom_candle_work_mem_mb,
            )
            completed_at = perf_counter()
        logger.info(
            "custom_candle_snapshot_built",
            symbol=symbol,
            timeframe=ctf.raw,
            source_limit=min(
                requested_source_ticks,
                settings.custom_candle_max_source_ticks,
            ),
            requested_source_ticks=requested_source_ticks,
            output_bars=len(rows),
            queue_ms=round((started_at - queued_at) * 1000, 1),
            query_ms=round((completed_at - started_at) * 1000, 1),
            build_ms=0.0,
            total_ms=round((completed_at - queued_at) * 1000, 1),
        )
        return _paginate_candles(
            rows,
            effective_limit,
            latest_n=use_latest_n,
            meta={
                "source_limit": min(
                    requested_source_ticks,
                    settings.custom_candle_max_source_ticks,
                ),
                "source_truncated": (
                    requested_source_ticks
                    > settings.custom_candle_max_source_ticks
                ),
            },
        )

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
        limit=fetch_limit,
        source_tf=source_tf,
    )
    return _paginate_candles(rows, effective_limit, latest_n=use_latest_n)


def _paginate_candles(
    rows: list[dict],
    limit: int,
    *,
    latest_n: bool = False,
    meta: dict | None = None,
) -> PaginatedResponse[CandleResponse]:
    """Build a PaginatedResponse from raw rows, detecting has_more."""
    if latest_n:
        # No date-range: rows are the latest N in ASC order (no extra row).
        data = [CandleResponse(**r) for r in rows]
        return PaginatedResponse(
            data=data,
            count=len(data),
            has_more=len(rows) >= limit,
            meta=meta,
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
        meta=meta,
    )
