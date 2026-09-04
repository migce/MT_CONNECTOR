"""Connector-owned Symbol Management API."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from src.api.services.validation import validate_symbol
from src.api.symbol_registry import get_all_mt5_symbols
from src.config import Timeframe, custom_timeframe_source, parse_custom_timeframe
from src.db import symbol_management as sm

router = APIRouter(prefix="/api/v1/symbol-management", tags=["symbol-management"])


class ManagedSymbolUpdate(BaseModel):
    active: bool = True


class CustomTimeframeCreate(BaseModel):
    code: str = Field(min_length=2, max_length=16)
    kind: Literal["time", "tick"] | None = None


class CustomTimeframeBinding(BaseModel):
    mode: Literal["virtual", "materialized"] = "virtual"
    enabled: bool = True


class BackfillJobCreate(BaseModel):
    symbol: str
    target_type: Literal["candles", "ticks", "custom"]
    timeframe: str | None = None
    mode: Literal["fill_missing", "refresh"] = "fill_missing"
    from_dt: datetime = Field(alias="from")
    to_dt: datetime = Field(alias="to")
    requested_by: str | None = None

    model_config = {"populate_by_name": True}


class RetentionApply(BaseModel):
    days: int = Field(ge=7, le=3650)
    confirm: bool = False


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _available_symbol(raw: str) -> tuple[str, str]:
    """Resolve only symbols advertised by the connected broker terminal."""
    normalized = validate_symbol(raw)
    catalog = get_all_mt5_symbols()
    if not catalog:
        raise HTTPException(status_code=503, detail="MT5 symbol catalogue is unavailable")
    if normalized not in catalog:
        raise HTTPException(status_code=404, detail="Symbol is not available in the MT5 Connector")
    return normalized, catalog[normalized]


def _timeframe_metadata(item: dict) -> dict:
    parsed = parse_custom_timeframe(str(item["code"]))
    return {
        **item,
        "kind": "tick" if parsed.is_tick_bar else "time",
        "source_type": "ticks" if parsed.is_tick_bar else "candles",
        "source_timeframe": custom_timeframe_source(parsed),
    }


@router.get("/tree")
async def get_tree():
    rows = await sm.coverage_tree()
    custom = [_timeframe_metadata(item) for item in await sm.list_custom_timeframes()]
    definitions = {item["code"]: item for item in custom}
    for row in rows:
        bindings = await sm.symbol_bindings(row["symbol"])
        row["custom_timeframes"] = [
            {**binding, "definition": definitions.get(binding["timeframe"])}
            for binding in bindings
        ]
    return {
        "symbols": rows,
        "standard_timeframes": list(sm.STANDARD_TIMEFRAMES),
        "custom_timeframes": custom,
    }


@router.get("/catalog")
async def get_catalog(query: str = Query(default="", max_length=64)):
    managed = {item["symbol"]: item for item in await sm.list_managed_symbols()}
    needle = query.strip().upper()
    rows = []
    for symbol, description in sorted(get_all_mt5_symbols().items()):
        if needle and needle not in symbol and needle not in description.upper():
            continue
        rows.append({
            "symbol": symbol,
            "description": description,
            "active": bool(managed.get(symbol, {}).get("active", False)),
        })
        if len(rows) >= 100:
            break
    return rows


@router.put("/symbols/{symbol}")
async def update_symbol(symbol: str, body: ManagedSymbolUpdate):
    normalized, description = _available_symbol(symbol)
    return await sm.set_managed_symbol(normalized, description, body.active)


@router.get("/symbols/{symbol}/details")
async def get_symbol_detail(
    symbol: str,
    target_type: Literal["candles", "ticks", "custom"],
    timeframe: str | None = None,
):
    normalized, _description = _available_symbol(symbol)
    if target_type != "ticks" and not timeframe:
        raise HTTPException(status_code=400, detail="timeframe is required")
    detail = await sm.coverage_detail(normalized, "ticks" if target_type == "ticks" else "candles", timeframe)
    detail.update({"symbol": normalized, "target_type": target_type, "timeframe": timeframe})
    return detail


@router.get("/timeframes")
async def get_timeframes():
    return [_timeframe_metadata(item) for item in await sm.list_custom_timeframes()]


@router.post("/timeframes", status_code=201)
async def create_timeframe(body: CustomTimeframeCreate):
    code = body.code.strip().upper()
    if code in sm.STANDARD_TIMEFRAMES:
        raise HTTPException(status_code=409, detail="Standard timeframe already exists")
    try:
        parsed = parse_custom_timeframe(code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if parsed.is_tick_bar and parsed.tick_count < 2:
        raise HTTPException(status_code=400, detail="Tick bar size must be at least 2")
    parsed_kind = "tick" if parsed.is_tick_bar else "time"
    if body.kind is not None and body.kind != parsed_kind:
        raise HTTPException(status_code=400, detail="Timeframe type does not match its code")
    unit = code[0]
    value = parsed.tick_count if parsed.is_tick_bar else int(code[1:])
    return _timeframe_metadata(await sm.upsert_custom_timeframe(code, unit, value))


@router.put("/symbols/{symbol}/timeframes/{timeframe}")
async def bind_timeframe(symbol: str, timeframe: str, body: CustomTimeframeBinding):
    normalized, description = _available_symbol(symbol)
    code = timeframe.strip().upper()
    parsed = parse_custom_timeframe(code)
    if parsed.is_tick_bar and body.mode == "materialized":
        raise HTTPException(status_code=400, detail="Tick bars are virtual-only")
    await sm.set_managed_symbol(normalized, description, True)
    return await sm.bind_custom_timeframe(normalized, code, body.mode, body.enabled)


@router.get("/jobs")
async def get_jobs(limit: int = Query(default=50, ge=1, le=200)):
    return await sm.list_jobs(limit)


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    job = await sm.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    return job


@router.post("/jobs", status_code=202)
async def create_job(body: BackfillJobCreate):
    symbol, _description = _available_symbol(body.symbol)
    dt_from, dt_to = _utc(body.from_dt), _utc(body.to_dt)
    if dt_from >= dt_to:
        raise HTTPException(status_code=400, detail="from must be before to")
    if dt_to > datetime.now(UTC):
        raise HTTPException(status_code=400, detail="to cannot be in the future")

    target_type = body.target_type
    timeframe = body.timeframe.strip().upper() if body.timeframe else None
    source_type = target_type
    source_timeframe = timeframe
    if target_type == "candles":
        if timeframe is None:
            raise HTTPException(status_code=400, detail="timeframe is required")
        try:
            Timeframe(timeframe)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Use target_type=custom for custom timeframes") from exc
    elif target_type == "custom":
        if timeframe is None:
            raise HTTPException(status_code=400, detail="timeframe is required")
        parsed = parse_custom_timeframe(timeframe)
        source_type = "ticks" if parsed.is_tick_bar else "candles"
        source_timeframe = None
        if not parsed.is_tick_bar:
            source_timeframe = custom_timeframe_source(parsed)
    else:
        timeframe = None
        retention = await sm.get_retention_days()
        cutoff = datetime.now(UTC).timestamp() - retention * 86400
        if dt_from.timestamp() < cutoff:
            raise HTTPException(status_code=409, detail="Increase tick retention before loading this range")

    values = {
        "symbol": symbol,
        "target_type": target_type,
        "timeframe": timeframe,
        "source_type": source_type,
        "source_timeframe": source_timeframe,
        "mode": body.mode,
        "range_from": dt_from,
        "range_to": dt_to,
        "requested_by": body.requested_by,
    }
    job, created = await sm.create_job(values)
    if created:
        from src.api.app import get_backfill_requester
        requester = get_backfill_requester()
        if requester is None:
            await sm.update_job(
                job["id"],
                status="failed",
                error="Backfill requester is unavailable",
                finished_at=datetime.now(UTC),
            )
            raise HTTPException(status_code=503, detail="Backfill requester is unavailable")
        try:
            await requester.enqueue_job(job)
        except Exception as exc:
            await sm.update_job(
                job["id"],
                status="failed",
                error="Unable to enqueue the backfill job",
                finished_at=datetime.now(UTC),
            )
            raise HTTPException(
                status_code=503,
                detail="Unable to enqueue the backfill job",
            ) from exc
    return {**job, "deduplicated": not created}


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    job = await sm.request_cancel(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    return job


@router.get("/retention")
async def get_retention():
    return {"days": await sm.get_retention_days()}


@router.get("/retention/preview")
async def preview_retention(days: int = Query(ge=7, le=3650)):
    return await sm.retention_preview(days)


@router.put("/retention")
async def update_retention(body: RetentionApply):
    current = await sm.get_retention_days()
    if body.days < current and not body.confirm:
        raise HTTPException(status_code=409, detail={
            "message": "Retention reduction requires confirmation",
            "preview": await sm.retention_preview(body.days),
        })
    return {"days": await sm.apply_retention_days(body.days)}
