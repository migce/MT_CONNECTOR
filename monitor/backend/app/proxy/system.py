"""
Proxy: system routes (health, stats, uptime) — admin only.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.auth.dependencies import get_current_user, require_admin
from app.db.models import User
from app.proxy import get_mt5_client

router = APIRouter()


async def _forward_get(path: str, request: Request) -> JSONResponse:
    client = get_mt5_client()
    resp = await client.get(path, params=dict(request.query_params))
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


@router.get("/health")
async def health(
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get("/api/v1/health", request)


@router.get("/stats")
async def stats(
    request: Request,
    _admin: Annotated[User, Depends(require_admin)],
):
    return await _forward_get("/api/v1/stats", request)


@router.get("/stats/daily")
async def daily_stats(
    request: Request,
    _admin: Annotated[User, Depends(require_admin)],
):
    return await _forward_get("/api/v1/stats/daily", request)


@router.get("/uptime")
async def uptime(
    request: Request,
    _admin: Annotated[User, Depends(require_admin)],
):
    return await _forward_get("/api/v1/uptime", request)


@router.get("/coverage")
async def coverage(
    request: Request,
    _admin: Annotated[User, Depends(require_admin)],
):
    return await _forward_get("/api/v1/coverage", request)
