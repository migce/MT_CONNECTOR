"""
REST proxy: ``/api/v1/supervisor/…``

Forwards service-management requests to the native-Windows process
supervisor running on ``SUPERVISOR_URL`` (default ``http://localhost:9100``).
"""

from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1/supervisor", tags=["supervisor"])

SUPERVISOR_URL = os.environ.get("SUPERVISOR_URL", "http://localhost:9100")
_TIMEOUT = 15.0  # generous — stop/restart can take a few seconds


async def _proxy_get(path: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(f"{SUPERVISOR_URL}{path}")
            resp.raise_for_status()
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(503, "Supervisor unreachable — is it running?")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, exc.response.text)
    except Exception:
        raise HTTPException(503, "Supervisor communication error")


async def _proxy_post(path: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(f"{SUPERVISOR_URL}{path}")
            resp.raise_for_status()
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(503, "Supervisor unreachable — is it running?")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, exc.response.text)
    except Exception:
        raise HTTPException(503, "Supervisor communication error")


# ── Read-only status ────────────────────────────────────────────────

@router.get(
    "/status",
    summary="Supervisor & service status",
    description=(
        "Returns supervisor PID, watchdog interval, and state of each "
        "managed service (poller, trader): alive, PID, uptime, restarts."
    ),
)
async def supervisor_status() -> dict:
    return await _proxy_get("/status")


# ── Per-service control ─────────────────────────────────────────────

@router.post(
    "/services/{name}/start",
    summary="Start a service",
    description="Start the named service. Returns 409 if an operation is already in progress.",
)
async def supervisor_start(name: str) -> dict:
    return await _proxy_post(f"/services/{name}/start")


@router.post(
    "/services/{name}/stop",
    summary="Stop a service (no auto-restart)",
    description="Gracefully stop the service. Watchdog will NOT restart it until manual start.",
)
async def supervisor_stop(name: str) -> dict:
    return await _proxy_post(f"/services/{name}/stop")


@router.post(
    "/services/{name}/restart",
    summary="Restart a service",
    description="Stop then start. Returns 409 if an operation is already in progress.",
)
async def supervisor_restart(name: str) -> dict:
    return await _proxy_post(f"/services/{name}/restart")


# ── Bulk control ────────────────────────────────────────────────────

@router.post("/services/start-all", summary="Start all services")
async def supervisor_start_all() -> dict:
    return await _proxy_post("/services/start-all")


@router.post("/services/stop-all", summary="Stop all services (no auto-restart)")
async def supervisor_stop_all() -> dict:
    return await _proxy_post("/services/stop-all")
