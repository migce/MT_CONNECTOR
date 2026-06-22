"""
Shared httpx client for proxying to MT5 Connector API.
"""
from __future__ import annotations

import httpx

from app.config import get_settings

_client: httpx.AsyncClient | None = None


def get_mt5_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        settings = get_settings()
        _client = httpx.AsyncClient(
            base_url=settings.mt5_api_url,
            timeout=90.0,
        )
    return _client


async def close_mt5_client() -> None:
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None
