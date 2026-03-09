"""
MT5 Connector — Python Client SDK.

A lightweight async/sync client for consuming the MT5 Connector REST
and WebSocket APIs from another Python project.

Usage::

    from src.api.client import MT5Client

    client = MT5Client("http://localhost:8000")

    # REST — historical candles
    candles = await client.get_candles("EURUSD", "H1", limit=100)

    # REST — historical ticks
    ticks = await client.get_ticks("EURUSD", limit=500)

    # WebSocket — real-time ticks
    async for tick in client.stream_ticks("EURUSD"):
        print(tick)

    # WebSocket — real-time candle updates
    async for candle in client.stream_candles("EURUSD", "M1"):
        print(candle)
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Optional

import httpx

try:
    import websockets
    import websockets.client
    _HAS_WEBSOCKETS = True
except ImportError:
    _HAS_WEBSOCKETS = False


class MT5Client:
    """
    Async client for the MT5 Connector API.

    Parameters
    ----------
    base_url : str
        Base URL of the API server, e.g. ``"http://localhost:8000"``.
    timeout : float
        HTTP request timeout in seconds (default 30).
    """

    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self._base, timeout=timeout)

    async def close(self) -> None:
        await self._http.aclose()

    # ---------------------------------------------------------------
    # REST helpers
    # ---------------------------------------------------------------

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        resp = await self._http.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    # ---------------------------------------------------------------
    # Symbols
    # ---------------------------------------------------------------

    async def get_symbols(self) -> list[dict[str, Any]]:
        """GET /api/v1/symbols"""
        return await self._get("/api/v1/symbols")

    # ---------------------------------------------------------------
    # Candles
    # ---------------------------------------------------------------

    async def get_candles(
        self,
        symbol: str,
        timeframe: str = "M1",
        from_dt: Optional[datetime | str] = None,
        to_dt: Optional[datetime | str] = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """GET /api/v1/candles/{symbol} — returns paginated response dict.

        Keys: ``data`` (list), ``count``, ``has_more``, ``next_from``.
        """
        params: dict[str, Any] = {
            "timeframe": timeframe,
            "limit": limit,
        }
        if from_dt:
            params["from"] = from_dt if isinstance(from_dt, str) else from_dt.isoformat()
        if to_dt:
            params["to"] = to_dt if isinstance(to_dt, str) else to_dt.isoformat()
        return await self._get(f"/api/v1/candles/{symbol}", params)

    async def get_all_candles(
        self,
        symbol: str,
        timeframe: str = "M1",
        from_dt: Optional[datetime | str] = None,
        to_dt: Optional[datetime | str] = None,
        limit: int = 1000,
        max_pages: int = 100,
    ) -> list[dict[str, Any]]:
        """Auto-paginate through all candle pages and return a flat list."""
        all_rows: list[dict[str, Any]] = []
        cursor = from_dt
        for _ in range(max_pages):
            page = await self.get_candles(symbol, timeframe, cursor, to_dt, limit)
            all_rows.extend(page["data"])
            if not page["has_more"]:
                break
            cursor = page["next_from"]
        return all_rows

    # ---------------------------------------------------------------
    # Ticks
    # ---------------------------------------------------------------

    async def get_ticks(
        self,
        symbol: str,
        from_dt: Optional[datetime | str] = None,
        to_dt: Optional[datetime | str] = None,
        limit: int = 5000,
    ) -> dict[str, Any]:
        """GET /api/v1/ticks/{symbol} — returns paginated response dict.

        Keys: ``data`` (list), ``count``, ``has_more``, ``next_from``.
        """
        params: dict[str, Any] = {"limit": limit}
        if from_dt:
            params["from"] = from_dt if isinstance(from_dt, str) else from_dt.isoformat()
        if to_dt:
            params["to"] = to_dt if isinstance(to_dt, str) else to_dt.isoformat()
        return await self._get(f"/api/v1/ticks/{symbol}", params)

    async def get_all_ticks(
        self,
        symbol: str,
        from_dt: Optional[datetime | str] = None,
        to_dt: Optional[datetime | str] = None,
        limit: int = 5000,
        max_pages: int = 100,
    ) -> list[dict[str, Any]]:
        """Auto-paginate through all tick pages and return a flat list."""
        all_rows: list[dict[str, Any]] = []
        cursor = from_dt
        for _ in range(max_pages):
            page = await self.get_ticks(symbol, cursor, to_dt, limit)
            all_rows.extend(page["data"])
            if not page["has_more"]:
                break
            cursor = page["next_from"]
        return all_rows

    # ---------------------------------------------------------------
    # Health
    # ---------------------------------------------------------------

    async def health(self) -> dict[str, Any]:
        """GET /api/v1/health"""
        return await self._get("/api/v1/health")

    # ---------------------------------------------------------------
    # WebSocket streaming
    # ---------------------------------------------------------------

    async def stream_ticks(
        self,
        symbol: str,
        on_message: Optional[Callable[[dict], Any]] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Connect to ``/ws/ticks/{symbol}`` and yield tick messages.

        If *on_message* is provided, it is called for each tick instead
        of yielding.
        """
        if not _HAS_WEBSOCKETS:
            raise RuntimeError("Install 'websockets' package for streaming support")

        ws_url = self._base.replace("http://", "ws://").replace("https://", "wss://")
        url = f"{ws_url}/ws/ticks/{symbol.upper()}"

        async for ws in websockets.client.connect(url):
            try:
                async for raw in ws:
                    data = json.loads(raw)
                    if on_message:
                        on_message(data)
                    else:
                        yield data
            except websockets.ConnectionClosed:
                continue  # auto-reconnect

    async def stream_candles(
        self,
        symbol: str,
        timeframe: str = "M1",
        on_message: Optional[Callable[[dict], Any]] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Connect to ``/ws/candles/{symbol}/{timeframe}`` and yield candle updates.
        """
        if not _HAS_WEBSOCKETS:
            raise RuntimeError("Install 'websockets' package for streaming support")

        ws_url = self._base.replace("http://", "ws://").replace("https://", "wss://")
        url = f"{ws_url}/ws/candles/{symbol.upper()}/{timeframe.upper()}"

        async for ws in websockets.client.connect(url):
            try:
                async for raw in ws:
                    data = json.loads(raw)
                    if on_message:
                        on_message(data)
                    else:
                        yield data
            except websockets.ConnectionClosed:
                continue


# ---------------------------------------------------------------
# Synchronous convenience wrapper
# ---------------------------------------------------------------

class MT5ClientSync:
    """
    Synchronous HTTP client using ``httpx.Client`` directly.

    Suitable for scripts and notebooks that don't run an event loop.
    """

    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.Client(base_url=self._base, timeout=timeout)

    def get_symbols(self):
        resp = self._http.get("/api/v1/symbols")
        resp.raise_for_status()
        return resp.json()

    def get_candles(self, symbol, timeframe="M1", from_dt=None, to_dt=None, limit=1000):
        params: dict = {"timeframe": timeframe, "limit": limit}
        if from_dt:
            params["from"] = from_dt if isinstance(from_dt, str) else from_dt.isoformat()
        if to_dt:
            params["to"] = to_dt if isinstance(to_dt, str) else to_dt.isoformat()
        resp = self._http.get(f"/api/v1/candles/{symbol}", params=params)
        resp.raise_for_status()
        return resp.json()

    def get_all_candles(self, symbol, timeframe="M1", from_dt=None, to_dt=None, limit=1000, max_pages=100):
        """Auto-paginate through all candle pages and return a flat list."""
        all_rows = []
        cursor = from_dt
        for _ in range(max_pages):
            page = self.get_candles(symbol, timeframe, cursor, to_dt, limit)
            all_rows.extend(page["data"])
            if not page["has_more"]:
                break
            cursor = page["next_from"]
        return all_rows

    def get_ticks(self, symbol, from_dt=None, to_dt=None, limit=5000):
        params: dict = {"limit": limit}
        if from_dt:
            params["from"] = from_dt if isinstance(from_dt, str) else from_dt.isoformat()
        if to_dt:
            params["to"] = to_dt if isinstance(to_dt, str) else to_dt.isoformat()
        resp = self._http.get(f"/api/v1/ticks/{symbol}", params=params)
        resp.raise_for_status()
        return resp.json()

    def get_all_ticks(self, symbol, from_dt=None, to_dt=None, limit=5000, max_pages=100):
        """Auto-paginate through all tick pages and return a flat list."""
        all_rows = []
        cursor = from_dt
        for _ in range(max_pages):
            page = self.get_ticks(symbol, cursor, to_dt, limit)
            all_rows.extend(page["data"])
            if not page["has_more"]:
                break
            cursor = page["next_from"]
        return all_rows

    def health(self):
        resp = self._http.get("/api/v1/health")
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._http.close()
