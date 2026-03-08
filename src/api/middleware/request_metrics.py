"""
API request metrics middleware.

Counts every HTTP request (excluding WebSocket upgrades) and records
response latency.  Metrics are served via ``/api/v1/stats``.
"""

from __future__ import annotations

import time
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


@dataclass
class _RequestRecord:
    ts: float  # time.monotonic()
    latency_ms: float
    status: int
    method: str
    path: str


class ApiMetrics:
    """Thread-safe request counter with sliding-window stats."""

    _instance: "ApiMetrics | None" = None
    _lock_cls = threading.Lock()

    def __new__(cls) -> "ApiMetrics":
        with cls._lock_cls:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init()
            return cls._instance

    @classmethod
    def reset(cls) -> None:
        with cls._lock_cls:
            cls._instance = None

    def _init(self) -> None:
        self._lock = threading.Lock()
        self.total_requests: int = 0
        self.total_errors: int = 0  # 5xx
        # Keep timestamps for windowed counting (last 7 days max ~604800 entries)
        # At 1 req/s that's ~600K entries ≈ 50 MB — acceptable.
        # In practice API traffic is much lower.
        self._history: deque[_RequestRecord] = deque(maxlen=700_000)
        self._start_time: float = time.monotonic()

    def record(self, method: str, path: str, status: int, latency_ms: float) -> None:
        now = time.monotonic()
        rec = _RequestRecord(ts=now, latency_ms=latency_ms, status=status, method=method, path=path)
        with self._lock:
            self.total_requests += 1
            if status >= 500:
                self.total_errors += 1
            self._history.append(rec)

    def _count_since(self, seconds: float) -> int:
        cutoff = time.monotonic() - seconds
        with self._lock:
            return sum(1 for r in self._history if r.ts >= cutoff)

    def _avg_latency_since(self, seconds: float) -> float:
        cutoff = time.monotonic() - seconds
        with self._lock:
            recent = [r.latency_ms for r in self._history if r.ts >= cutoff]
        if not recent:
            return 0.0
        return sum(recent) / len(recent)

    def _error_count_since(self, seconds: float) -> int:
        cutoff = time.monotonic() - seconds
        with self._lock:
            return sum(1 for r in self._history if r.ts >= cutoff and r.status >= 500)

    def snapshot(self) -> dict:
        """Return stats snapshot for the /stats endpoint."""
        return {
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "uptime_sec": round(time.monotonic() - self._start_time, 1),
            "requests_1h": self._count_since(3600),
            "requests_12h": self._count_since(43200),
            "requests_24h": self._count_since(86400),
            "errors_1h": self._error_count_since(3600),
            "avg_latency_ms_1h": round(self._avg_latency_since(3600), 2),
        }


class RequestMetricsMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that records every request into ApiMetrics."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Skip WebSocket upgrade requests
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        latency_ms = (time.monotonic() - start) * 1000

        metrics = ApiMetrics()
        metrics.record(
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            latency_ms=latency_ms,
        )
        return response
