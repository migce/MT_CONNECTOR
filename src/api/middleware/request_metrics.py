"""
API request metrics middleware.

Counts every HTTP request (excluding WebSocket upgrades) and records
response latency.  Metrics are served via ``/api/v1/stats``.

Uses minute-bucketed counters for O(window_minutes) snapshot instead of
scanning a full history deque.
"""

from __future__ import annotations

import time
import threading
from collections import Counter
from dataclasses import dataclass

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class ApiMetrics:
    """Thread-safe request counter with minute-bucket stats."""

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

    # -- initialisation ---------------------------------------------------

    def _init(self) -> None:
        self._lock = threading.Lock()
        self.total_requests: int = 0
        self.total_errors: int = 0  # 5xx
        self._start_time: float = time.monotonic()

        # Total latency accumulators (for daily persistence)
        self.total_latency_sum_ms: float = 0.0
        self.total_latency_count: int = 0

        # Minute-bucket counters (key = int(time.time()) // 60)
        self._req_buckets: Counter[int] = Counter()
        self._err_buckets: Counter[int] = Counter()
        self._latency_sum_buckets: Counter[int] = Counter()  # sum of latency_ms
        self._latency_cnt_buckets: Counter[int] = Counter()  # count for avg

    @staticmethod
    def _current_minute() -> int:
        return int(time.time()) // 60

    # -- record -----------------------------------------------------------

    def record(self, method: str, path: str, status: int, latency_ms: float) -> None:
        minute = self._current_minute()
        with self._lock:
            self.total_requests += 1
            self._req_buckets[minute] += 1
            self._latency_sum_buckets[minute] += latency_ms
            self._latency_cnt_buckets[minute] += 1
            self.total_latency_sum_ms += latency_ms
            self.total_latency_count += 1
            if status >= 500:
                self.total_errors += 1
                self._err_buckets[minute] += 1

    # -- window helpers (called under lock) -------------------------------

    def _count_since(self, minutes: int) -> int:
        cutoff = self._current_minute() - minutes
        return sum(c for m, c in self._req_buckets.items() if m >= cutoff)

    def _error_count_since(self, minutes: int) -> int:
        cutoff = self._current_minute() - minutes
        return sum(c for m, c in self._err_buckets.items() if m >= cutoff)

    def _avg_latency_since(self, minutes: int) -> float:
        cutoff = self._current_minute() - minutes
        total_lat = sum(v for m, v in self._latency_sum_buckets.items() if m >= cutoff)
        total_cnt = sum(v for m, v in self._latency_cnt_buckets.items() if m >= cutoff)
        return (total_lat / total_cnt) if total_cnt else 0.0

    # -- snapshot (single lock acquisition) -------------------------------

    def snapshot(self) -> dict:
        """Return stats snapshot for the /stats endpoint."""
        with self._lock:
            return {
                "total_requests": self.total_requests,
                "total_errors": self.total_errors,
                "uptime_sec": round(time.monotonic() - self._start_time, 1),
                "requests_1h": self._count_since(60),
                "requests_12h": self._count_since(720),
                "requests_24h": self._count_since(1440),
                "errors_1h": self._error_count_since(60),
                "avg_latency_ms_1h": round(self._avg_latency_since(60), 2),
            }

    # -- housekeeping -----------------------------------------------------

    def prune_old_buckets(self) -> None:
        """Remove buckets older than 25 hours."""
        cutoff = self._current_minute() - (25 * 60)
        with self._lock:
            for bucket in (self._req_buckets, self._err_buckets,
                           self._latency_sum_buckets, self._latency_cnt_buckets):
                stale = [m for m in bucket if m < cutoff]
                for m in stale:
                    del bucket[m]


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
