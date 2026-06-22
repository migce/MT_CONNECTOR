"""
Request validation helpers for API routes.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict

from fastapi import HTTPException

from src.config import get_settings
from src.api.symbol_registry import is_symbol_available
from src.active_symbols import touch_symbol as _touch_symbol

# Background tasks for fire-and-forget Redis writes
_bg_tasks: set[asyncio.Task] = set()


def validate_symbol(symbol: str) -> str:
    """
    Validate that *symbol* is available on the MT5 broker.

    Checks against the dynamic symbol registry (populated from MT5
    ``symbols_get``).  Returns the upper-cased symbol or raises 404.

    Also records the access so the poller starts actively polling
    the symbol (auto-expires after 7 days of inactivity).
    """
    symbol = symbol.upper()
    if not is_symbol_available(symbol):
        raise HTTPException(
            status_code=404,
            detail=f"Symbol '{symbol}' is not available on the MT5 broker.",
        )
    # Fire-and-forget: mark symbol as recently accessed
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_touch_symbol(symbol))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)
    except RuntimeError:
        pass  # no running loop (e.g. in sync tests)
    return symbol


# -----------------------------------------------------------------------
# Simple per-symbol rate limiter for backfill requests
# -----------------------------------------------------------------------

class _BackfillRateLimiter:
    """
    Token-bucket rate limiter: at most *max_calls* backfill triggers
    per *window_sec* seconds per symbol.
    """

    def __init__(self, max_calls: int = 3, window_sec: float = 60.0) -> None:
        self._max = max_calls
        self._window = window_sec
        self._timestamps: dict[str, list[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def check(self, symbol: str) -> None:
        """Raise 429 if the rate limit has been exceeded."""
        now = time.monotonic()
        async with self._lock:
            history = self._timestamps[symbol]
            # Prune old entries
            self._timestamps[symbol] = [
                t for t in history if now - t < self._window
            ]
            if len(self._timestamps[symbol]) >= self._max:
                raise HTTPException(
                    status_code=429,
                    detail=f"Backfill rate limit exceeded for {symbol}. "
                           f"Max {self._max} requests per {self._window}s.",
                )
            self._timestamps[symbol].append(now)


backfill_limiter = _BackfillRateLimiter(max_calls=3, window_sec=60.0)
