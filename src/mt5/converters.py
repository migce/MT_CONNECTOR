"""
Shared MT5 data conversion utilities.

Used by both the real-time collector and the backfill module to convert
numpy structured arrays returned by MetaTrader5 into dicts suitable
for database insertion.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np


def bars_to_dicts(
    bars: np.ndarray, symbol: str, timeframe: str
) -> list[dict[str, Any]]:
    """Convert numpy structured array from MT5 ``copy_rates_*`` into list of dicts."""
    result = []
    for bar in bars:
        result.append({
            "time": datetime.fromtimestamp(int(bar["time"]), tz=timezone.utc),
            "symbol": symbol,
            "timeframe": timeframe,
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"]),
            "tick_volume": int(bar["tick_volume"]),
            "real_volume": int(bar["real_volume"]),
            "spread": int(bar["spread"]),
        })
    return result


def ticks_to_dicts(
    ticks: np.ndarray, symbol: str
) -> list[dict[str, Any]]:
    """Convert numpy structured array from MT5 ``copy_ticks_*`` into list of dicts."""
    result = []
    for t in ticks:
        msc = int(t["time_msc"])
        result.append({
            "time_msc": datetime.fromtimestamp(msc / 1000.0, tz=timezone.utc),
            "symbol": symbol,
            "bid": float(t["bid"]),
            "ask": float(t["ask"]),
            "last": float(t["last"]),
            "volume": int(t["volume"]),
            "flags": int(t["flags"]),
        })
    return result
