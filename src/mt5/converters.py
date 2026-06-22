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

from src.mt5.connection import get_digits


def bars_to_dicts(
    bars: np.ndarray, symbol: str, timeframe: str
) -> list[dict[str, Any]]:
    """Convert numpy structured array from MT5 ``copy_rates_*`` into list of dicts."""
    d = get_digits(symbol)
    result = []
    for bar in bars:
        result.append({
            "time": datetime.fromtimestamp(int(bar["time"]), tz=timezone.utc),
            "symbol": symbol,
            "timeframe": timeframe,
            "open": round(float(bar["open"]), d),
            "high": round(float(bar["high"]), d),
            "low": round(float(bar["low"]), d),
            "close": round(float(bar["close"]), d),
            "tick_volume": int(bar["tick_volume"]),
            "real_volume": int(bar["real_volume"]),
            "spread": int(bar["spread"]),
        })
    return result


def ticks_to_dicts(
    ticks: np.ndarray, symbol: str
) -> list[dict[str, Any]]:
    """Convert numpy structured array from MT5 ``copy_ticks_*`` into list of dicts."""
    d = get_digits(symbol)
    result = []
    for t in ticks:
        msc = int(t["time_msc"])
        result.append({
            "time_msc": datetime.fromtimestamp(msc / 1000.0, tz=timezone.utc),
            "symbol": symbol,
            "bid": round(float(t["bid"]), d),
            "ask": round(float(t["ask"]), d),
            "last": round(float(t["last"]), d),
            "volume": int(t["volume"]),
            "flags": int(t["flags"]),
        })
    return result
