"""
Tests for src.mt5.converters (shared conversion utilities).
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from src.mt5.converters import bars_to_dicts, ticks_to_dicts


def _make_bar_array(n: int = 2) -> np.ndarray:
    """Create a fake MT5 bar structured array."""
    dtype = np.dtype([
        ("time", "i8"),
        ("open", "f8"),
        ("high", "f8"),
        ("low", "f8"),
        ("close", "f8"),
        ("tick_volume", "i8"),
        ("real_volume", "i8"),
        ("spread", "i4"),
    ])
    arr = np.zeros(n, dtype=dtype)
    for i in range(n):
        arr[i]["time"] = 1700000000 + i * 60
        arr[i]["open"] = 1.1000 + i * 0.001
        arr[i]["high"] = 1.1010 + i * 0.001
        arr[i]["low"] = 1.0990 + i * 0.001
        arr[i]["close"] = 1.1005 + i * 0.001
        arr[i]["tick_volume"] = 100 + i
        arr[i]["real_volume"] = 0
        arr[i]["spread"] = 2
    return arr


def _make_tick_array(n: int = 3) -> np.ndarray:
    dtype = np.dtype([
        ("time_msc", "i8"),
        ("bid", "f8"),
        ("ask", "f8"),
        ("last", "f8"),
        ("volume", "i8"),
        ("flags", "i4"),
    ])
    arr = np.zeros(n, dtype=dtype)
    for i in range(n):
        arr[i]["time_msc"] = 1700000000000 + i
        arr[i]["bid"] = 1.1000
        arr[i]["ask"] = 1.1002
        arr[i]["last"] = 0.0
        arr[i]["volume"] = 1
        arr[i]["flags"] = 6
    return arr


class TestBarsToDict:
    def test_basic_conversion(self):
        bars = _make_bar_array(2)
        result = bars_to_dicts(bars, "EURUSD", "M1")

        assert len(result) == 2
        assert result[0]["symbol"] == "EURUSD"
        assert result[0]["timeframe"] == "M1"
        assert isinstance(result[0]["time"], datetime)
        assert result[0]["time"].tzinfo == timezone.utc
        assert result[0]["open"] == bars[0]["open"]

    def test_empty(self):
        dtype = np.dtype([
            ("time", "i8"), ("open", "f8"), ("high", "f8"),
            ("low", "f8"), ("close", "f8"), ("tick_volume", "i8"),
            ("real_volume", "i8"), ("spread", "i4"),
        ])
        empty = np.zeros(0, dtype=dtype)
        assert bars_to_dicts(empty, "X", "M1") == []


class TestTicksToDict:
    def test_basic_conversion(self):
        ticks = _make_tick_array(3)
        result = ticks_to_dicts(ticks, "GBPUSD")

        assert len(result) == 3
        assert result[0]["symbol"] == "GBPUSD"
        assert isinstance(result[0]["time_msc"], datetime)
        assert result[0]["bid"] == 1.1000
