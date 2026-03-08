"""
Tests for src.config.
"""

from __future__ import annotations

import os

import pytest

from src.config import (
    CustomTimeframe,
    Timeframe,
    is_standard_timeframe,
    parse_custom_timeframe,
)


class TestTimeframe:
    def test_seconds(self):
        assert Timeframe.M1.seconds == 60
        assert Timeframe.H1.seconds == 3600
        assert Timeframe.D1.seconds == 86400

    def test_mt5_constant(self):
        assert Timeframe.M1.mt5_constant == 1
        assert Timeframe.H1.mt5_constant == 16385

    def test_from_string(self):
        assert Timeframe.from_string("m1") == Timeframe.M1
        assert Timeframe.from_string("H4") == Timeframe.H4


class TestCustomTimeframe:
    def test_time_based(self):
        ct = parse_custom_timeframe("M2")
        assert ct.seconds == 120
        assert not ct.is_tick_bar

    def test_tick_bar(self):
        ct = parse_custom_timeframe("T500")
        assert ct.is_tick_bar
        assert ct.tick_count == 500

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_custom_timeframe("XYZ")


class TestIsStandard:
    def test_standard(self):
        assert is_standard_timeframe("M1")
        assert is_standard_timeframe("h4")

    def test_non_standard(self):
        assert not is_standard_timeframe("M2")
        assert not is_standard_timeframe("T100")
