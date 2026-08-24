"""
Tests for src.config.
"""

from __future__ import annotations

import pytest

from src.config import (
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
        assert not ct.is_information_bar
        assert not ct.is_adaptive_target_bar
        assert ct.tick_count == 500

    def test_information_bar(self):
        ct = parse_custom_timeframe("i500")
        assert not ct.is_tick_bar
        assert ct.is_information_bar
        assert not ct.is_adaptive_target_bar
        assert ct.information_budget == 500
        assert ct.seconds == 0

    def test_adaptive_target_bar(self):
        ct = parse_custom_timeframe("a500")
        assert not ct.is_tick_bar
        assert not ct.is_information_bar
        assert ct.is_adaptive_target_bar
        assert ct.adaptive_target_ticks == 500
        assert ct.seconds == 0

    @pytest.mark.parametrize(
        ("raw", "minutes"),
        [("v7m5", 5), ("V7M15", 15), ("V7M30", 30), ("V7M60", 60)],
    )
    def test_a3c_v7_visual_presets(self, raw: str, minutes: int):
        ct = parse_custom_timeframe(raw)
        assert ct.raw == f"V7M{minutes}"
        assert ct.is_a3c_v7_bar
        assert not ct.is_tick_bar
        assert not ct.is_information_bar
        assert not ct.is_adaptive_target_bar
        assert ct.a3c_v7_analog_minutes == minutes
        assert ct.seconds == 0

    @pytest.mark.parametrize("raw", ["V7M1", "V7M10", "V7M45", "V7M120"])
    def test_unknown_a3c_v7_visual_presets_are_rejected(self, raw: str):
        with pytest.raises(ValueError):
            parse_custom_timeframe(raw)

    def test_websocket_accepts_only_frozen_a3c_v7_visual_presets(self):
        from src.api.websocket.streams import _validate_ws_timeframe

        assert all(
            _validate_ws_timeframe(timeframe)
            for timeframe in ("V7M5", "V7M15", "V7M30", "V7M60")
        )
        assert not _validate_ws_timeframe("V7M10")

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
        assert not is_standard_timeframe("I100")
        assert not is_standard_timeframe("A100")
        assert not is_standard_timeframe("V7M15")
