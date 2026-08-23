from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.information_bars import (
    INFORMATION_BAR_ALGORITHM,
    InformationBarBuilder,
    InformationBarConfig,
    build_information_bars,
    information_source_limit,
)


def _ticks(prices: list[float], *, symbol: str = "EURUSD") -> list[dict[str, object]]:
    start = datetime(2026, 8, 1, tzinfo=UTC)
    return [
        {
            "time_msc": start + timedelta(milliseconds=index),
            "symbol": symbol,
            "bid": price,
            "ask": price + 0.00002,
            "last": price,
            "volume": 1,
            "flags": 6,
        }
        for index, price in enumerate(prices)
    ]


def test_directional_path_expands_and_two_sided_chop_compresses() -> None:
    count = 4_000
    trend = [1.0 + index * 0.00001 for index in range(count)]
    chop = [1.0 + (0.00001 if index % 2 else 0.0) for index in range(count)]
    config = InformationBarConfig(budget=100)

    trend_bars = build_information_bars(_ticks(trend), config)
    chop_bars = build_information_bars(_ticks(chop), config)

    assert len(trend_bars) >= 35
    assert len(chop_bars) <= 12
    assert len(trend_bars) >= len(chop_bars) * 3
    assert all(bar["is_complete"] is True for bar in trend_bars + chop_bars)


def test_quiet_prices_use_the_minimum_information_weight() -> None:
    config = InformationBarConfig(budget=100)
    bars = build_information_bars(_ticks([1.0] * 800), config, include_incomplete=True)

    assert len(bars) == 2
    assert bars[0]["tick_volume"] == config.max_ticks_per_bar
    assert bars[0]["mean_information_weight"] == pytest.approx(config.min_weight)
    assert bars[1]["is_complete"] is True


def test_builder_is_prefix_and_replay_invariant() -> None:
    prices = [1.0 + index * 0.00001 for index in range(900)]
    ticks = _ticks(prices)
    config = InformationBarConfig(budget=50)

    prefix = build_information_bars(ticks[:600], config)
    replay = build_information_bars(ticks[:600], config)
    full = build_information_bars(ticks, config)

    assert replay == prefix
    assert full[: len(prefix)] == prefix


def test_completed_bar_preserves_research_metadata() -> None:
    config = InformationBarConfig(budget=2, min_weight=1.0)
    builder = InformationBarBuilder(config, price_field="mid")
    ticks = _ticks([1.0, 1.0001])

    assert builder.update(ticks[0])[0] is None
    completed, current = builder.update(ticks[1])

    assert completed is current
    assert completed is not None
    assert completed["timeframe"] == "I2"
    assert completed["tick_volume"] == 2
    assert completed["duration_ms"] == 1
    assert completed["end_time"] == ticks[1]["time_msc"]
    assert completed["information_value"] >= 2
    assert completed["open"] == pytest.approx(1.00001)


def test_source_limit_and_metadata_are_bounded_and_versioned() -> None:
    config = InformationBarConfig(budget=500)
    assert information_source_limit(config, 10) < 1_000_000
    assert information_source_limit(config, 50_000) == 1_000_000
    assert config.metadata()["algorithm"] == INFORMATION_BAR_ALGORITHM
    assert config.metadata()["max_ticks_per_bar"] == 2_000
