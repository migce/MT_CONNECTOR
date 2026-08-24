from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.information_bars_a3c import (
    A3C_ALGORITHM,
    A3C_TIMEFRAME,
    A3CConfig,
    build_a3c_bars,
)


def _test_config(**overrides: object) -> A3CConfig:
    values: dict[str, object] = {
        "entry_cusum": 4.0,
        "evidence_budget": 6.0,
        "window_seconds": (2, 8, 20),
        "window_weights": (0.50, 0.30, 0.20),
        "minimum_window_events": 4,
        "scale_span": 32,
        "warmup_price_events": 32,
        "cusum_allowance": 0.10,
        "drift_strength_full_scale": 2.0,
        "entry_score": 0.55,
        "exit_score": 0.25,
        "exit_confirmation_events": 6,
        "neutral_max_duration_ms": 60_000,
        "neutral_max_price_events": 200,
        "trend_max_duration_ms": 10_000,
        "trend_max_price_events": 40,
        "hard_max_raw_ticks": 10_000,
        "gap_reset_ms": 300_000,
    }
    values.update(overrides)
    return A3CConfig(**values)  # type: ignore[arg-type]


def _ticks(
    prices: list[float],
    *,
    intervals_ms: int | list[int] = 100,
    start: datetime | None = None,
) -> list[dict[str, object]]:
    intervals = (
        [intervals_ms] * len(prices)
        if isinstance(intervals_ms, int)
        else intervals_ms
    )
    assert len(prices) == len(intervals)
    current = start or datetime(2026, 8, 1, tzinfo=UTC)
    output: list[dict[str, object]] = []
    for price, interval in zip(prices, intervals, strict=True):
        current += timedelta(milliseconds=interval)
        output.append(
            {
                "time_msc": current,
                "symbol": "EURUSD",
                "bid": price,
                "ask": price + 0.00002,
                "last": price,
                "volume": 1,
            }
        )
    return output


def _alternating(count: int, start: float = 1.0, step: float = 0.00001) -> list[float]:
    return [start + (step if index % 2 else 0.0) for index in range(count)]


def test_directional_evidence_expands_trend_and_compresses_equal_chop() -> None:
    warmup = _alternating(200)
    trend = [warmup[-1] + (index + 1) * 0.00001 for index in range(500)]
    chop = _alternating(500, start=warmup[-1], step=0.00001)
    config = _test_config()

    trend_bars = build_a3c_bars(_ticks(warmup + trend), config)
    chop_bars = build_a3c_bars(_ticks(warmup + chop), config)
    trend_start = _ticks(warmup)[-1]["time_msc"]
    chop_start = trend_start
    assert isinstance(trend_start, datetime)

    trend_structural = [
        bar
        for bar in trend_bars
        if bar["end_time"] > trend_start
        and bar["closure_reason"] in {"trend_entry", "trend_evidence", "trend_event_guard"}
    ]
    chop_structural = [
        bar
        for bar in chop_bars
        if bar["end_time"] > chop_start
        and bar["closure_reason"] in {"trend_entry", "trend_evidence", "trend_event_guard"}
    ]

    assert len(trend_structural) >= 4 * max(1, len(chop_structural))
    assert any(bar["closure_reason"] == "trend_evidence" for bar in trend_structural)
    assert not any(bar["closure_reason"] == "trend_evidence" for bar in chop_structural)


def test_duplicate_quote_storm_does_not_advance_or_close_clock() -> None:
    config = _test_config(neutral_max_duration_ms=45_000)
    sparse = _ticks([1.0] * 30, intervals_ms=1_000)
    dense = _ticks([1.0] * 3_000, intervals_ms=10)

    sparse_bars = build_a3c_bars(sparse, config, include_incomplete=True)
    dense_bars = build_a3c_bars(dense, config, include_incomplete=True)

    assert len(sparse_bars) == len(dense_bars) == 1
    assert sparse_bars[0]["is_complete"] is False
    assert dense_bars[0]["is_complete"] is False
    assert sparse_bars[0]["price_event_count"] == 0
    assert dense_bars[0]["price_event_count"] == 0
    assert dense_bars[0]["tick_volume"] == 3_000


def test_established_trend_reversal_cuts_bar_causally() -> None:
    warmup = _alternating(200)
    up = [warmup[-1] + (index + 1) * 0.00001 for index in range(200)]
    peak = up[-1]
    down = [peak - (index + 1) * 0.00001 for index in range(80)]
    ticks = _ticks(warmup + up + down)
    reversal_start = ticks[len(warmup) + len(up) - 1]["time_msc"]
    assert isinstance(reversal_start, datetime)

    bars = build_a3c_bars(ticks, _test_config())
    reversal = next(bar for bar in bars if bar["closure_reason"] == "trend_reversal")
    events_after_peak = sum(
        1
        for tick in ticks
        if reversal_start < tick["time_msc"] <= reversal["end_time"]
    )

    assert reversal["regime_at_open"] == 1
    assert reversal["regime_at_close"] == -1
    assert events_after_peak <= 20
    assert reversal["availability_time"] == reversal["end_time"]


def test_builder_is_prefix_and_replay_invariant() -> None:
    warmup = _alternating(200)
    movement = [warmup[-1] + (index + 1) * 0.000005 for index in range(600)]
    ticks = _ticks(warmup + movement)
    config = _test_config()

    prefix = build_a3c_bars(ticks[:500], config)
    replay = build_a3c_bars(ticks[:500], config)
    full = build_a3c_bars(ticks, config)

    assert replay == prefix
    assert full[: len(prefix)] == prefix


def test_completed_ohlc_and_tick_counts_match_exact_assigned_path() -> None:
    warmup = _alternating(200)
    movement = [warmup[-1] + (index + 1) * 0.00001 for index in range(400)]
    ticks = _ticks(warmup + movement)
    bars = build_a3c_bars(ticks, _test_config())

    previous_end: datetime | None = None
    for bar in bars:
        assigned = [
            tick
            for tick in ticks
            if (previous_end is None or tick["time_msc"] > previous_end)
            and tick["time_msc"] <= bar["end_time"]
        ]
        prices = [float(tick["bid"]) for tick in assigned]
        assert len(prices) == bar["tick_volume"]
        assert prices[0] == bar["open"]
        assert prices[-1] == bar["close"]
        assert max(prices) == bar["high"]
        assert min(prices) == bar["low"]
        assert bar["time"] == assigned[0]["time_msc"]
        assert bar["is_complete"] is True
        previous_end = bar["end_time"]


def test_gap_closes_old_segment_without_cross_gap_duration_or_return() -> None:
    before = _alternating(120)
    after_start = before[-1] + 0.01
    after = [after_start + index * 0.00001 for index in range(80)]
    intervals = [100] * len(before) + [3_600_000] + [100] * (len(after) - 1)
    ticks = _ticks(before + after, intervals_ms=intervals)

    bars = build_a3c_bars(ticks, _test_config(), include_incomplete=True)
    gap_bar = next(bar for bar in bars if bar.get("closure_reason") == "gap_reset")
    post_gap = bars[bars.index(gap_bar) + 1]

    assert gap_bar["end_time"] == ticks[len(before) - 1]["time_msc"]
    assert gap_bar["duration_ms"] < 60_000
    assert post_gap["time"] == ticks[len(before)]["time_msc"]
    assert post_gap["open"] == after[0]
    assert post_gap["gap_count_at_open"] == 1
    assert post_gap["path_log_return"] < 0.01


def test_metadata_exposes_research_only_contract() -> None:
    config = A3CConfig()
    metadata = config.metadata()

    assert metadata["algorithm"] == A3C_ALGORITHM
    assert metadata["timeframe"] == A3C_TIMEFRAME
    assert metadata["strategy_eligible"] is False
    assert metadata["reversal_cusum"] == pytest.approx(6.0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"entry_cusum": 0},
        {"window_seconds": (300, 60, 900)},
        {"window_weights": (0.5, 0.5, 0.5)},
        {"exit_score": 0.7},
        {"counter_movement_penalty": 0.5},
        {"trend_max_duration_ms": 3_000_000},
    ],
)
def test_invalid_configuration_is_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        A3CConfig(**kwargs)  # type: ignore[arg-type]
