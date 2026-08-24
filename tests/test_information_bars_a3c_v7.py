from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.information_bars_a3c_v7 import (
    A3C_V7_ALGORITHM,
    A3C_V7_TIMEFRAME,
    A3CV7Builder,
    A3CV7Config,
    build_a3c_v7_bars,
)


def _config(**overrides: object) -> A3CV7Config:
    values: dict[str, object] = {
        "evidence_budget": 0.75,
        "trend_max_duration_ms": 5 * 60_000,
        "drift_horizons": (2, 3, 5),
        "drift_weights": (0.20, 0.30, 0.50),
        "scale_lookback": 5,
        "minimum_scale_returns": 2,
        "trend_minimum_returns": 2,
        "normalized_minute_return_clip": 2.0,
        "confidence_floor": 0.35,
        "counter_return_penalty": 1.25,
        "trend_drift_threshold": 0.25,
        "max_duration_ms": 20 * 60_000,
        "hard_max_raw_ticks": 20_000,
        "gap_reset_ms": 300_000,
    }
    values.update(overrides)
    return A3CV7Config(**values)  # type: ignore[arg-type]


def _tick(price: float, time: datetime) -> dict[str, object]:
    return {
        "time_msc": time,
        "symbol": "EURUSD",
        "bid": price,
        "ask": price + 0.00002,
        "last": price,
        "volume": 1,
    }


def _endpoint_ticks(
    endpoints: list[float],
    *,
    start: datetime | None = None,
    dense_bounce: bool = False,
    bounce: float = 0.000002,
) -> list[dict[str, object]]:
    origin = start or datetime(2026, 8, 1, tzinfo=UTC)
    output: list[dict[str, object]] = []
    previous = endpoints[0]
    for minute, endpoint in enumerate(endpoints):
        minute_start = origin + timedelta(minutes=minute)
        if dense_bounce:
            for second in range(2, 50, 2):
                fraction = second / 50.0
                center = previous + fraction * (endpoint - previous)
                price = center + (bounce if second % 4 else -bounce)
                output.append(_tick(price, minute_start + timedelta(seconds=second)))
        else:
            midpoint = previous + 0.5 * (endpoint - previous)
            output.append(_tick(midpoint, minute_start + timedelta(seconds=20)))
        output.append(_tick(endpoint, minute_start + timedelta(seconds=50)))
        previous = endpoint
    return output


def _up(count: int, start: float = 1.0, step: float = 0.00010) -> list[float]:
    return [start + index * step for index in range(count)]


def test_metadata_is_offline_v7_contract() -> None:
    metadata = A3CV7Config().metadata()

    assert metadata["algorithm"] == A3C_V7_ALGORITHM
    assert metadata["timeframe"] == A3C_V7_TIMEFRAME
    assert metadata["strategy_eligible"] is False
    assert metadata["confidence_floor"] == pytest.approx(0.35)
    assert metadata["counter_return_penalty"] == pytest.approx(1.25)


def test_invalid_dual_clock_parameters_are_rejected() -> None:
    with pytest.raises(ValueError, match="trend duration"):
        A3CV7Config(trend_max_duration_ms=121 * 60_000)
    with pytest.raises(ValueError, match="confidence_floor"):
        A3CV7Config(confidence_floor=0.0)
    with pytest.raises(ValueError, match="counter_return_penalty"):
        A3CV7Config(counter_return_penalty=0.9)


def test_same_endpoints_ignore_dense_intraminute_bounce() -> None:
    endpoints = _up(30)
    config = _config(evidence_budget=1.5)
    sparse = build_a3c_v7_bars(_endpoint_ticks(endpoints), config)
    dense = build_a3c_v7_bars(_endpoint_ticks(endpoints, dense_bounce=True), config)

    assert [bar["closure_reason"] for bar in sparse] == [bar["closure_reason"] for bar in dense]
    assert [bar["end_time"].replace(second=0, microsecond=0) for bar in sparse] == [
        bar["end_time"].replace(second=0, microsecond=0) for bar in dense
    ]
    assert [bar["progress_at_close"] for bar in sparse] == pytest.approx([bar["progress_at_close"] for bar in dense])


def test_trend_expands_while_flat_endpoints_remain_compressed() -> None:
    preparation = _up(8)
    continuation = _up(32, start=preparation[-1] + 0.00010)
    flat = [preparation[-1]] * 32
    preparation_end = _endpoint_ticks(preparation)[-1]["time_msc"]
    assert isinstance(preparation_end, datetime)

    trend = [
        bar
        for bar in build_a3c_v7_bars(_endpoint_ticks(preparation + continuation), _config())
        if bar["end_time"] > preparation_end
    ]
    neutral = [
        bar
        for bar in build_a3c_v7_bars(_endpoint_ticks(preparation + flat), _config())
        if bar["end_time"] > preparation_end
    ]

    assert len(trend) >= 4
    assert len(trend) >= 4 * max(1, len(neutral))
    assert sum(bar["closure_reason"].startswith("completed_minute") for bar in trend) >= 4


def test_unreachable_evidence_uses_trend_duration_not_neutral_guard() -> None:
    config = _config(evidence_budget=100.0, trend_max_duration_ms=5 * 60_000)
    trend = build_a3c_v7_bars(_endpoint_ticks(_up(25)), config)
    flat = build_a3c_v7_bars(_endpoint_ticks([1.0] * 25), config)

    assert any(bar["closure_reason"] == "completed_minute_trend_duration" for bar in trend)
    assert not any(bar["closure_reason"] == "completed_minute_trend_duration" for bar in flat)


def test_current_minute_cannot_change_macro_or_progress() -> None:
    ticks = _endpoint_ticks(_up(12))
    builder = A3CV7Builder(_config(evidence_budget=100.0))
    for tick in ticks:
        builder.update(tick)
    last_time = ticks[-1]["time_msc"]
    assert isinstance(last_time, datetime)
    next_minute = last_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    builder.update(_tick(float(ticks[-1]["bid"]), next_minute + timedelta(seconds=5)))
    before = (
        builder.macro_drift,
        builder.minute_rms,
        builder.completed_minute_returns,
        builder.progress,
    )

    builder.update(_tick(float(ticks[-1]["bid"]) + 0.01, next_minute + timedelta(seconds=30)))

    assert builder.macro_drift == pytest.approx(before[0])
    assert builder.minute_rms == pytest.approx(before[1])
    assert builder.completed_minute_returns == before[2]
    assert builder.progress == pytest.approx(before[3])


def test_counter_return_cancels_progress_and_sign_change_resets() -> None:
    builder = A3CV7Builder(_config(evidence_budget=100.0, trend_max_duration_ms=20 * 60_000))
    up_ticks = _endpoint_ticks(_up(12))
    for tick in up_ticks:
        builder.update(tick)
    progress_before = builder.progress
    assert progress_before > 0

    last_time = up_ticks[-1]["time_msc"]
    assert isinstance(last_time, datetime)
    last_price = float(up_ticks[-1]["bid"])
    down_ticks = _endpoint_ticks(
        [last_price - (index + 1) * 0.00020 for index in range(12)],
        start=last_time.replace(second=0, microsecond=0) + timedelta(minutes=1),
    )
    saw_reset = False
    for tick in down_ticks:
        _completed, current = builder.update(tick)
        if current is not None and int(current["macro_direction_resets"]) > 0:
            saw_reset = True
            break

    assert saw_reset
    assert builder.progress < progress_before


def test_continuation_repeats_purposeful_boundaries() -> None:
    bars = build_a3c_v7_bars(_endpoint_ticks(_up(45)), _config())
    purposeful = [bar for bar in bars if bar["closure_reason"].startswith("completed_minute")]

    assert len(purposeful) >= 5
    assert all(bar["completed_minute_returns_at_close"] >= 2 for bar in purposeful)
    assert all(bar["timeframe"] == A3C_V7_TIMEFRAME for bar in purposeful)


def test_prefix_replay_and_exact_partition() -> None:
    ticks = _endpoint_ticks(_up(45), dense_bounce=True)
    config = _config()
    split = len(ticks) // 2
    prefix = build_a3c_v7_bars(ticks[:split], config)
    replay = build_a3c_v7_bars(ticks[:split], config)
    full = build_a3c_v7_bars(ticks, config)

    assert prefix == replay
    assert full[: len(prefix)] == prefix

    previous_end: datetime | None = None
    for bar in full:
        assigned = [
            tick
            for tick in ticks
            if (previous_end is None or tick["time_msc"] > previous_end) and tick["time_msc"] <= bar["end_time"]
        ]
        prices = [float(tick["bid"]) for tick in assigned]
        assert len(prices) == bar["tick_volume"]
        assert prices[0] == bar["open"]
        assert prices[-1] == bar["close"]
        assert max(prices) == bar["high"]
        assert min(prices) == bar["low"]
        assert bar["availability_time"] == bar["end_time"]
        assert bar["is_complete"] is True
        previous_end = bar["end_time"]


def test_gap_closes_and_resets_without_jump() -> None:
    before = _endpoint_ticks(_up(10))
    before_end = before[-1]["time_msc"]
    assert isinstance(before_end, datetime)
    after = _endpoint_ticks(_up(8, start=1.02), start=before_end + timedelta(hours=1))
    bars = build_a3c_v7_bars(
        before + after,
        _config(evidence_budget=100.0, trend_max_duration_ms=20 * 60_000),
        include_incomplete=True,
    )

    gap = next(bar for bar in bars if bar.get("closure_reason") == "gap_reset")
    post_gap = bars[bars.index(gap) + 1]
    assert gap["end_time"] == before[-1]["time_msc"]
    assert post_gap["time"] == after[0]["time_msc"]
    assert post_gap["open"] == after[0]["bid"]
    assert post_gap["gap_count_at_open"] == 1
    assert post_gap["path_log_return"] < 0.01
    assert post_gap["completed_minute_returns_at_close"] <= 5
