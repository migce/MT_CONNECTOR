from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.information_bars_a3c_v6 import (
    A3C_V6_ALGORITHM,
    A3C_V6_TIMEFRAME,
    A3CV6Builder,
    A3CV6Config,
    build_a3c_v6_bars,
)


def _test_config(**overrides: object) -> A3CV6Config:
    values: dict[str, object] = {
        "evidence_budget": 0.50,
        "drift_horizons": (2, 3, 5),
        "drift_weights": (0.20, 0.30, 0.50),
        "scale_lookback": 5,
        "minimum_scale_returns": 2,
        "normalized_minute_return_clip": 2.0,
        "max_duration_ms": 20 * 60_000,
        "hard_max_raw_ticks": 20_000,
        "gap_reset_ms": 300_000,
    }
    values.update(overrides)
    return A3CV6Config(**values)  # type: ignore[arg-type]


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


def _up_endpoints(count: int, start: float = 1.0, step: float = 0.00010) -> list[float]:
    return [start + index * step for index in range(count)]


def test_same_minute_endpoints_ignore_dense_bid_bounce() -> None:
    endpoints = _up_endpoints(18)
    config = _test_config(evidence_budget=1.50)

    sparse = build_a3c_v6_bars(
        _endpoint_ticks(endpoints, dense_bounce=False), config
    )
    dense = build_a3c_v6_bars(
        _endpoint_ticks(endpoints, dense_bounce=True), config
    )

    assert len(sparse) == len(dense)
    assert [bar["closure_reason"] for bar in sparse] == [
        bar["closure_reason"] for bar in dense
    ]
    assert [bar["macro_drift_at_close"] for bar in sparse] == pytest.approx(
        [bar["macro_drift_at_close"] for bar in dense]
    )


def test_completed_minute_drift_expands_trend_and_compresses_flat_endpoints() -> None:
    preparation = _up_endpoints(8)
    continuation = _up_endpoints(30, start=preparation[-1] + 0.00010)
    flat = [preparation[-1]] * 30
    preparation_end = _endpoint_ticks(preparation)[-1]["time_msc"]
    assert isinstance(preparation_end, datetime)

    trend_bars = [
        bar
        for bar in build_a3c_v6_bars(
            _endpoint_ticks(preparation + continuation), _test_config()
        )
        if bar["end_time"] > preparation_end
    ]
    flat_bars = [
        bar
        for bar in build_a3c_v6_bars(
            _endpoint_ticks(preparation + flat), _test_config()
        )
        if bar["end_time"] > preparation_end
    ]

    assert len(trend_bars) >= 4
    assert len(trend_bars) >= 4 * max(1, len(flat_bars))
    assert all(
        bar["closure_reason"] == "completed_minute_drift_evidence"
        for bar in trend_bars
    )
    assert sum(
        bar["closure_reason"] == "completed_minute_drift_evidence"
        for bar in flat_bars
    ) <= 1


def test_current_minute_cannot_change_its_own_macro_state() -> None:
    ticks = _endpoint_ticks(_up_endpoints(10))
    builder = A3CV6Builder(_test_config(evidence_budget=100.0))
    for tick in ticks:
        completed, _current = builder.update(tick)
        assert completed is None

    last_time = ticks[-1]["time_msc"]
    assert isinstance(last_time, datetime)
    next_minute = last_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    builder.update(_tick(float(ticks[-1]["bid"]), next_minute + timedelta(seconds=5)))
    macro_before = builder.macro_drift
    rms_before = builder.minute_rms
    returns_before = builder.completed_minute_returns
    progress_before = builder.progress

    builder.update(
        _tick(float(ticks[-1]["bid"]) + 0.01, next_minute + timedelta(seconds=30))
    )

    assert builder.macro_drift == pytest.approx(macro_before)
    assert builder.minute_rms == pytest.approx(rms_before)
    assert builder.completed_minute_returns == returns_before
    assert builder.progress == pytest.approx(progress_before)


def test_retracement_and_macro_sign_change_cancel_unfinished_progress() -> None:
    builder = A3CV6Builder(_test_config(evidence_budget=100.0))
    up_ticks = _endpoint_ticks(_up_endpoints(12))
    for tick in up_ticks:
        completed, _current = builder.update(tick)
        assert completed is None
    progress_before = builder.progress
    assert builder.macro_drift > 0.5
    assert progress_before > 0

    last_time = up_ticks[-1]["time_msc"]
    last_price = float(up_ticks[-1]["bid"])
    down_endpoints = [last_price - (index + 1) * 0.00020 for index in range(12)]
    down_ticks = _endpoint_ticks(
        down_endpoints,
        start=last_time.replace(second=0, microsecond=0) + timedelta(minutes=1),
    )
    saw_reset = False
    for tick in down_ticks:
        completed, current = builder.update(tick)
        assert completed is None
        if current is not None and int(current["macro_direction_resets"]) > 0:
            saw_reset = True
            break

    assert saw_reset
    assert builder.progress < progress_before


def test_continuation_repeats_evidence_bars_without_macro_warmup() -> None:
    endpoints = _up_endpoints(35)
    bars = build_a3c_v6_bars(_endpoint_ticks(endpoints), _test_config())
    evidence = [
        bar
        for bar in bars
        if bar["closure_reason"] == "completed_minute_drift_evidence"
    ]

    assert len(evidence) >= 4
    assert all(bar["completed_minute_returns_at_close"] >= 2 for bar in evidence)
    assert all(bar["macro_drift_at_close"] > 0.5 for bar in evidence)


def test_builder_is_prefix_and_replay_invariant() -> None:
    ticks = _endpoint_ticks(_up_endpoints(50), dense_bounce=True)
    config = _test_config()
    split = len(ticks) // 2

    prefix = build_a3c_v6_bars(ticks[:split], config)
    replay = build_a3c_v6_bars(ticks[:split], config)
    full = build_a3c_v6_bars(ticks, config)

    assert replay == prefix
    assert full[: len(prefix)] == prefix


def test_completed_ohlc_and_tick_counts_match_exact_assigned_path() -> None:
    ticks = _endpoint_ticks(_up_endpoints(35), dense_bounce=True)
    bars = build_a3c_v6_bars(ticks, _test_config())

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
        assert bar["availability_time"] == bar["end_time"]
        assert bar["is_complete"] is True
        previous_end = bar["end_time"]


def test_gap_resets_completed_minute_state_and_excludes_jump() -> None:
    before_ticks = _endpoint_ticks(_up_endpoints(10))
    before_end = before_ticks[-1]["time_msc"]
    assert isinstance(before_end, datetime)
    after_start = before_end + timedelta(hours=1)
    after_ticks = _endpoint_ticks(
        _up_endpoints(8, start=1.02), start=after_start
    )
    ticks = before_ticks + after_ticks

    bars = build_a3c_v6_bars(
        ticks, _test_config(evidence_budget=100.0), include_incomplete=True
    )
    gap_bar = next(bar for bar in bars if bar.get("closure_reason") == "gap_reset")
    post_gap = bars[bars.index(gap_bar) + 1]

    assert gap_bar["end_time"] == before_ticks[-1]["time_msc"]
    assert post_gap["time"] == after_ticks[0]["time_msc"]
    assert post_gap["open"] == after_ticks[0]["bid"]
    assert post_gap["gap_count_at_open"] == 1
    assert post_gap["path_log_return"] < 0.01
    assert post_gap["completed_minute_returns_at_close"] <= 5


def test_metadata_exposes_research_only_contract() -> None:
    metadata = A3CV6Config().metadata()

    assert metadata["algorithm"] == A3C_V6_ALGORITHM
    assert metadata["timeframe"] == A3C_V6_TIMEFRAME
    assert metadata["strategy_eligible"] is False
    assert metadata["drift_horizons"] == (5, 15, 60)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"evidence_budget": 0},
        {"drift_horizons": (15, 5, 60)},
        {"drift_weights": (0.5, 0.5, 0.5)},
        {"scale_lookback": 30},
        {"minimum_scale_returns": 1},
        {"normalized_minute_return_clip": 0},
        {"max_duration_ms": 0},
    ],
)
def test_invalid_configuration_is_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        A3CV6Config(**kwargs)  # type: ignore[arg-type]
