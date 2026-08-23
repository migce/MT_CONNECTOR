from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.information_bars_v2 import (
    INFORMATION_BAR_V2_ALGORITHM,
    InformationBarV2Config,
    build_information_bars_v2,
)


def _regime_ticks(
    prices: list[float],
    intervals_ms: list[int],
    *,
    symbol: str = "EURUSD",
) -> list[dict[str, object]]:
    assert len(prices) == len(intervals_ms)
    current = datetime(2026, 8, 1, tzinfo=UTC)
    ticks: list[dict[str, object]] = []
    for price, interval_ms in zip(prices, intervals_ms, strict=True):
        current += timedelta(milliseconds=interval_ms)
        ticks.append(
            {
                "time_msc": current,
                "symbol": symbol,
                "bid": price,
                "ask": price + 0.00002,
                "last": price,
                "volume": 1,
            }
        )
    return ticks


def test_target_is_frozen_at_open_and_completed_volume_matches_it() -> None:
    prices = [1.0 + (index % 17) * 0.000001 for index in range(3_000)]
    ticks = _regime_ticks(prices, [20] * len(prices))
    config = InformationBarV2Config(
        neutral_ticks=100,
        fast_span=32,
        slow_span=256,
        efficiency_window=32,
        warmup_ticks=256,
    )

    bars = build_information_bars_v2(ticks, config)

    assert any(bar["regime_warmed"] for bar in bars)
    assert all(bar["tick_volume"] == bar["target_tick_count"] for bar in bars)
    assert all(config.min_target_ticks <= bar["tick_volume"] <= config.max_target_ticks for bar in bars)
    assert all(bar["path_log_return"] >= 0 for bar in bars)
    assert all(0 <= bar["realized_directional_efficiency"] <= 1 for bar in bars)


def test_fast_directional_regime_expands_and_quiet_regime_compresses() -> None:
    warmup_count = 2_000
    active_count = 2_000
    quiet_count = 4_000
    warmup = [1.0 + (0.00001 if index % 2 else 0.0) for index in range(warmup_count)]
    active = [warmup[-1] + (index + 1) * 0.00001 for index in range(active_count)]
    quiet = [active[-1]] * quiet_count
    prices = warmup + active + quiet
    intervals = [100] * warmup_count + [10] * active_count + [1_000] * quiet_count
    config = InformationBarV2Config(
        neutral_ticks=100,
        fast_span=32,
        slow_span=512,
        efficiency_window=64,
        warmup_ticks=512,
    )

    bars = build_information_bars_v2(_regime_ticks(prices, intervals), config)
    active_targets = [
        bar["target_tick_count"]
        for bar in bars
        if warmup_count <= bar["regime_observed_ticks"] < warmup_count + active_count
    ]
    quiet_targets = [
        bar["target_tick_count"]
        for bar in bars
        if bar["regime_observed_ticks"] >= warmup_count + active_count + 500
    ]

    assert min(active_targets) <= 35
    assert max(quiet_targets) >= 300
    assert sum(active_targets) / len(active_targets) < 60
    assert sum(quiet_targets) / len(quiet_targets) > 180


def test_builder_is_prefix_and_replay_invariant() -> None:
    count = 5_000
    prices = [1.0 + index * 0.000001 for index in range(count)]
    ticks = _regime_ticks(prices, [10 + index % 7 for index in range(count)])
    config = InformationBarV2Config(
        neutral_ticks=100,
        fast_span=32,
        slow_span=256,
        efficiency_window=32,
        warmup_ticks=256,
    )

    prefix = build_information_bars_v2(ticks[:3_000], config)
    replay = build_information_bars_v2(ticks[:3_000], config)
    full = build_information_bars_v2(ticks, config)

    assert replay == prefix
    assert full[: len(prefix)] == prefix


def test_large_time_gap_does_not_pollute_next_regime_snapshot() -> None:
    count = 3_000
    prices = [1.0 + (index % 13) * 0.000001 for index in range(count)]
    intervals = [20] * count
    intervals[1_500] = 3_600_000
    config = InformationBarV2Config(
        neutral_ticks=100,
        fast_span=32,
        slow_span=256,
        efficiency_window=32,
        warmup_ticks=256,
    )

    bars = build_information_bars_v2(_regime_ticks(prices, intervals), config)
    before = next(bar for bar in bars if bar["regime_observed_ticks"] >= 1_400)
    after = next(bar for bar in bars if bar["regime_observed_ticks"] >= 1_600)

    assert 0.8 <= before["arrival_ratio_at_open"] <= 1.2
    assert 0.8 <= after["arrival_ratio_at_open"] <= 1.2
    assert after["gap_count_at_open"] == 1


def test_metadata_exposes_experimental_contract_and_bounds() -> None:
    config = InformationBarV2Config(neutral_ticks=1_000)

    assert config.metadata()["algorithm"] == INFORMATION_BAR_V2_ALGORITHM
    assert config.min_target_ticks == 250
    assert config.max_target_ticks == 4_000
