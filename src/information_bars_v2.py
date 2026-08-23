"""Offline causal prototype for adaptive target-tick information bars.

Unlike the deployed v1 information clock, this prototype chooses a concrete
tick target before a bar opens and does not change that target while the bar is
forming.  The choice uses only state accumulated from earlier ticks.

The module is intentionally not wired into REST or WebSocket routes.  It is a
research candidate that must be calibrated and validated before integration.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from src.information_bars import PriceField, _as_datetime, tick_price

if TYPE_CHECKING:
    from datetime import datetime

INFORMATION_BAR_V2_ALGORITHM = "adaptive-target-tick-bars-v2"


@dataclass(frozen=True)
class InformationBarV2Config:
    """Frozen experimental parameters for the v2 causal regime clock."""

    neutral_ticks: int
    fast_span: int = 128
    slow_span: int = 4096
    efficiency_window: int = 256
    efficiency_floor: float = 0.02
    neutral_efficiency: float = 0.08
    warmup_ticks: int = 4096
    arrival_exponent: float = 0.40
    activity_exponent: float = 0.55
    efficiency_exponent: float = 0.65
    min_target_fraction: float = 0.25
    max_target_fraction: float = 4.0
    component_ratio_floor: float = 0.25
    component_ratio_ceiling: float = 4.0
    max_interarrival_ms: int = 10_000
    gap_reset_ms: int = 300_000

    def __post_init__(self) -> None:
        if self.neutral_ticks < 2:
            raise ValueError("neutral_ticks must be >= 2.")
        if min(self.fast_span, self.slow_span, self.efficiency_window) < 2:
            raise ValueError("Regime spans and windows must be >= 2.")
        if self.fast_span >= self.slow_span:
            raise ValueError("fast_span must be below slow_span.")
        if self.warmup_ticks < self.efficiency_window:
            raise ValueError("warmup_ticks must cover efficiency_window.")
        if self.efficiency_floor <= 0 or not 0 < self.neutral_efficiency <= 1:
            raise ValueError("Efficiency floor and neutral level must be positive.")
        if min(
            self.arrival_exponent,
            self.activity_exponent,
            self.efficiency_exponent,
        ) < 0:
            raise ValueError("Regime exponents must be non-negative.")
        if not 0 < self.min_target_fraction <= 1 <= self.max_target_fraction:
            raise ValueError("Target fractions must bracket 1.")
        if not 0 < self.component_ratio_floor <= 1 <= self.component_ratio_ceiling:
            raise ValueError("Component ratio bounds must bracket 1.")
        if not 0 < self.max_interarrival_ms < self.gap_reset_ms:
            raise ValueError("Interarrival cap must be positive and below gap reset.")

    @property
    def min_target_ticks(self) -> int:
        return max(2, round(self.neutral_ticks * self.min_target_fraction))

    @property
    def max_target_ticks(self) -> int:
        return max(self.min_target_ticks, round(self.neutral_ticks * self.max_target_fraction))

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": INFORMATION_BAR_V2_ALGORITHM,
            **asdict(self),
            "min_target_ticks": self.min_target_ticks,
            "max_target_ticks": self.max_target_ticks,
        }


def information_v2_source_limit(
    config: InformationBarV2Config,
    bar_limit: int,
    *,
    hard_cap: int = 1_000_000,
) -> int:
    """Return a bounded source window for an on-demand v2 chart request."""
    requested = (
        config.max_target_ticks * (max(1, bar_limit) + 2)
        + config.warmup_ticks
    )
    return min(hard_cap, requested)


@dataclass(frozen=True)
class RegimeSnapshot:
    """Causal inputs frozen immediately before a bar's first tick."""

    is_warmed: bool
    observed_ticks: int
    arrival_ratio: float
    activity_ratio: float
    efficiency: float
    slow_efficiency: float
    efficiency_ratio: float
    regime_score: float
    target_ticks: int


class InformationBarV2Builder:
    """Stateful target-tick builder used only by the offline experiment."""

    __slots__ = (
        "_bar",
        "_config",
        "_efficiency_abs_sum",
        "_efficiency_returns",
        "_efficiency_sum",
        "_fast_abs_return",
        "_fast_log_dt",
        "_gap_count",
        "_observed_ticks",
        "_previous_price",
        "_previous_time",
        "_price_field",
        "_slow_abs_return",
        "_slow_efficiency",
        "_slow_log_dt",
    )

    def __init__(
        self,
        config: InformationBarV2Config,
        price_field: PriceField = "bid",
    ) -> None:
        self._config = config
        self._price_field = price_field
        self._previous_price: float | None = None
        self._previous_time: datetime | None = None
        self._fast_abs_return: float | None = None
        self._slow_abs_return: float | None = None
        self._fast_log_dt: float | None = None
        self._slow_log_dt: float | None = None
        self._efficiency_returns: deque[float] = deque()
        self._efficiency_sum = 0.0
        self._efficiency_abs_sum = 0.0
        self._slow_efficiency: float | None = None
        self._observed_ticks = 0
        self._gap_count = 0
        self._bar: dict[str, Any] | None = None

    @staticmethod
    def _alpha(span: int) -> float:
        return 2.0 / (span + 1.0)

    @staticmethod
    def _ewma(previous: float | None, value: float, alpha: float) -> float:
        return value if previous is None else previous + alpha * (value - previous)

    def _clip_ratio(self, value: float) -> float:
        return min(
            self._config.component_ratio_ceiling,
            max(self._config.component_ratio_floor, value),
        )

    def _current_efficiency(self) -> float:
        if self._efficiency_abs_sum <= 1e-18:
            return 0.0
        return min(1.0, abs(self._efficiency_sum) / self._efficiency_abs_sum)

    def _ratio(self, fast: float | None, slow: float | None) -> float:
        if fast is None or slow is None:
            return 1.0
        if fast <= 1e-18 and slow <= 1e-18:
            return 1.0
        return self._clip_ratio((fast + 1e-18) / (slow + 1e-18))

    def _regime_snapshot(self) -> RegimeSnapshot:
        efficiency = self._current_efficiency()
        activity_ratio = self._ratio(self._fast_abs_return, self._slow_abs_return)
        if self._fast_log_dt is None or self._slow_log_dt is None:
            arrival_ratio = 1.0
        else:
            arrival_ratio = self._clip_ratio(
                math.exp(self._slow_log_dt - self._fast_log_dt)
            )
        slow_efficiency = self._slow_efficiency or 0.0
        efficiency_ratio = self._clip_ratio(
            (efficiency + self._config.efficiency_floor)
            / (self._config.neutral_efficiency + self._config.efficiency_floor)
        )

        is_warmed = self._observed_ticks >= self._config.warmup_ticks
        if is_warmed:
            log_score = (
                self._config.arrival_exponent * math.log(arrival_ratio)
                + self._config.activity_exponent * math.log(activity_ratio)
                + self._config.efficiency_exponent * math.log(efficiency_ratio)
            )
            score = math.exp(log_score)
            minimum_score = 1.0 / self._config.max_target_fraction
            maximum_score = 1.0 / self._config.min_target_fraction
            score = min(maximum_score, max(minimum_score, score))
            target = round(self._config.neutral_ticks / score)
            target = min(
                self._config.max_target_ticks,
                max(self._config.min_target_ticks, target),
            )
        else:
            score = 1.0
            target = self._config.neutral_ticks

        return RegimeSnapshot(
            is_warmed=is_warmed,
            observed_ticks=self._observed_ticks,
            arrival_ratio=arrival_ratio,
            activity_ratio=activity_ratio,
            efficiency=efficiency,
            slow_efficiency=slow_efficiency,
            efficiency_ratio=efficiency_ratio,
            regime_score=score,
            target_ticks=target,
        )

    def _update_regime(self, price: float, tick_time: datetime) -> None:
        previous_price = self._previous_price
        previous_time = self._previous_time
        self._previous_price = price
        self._previous_time = tick_time
        self._observed_ticks += 1
        if previous_price is None or previous_time is None:
            return

        elapsed_ms = max(0.0, (tick_time - previous_time).total_seconds() * 1000.0)
        if elapsed_ms > self._config.gap_reset_ms:
            self._gap_count += 1
            return

        bounded_ms = min(
            float(self._config.max_interarrival_ms),
            max(1.0, elapsed_ms),
        )
        log_dt = math.log(bounded_ms)
        self._fast_log_dt = self._ewma(
            self._fast_log_dt,
            log_dt,
            self._alpha(self._config.fast_span),
        )
        self._slow_log_dt = self._ewma(
            self._slow_log_dt,
            log_dt,
            self._alpha(self._config.slow_span),
        )

        signed_return = math.log(price / previous_price) if previous_price > 0 else 0.0
        absolute_return = abs(signed_return)
        self._fast_abs_return = self._ewma(
            self._fast_abs_return,
            absolute_return,
            self._alpha(self._config.fast_span),
        )
        self._slow_abs_return = self._ewma(
            self._slow_abs_return,
            absolute_return,
            self._alpha(self._config.slow_span),
        )

        self._efficiency_returns.append(signed_return)
        self._efficiency_sum += signed_return
        self._efficiency_abs_sum += absolute_return
        if len(self._efficiency_returns) > self._config.efficiency_window:
            removed = self._efficiency_returns.popleft()
            self._efficiency_sum -= removed
            self._efficiency_abs_sum -= abs(removed)
        efficiency = self._current_efficiency()
        self._slow_efficiency = self._ewma(
            self._slow_efficiency,
            efficiency,
            self._alpha(self._config.slow_span),
        )

    def _open_bar(
        self,
        tick: dict[str, Any],
        price: float,
        tick_time: datetime,
    ) -> dict[str, Any]:
        snapshot = self._regime_snapshot()
        return {
            "time": tick_time,
            "end_time": tick_time,
            "symbol": str(tick.get("symbol") or ""),
            "timeframe": f"A{self._config.neutral_ticks}",
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "high_time": tick_time,
            "low_time": tick_time,
            "tick_volume": 0,
            "real_volume": 0,
            "spread": 0,
            "mean_spread": 0.0,
            "duration_ms": 0,
            "path_log_return": 0.0,
            "realized_directional_efficiency": 0.0,
            "target_tick_count": snapshot.target_ticks,
            "regime_warmed": snapshot.is_warmed,
            "regime_observed_ticks": snapshot.observed_ticks,
            "arrival_ratio_at_open": snapshot.arrival_ratio,
            "activity_ratio_at_open": snapshot.activity_ratio,
            "directional_efficiency_at_open": snapshot.efficiency,
            "slow_efficiency_at_open": snapshot.slow_efficiency,
            "efficiency_ratio_at_open": snapshot.efficiency_ratio,
            "regime_score_at_open": snapshot.regime_score,
            "gap_count_at_open": self._gap_count,
            "is_complete": False,
        }

    def update(
        self,
        tick: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Feed one tick and return ``(completed, current)``."""
        price = tick_price(tick, self._price_field)
        raw_time = tick.get("time_msc", tick.get("time"))
        if price is None or raw_time is None:
            return None, self._bar
        tick_time = _as_datetime(raw_time)

        if self._bar is None:
            self._bar = self._open_bar(tick, price, tick_time)

        bar = self._bar
        count = int(bar["tick_volume"]) + 1
        if count > 1 and self._previous_price is not None and self._previous_price > 0:
            bar["path_log_return"] = float(bar["path_log_return"]) + abs(
                math.log(price / self._previous_price)
            )
        if price > float(bar["high"]):
            bar["high"] = price
            bar["high_time"] = tick_time
        if price < float(bar["low"]):
            bar["low"] = price
            bar["low_time"] = tick_time
        bar["close"] = price
        bar["end_time"] = tick_time
        bar["tick_volume"] = count
        bar["real_volume"] = int(bar["real_volume"]) + int(tick.get("volume") or 0)
        bid = float(tick.get("bid") or 0)
        ask = float(tick.get("ask") or 0)
        spread = ask - bid if bid > 0 and ask > 0 else 0.0
        bar["mean_spread"] = (
            float(bar["mean_spread"]) * (count - 1) + max(0.0, spread)
        ) / count
        bar["duration_ms"] = max(
            0,
            round((tick_time - _as_datetime(bar["time"])).total_seconds() * 1000),
        )
        if float(bar["path_log_return"]) > 1e-18:
            bar["realized_directional_efficiency"] = min(
                1.0,
                abs(math.log(float(bar["close"]) / float(bar["open"])))
                / float(bar["path_log_return"]),
            )

        self._update_regime(price, tick_time)

        if count >= int(bar["target_tick_count"]):
            bar["is_complete"] = True
            completed = bar
            self._bar = None
            return completed, completed
        return None, bar

    @property
    def current(self) -> dict[str, Any] | None:
        return self._bar


def build_information_bars_v2(
    ticks: list[dict[str, Any]],
    config: InformationBarV2Config,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic v2 bars from an ascending tick prefix."""
    builder = InformationBarV2Builder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
