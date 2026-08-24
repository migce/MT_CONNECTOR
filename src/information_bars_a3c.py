"""Strictly causal trend-evidence bars for offline A3C research.

A3C advances an evidence clock only on meaningful price changes. Sustained
directional movement accelerates bar completion, while alternating movement
cancels its own evidence. The sampling rate may change inside a forming bar,
but completed bars are immutable and never retrospectively split or revised.

This module is intentionally not wired into REST, WebSocket, Monitor or any
strategy path. It is a research candidate that must pass the frozen protocol
before visual integration is considered.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

from src.information_bars import PriceField, _as_datetime, tick_price

if TYPE_CHECKING:
    from datetime import datetime

A3C_ALGORITHM = "causal-trend-evidence-bars-v3-a3c"
A3C_TIMEFRAME = "A3C"
Regime = Literal[-1, 0, 1]


@dataclass(frozen=True)
class A3CConfig:
    """Frozen common A3C configuration plus two calibration dimensions."""

    entry_cusum: float = 8.0
    evidence_budget: float = 24.0
    reversal_cusum_fraction: float = 0.75
    window_seconds: tuple[int, int, int] = (60, 300, 900)
    window_weights: tuple[float, float, float] = (0.50, 0.30, 0.20)
    short_window_blend: float = 0.70
    minimum_window_events: int = 8
    scale_span: int = 256
    warmup_price_events: int = 256
    cusum_allowance: float = 0.15
    drift_strength_full_scale: float = 2.5
    entry_score: float = 0.60
    exit_score: float = 0.30
    exit_confirmation_events: int = 16
    standardized_return_clip: float = 4.0
    scale_floor: float = 1e-10
    counter_movement_penalty: float = 1.5
    neutral_max_duration_ms: int = 45 * 60 * 1000
    neutral_max_price_events: int = 1_500
    trend_max_duration_ms: int = 10 * 60 * 1000
    trend_max_price_events: int = 400
    hard_max_raw_ticks: int = 100_000
    gap_reset_ms: int = 5 * 60 * 1000

    def __post_init__(self) -> None:
        if self.entry_cusum <= 0 or self.evidence_budget <= 0:
            raise ValueError("entry_cusum and evidence_budget must be positive")
        if not 0 < self.reversal_cusum_fraction <= 1:
            raise ValueError("reversal_cusum_fraction must be in (0, 1]")
        if len(self.window_seconds) != 3 or len(self.window_weights) != 3:
            raise ValueError("A3C requires exactly three physical-time windows")
        if tuple(sorted(self.window_seconds)) != self.window_seconds:
            raise ValueError("window_seconds must be strictly ascending")
        if min(self.window_seconds) <= 0 or min(self.window_weights) <= 0:
            raise ValueError("window lengths and weights must be positive")
        if not math.isclose(sum(self.window_weights), 1.0, abs_tol=1e-12):
            raise ValueError("window_weights must sum to one")
        if not 0 <= self.short_window_blend <= 1:
            raise ValueError("short_window_blend must be in [0, 1]")
        if min(self.minimum_window_events, self.scale_span, self.warmup_price_events) < 2:
            raise ValueError("event windows, scale and warmup must be >= 2")
        if self.warmup_price_events < self.minimum_window_events:
            raise ValueError("warmup must cover the minimum eligible window")
        if self.cusum_allowance < 0 or self.drift_strength_full_scale <= 0:
            raise ValueError("CUSUM allowance and drift scale are invalid")
        if not 0 <= self.exit_score < self.entry_score <= 1:
            raise ValueError("scores must satisfy 0 <= exit < entry <= 1")
        if self.exit_confirmation_events < 1:
            raise ValueError("exit_confirmation_events must be positive")
        if self.standardized_return_clip <= 0 or self.scale_floor <= 0:
            raise ValueError("standardized return bounds must be positive")
        if self.counter_movement_penalty < 1:
            raise ValueError("counter_movement_penalty must be >= 1")
        if min(
            self.neutral_max_duration_ms,
            self.neutral_max_price_events,
            self.trend_max_duration_ms,
            self.trend_max_price_events,
            self.hard_max_raw_ticks,
            self.gap_reset_ms,
        ) <= 0:
            raise ValueError("all liveness and gap bounds must be positive")
        if self.trend_max_duration_ms > self.neutral_max_duration_ms:
            raise ValueError("trend duration bound must not exceed neutral bound")
        if self.trend_max_price_events > self.neutral_max_price_events:
            raise ValueError("trend event bound must not exceed neutral bound")

    @property
    def reversal_cusum(self) -> float:
        return self.entry_cusum * self.reversal_cusum_fraction

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": A3C_ALGORITHM,
            "timeframe": A3C_TIMEFRAME,
            "strategy_eligible": False,
            **asdict(self),
            "reversal_cusum": self.reversal_cusum,
        }


class _ReturnWindow:
    __slots__ = ("abs_sum", "entries", "return_sum", "seconds", "square_sum")

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self.entries: deque[tuple[datetime, float]] = deque()
        self.return_sum = 0.0
        self.abs_sum = 0.0
        self.square_sum = 0.0

    def append(self, event_time: datetime, value: float) -> None:
        self.entries.append((event_time, value))
        self.return_sum += value
        self.abs_sum += abs(value)
        self.square_sum += value * value
        cutoff = event_time - timedelta(seconds=self.seconds)
        while self.entries and self.entries[0][0] < cutoff:
            _expired_time, expired = self.entries.popleft()
            self.return_sum -= expired
            self.abs_sum -= abs(expired)
            self.square_sum -= expired * expired

    def quality(
        self,
        direction: Regime,
        *,
        minimum_events: int,
        drift_strength_full_scale: float,
    ) -> tuple[bool, float]:
        if len(self.entries) < minimum_events:
            return False, 0.0
        if self.return_sum == 0 or (1 if self.return_sum > 0 else -1) != direction:
            return True, 0.0
        efficiency = min(1.0, abs(self.return_sum) / max(self.abs_sum, 1e-18))
        strength = abs(self.return_sum) / max(math.sqrt(max(0.0, self.square_sum)), 1e-18)
        return True, efficiency * min(1.0, strength / drift_strength_full_scale)


@dataclass(frozen=True)
class _SignalUpdate:
    is_price_event: bool
    standardized_return: float
    previous_regime: Regime
    regime: Regime
    positive_cusum: float
    negative_cusum: float
    trend_score: float

    @property
    def state_changed(self) -> bool:
        return self.previous_regime != self.regime


class A3CBuilder:
    """One-pass strictly causal A3C builder."""

    __slots__ = (
        "_bar",
        "_bar_progress",
        "_below_exit_events",
        "_config",
        "_gap_count",
        "_negative_cusum",
        "_observed_price_events",
        "_positive_cusum",
        "_previous_price",
        "_previous_time",
        "_price_field",
        "_regime",
        "_return_scale",
        "_windows",
    )

    def __init__(self, config: A3CConfig | None = None, price_field: PriceField = "bid") -> None:
        self._config = config or A3CConfig()
        self._price_field = price_field
        self._bar: dict[str, Any] | None = None
        self._bar_progress = 0.0
        self._previous_price: float | None = None
        self._previous_time: datetime | None = None
        self._return_scale: float | None = None
        self._observed_price_events = 0
        self._positive_cusum = 0.0
        self._negative_cusum = 0.0
        self._regime: Regime = 0
        self._below_exit_events = 0
        self._gap_count = 0
        self._windows = tuple(_ReturnWindow(seconds) for seconds in self._config.window_seconds)

    @staticmethod
    def _alpha(span: int) -> float:
        return 2.0 / (span + 1.0)

    @staticmethod
    def _ewma(previous: float | None, value: float, alpha: float) -> float:
        return value if previous is None else previous + alpha * (value - previous)

    def _reset_signal(self) -> None:
        self._previous_price = None
        self._previous_time = None
        self._return_scale = None
        self._observed_price_events = 0
        self._positive_cusum = 0.0
        self._negative_cusum = 0.0
        self._regime = 0
        self._below_exit_events = 0
        self._bar_progress = 0.0
        self._windows = tuple(_ReturnWindow(seconds) for seconds in self._config.window_seconds)

    def _direction_score(self, direction: Regime) -> float:
        if direction == 0:
            return 0.0
        eligible_weight = 0.0
        weighted_quality = 0.0
        short_eligible = False
        short_quality = 0.0
        for index, (window, weight) in enumerate(
            zip(self._windows, self._config.window_weights, strict=True)
        ):
            eligible, quality = window.quality(
                direction,
                minimum_events=self._config.minimum_window_events,
                drift_strength_full_scale=self._config.drift_strength_full_scale,
            )
            if not eligible:
                continue
            eligible_weight += weight
            weighted_quality += weight * quality
            if index == 0:
                short_eligible = True
                short_quality = quality
        if eligible_weight <= 0:
            return 0.0
        multi_scale = weighted_quality / eligible_weight
        if not short_eligible:
            return min(1.0, multi_scale)
        blend = self._config.short_window_blend
        return min(1.0, blend * short_quality + (1.0 - blend) * multi_scale)

    def _update_signal(self, price: float, event_time: datetime) -> _SignalUpdate:
        previous_price = self._previous_price
        previous_regime = self._regime
        self._previous_price = price
        self._previous_time = event_time
        if previous_price is None or price == previous_price:
            return _SignalUpdate(
                False,
                0.0,
                previous_regime,
                self._regime,
                self._positive_cusum,
                self._negative_cusum,
                self._direction_score(self._regime),
            )

        signed_return = math.log(price / previous_price) if previous_price > 0 else 0.0
        absolute_return = abs(signed_return)
        scale = self._return_scale or absolute_return or self._config.scale_floor
        standardized = max(
            -self._config.standardized_return_clip,
            min(self._config.standardized_return_clip, signed_return / max(scale, self._config.scale_floor)),
        )
        self._return_scale = self._ewma(
            self._return_scale,
            absolute_return,
            self._alpha(self._config.scale_span),
        )
        self._observed_price_events += 1
        for window in self._windows:
            window.append(event_time, signed_return)

        allowance = self._config.cusum_allowance
        self._positive_cusum = min(
            self._config.entry_cusum * 2.0,
            max(0.0, self._positive_cusum + standardized - allowance),
        )
        self._negative_cusum = min(
            self._config.entry_cusum * 2.0,
            max(0.0, self._negative_cusum - standardized - allowance),
        )

        warmed = self._observed_price_events >= self._config.warmup_price_events
        positive_score = self._direction_score(1)
        negative_score = self._direction_score(-1)
        if self._regime == 0 and warmed:
            if (
                self._positive_cusum >= self._config.entry_cusum
                and positive_score >= self._config.entry_score
            ):
                self._regime = 1
            elif (
                self._negative_cusum >= self._config.entry_cusum
                and negative_score >= self._config.entry_score
            ):
                self._regime = -1
        elif self._regime != 0:
            opposite: Regime = -self._regime  # type: ignore[assignment]
            opposite_cusum = self._negative_cusum if opposite < 0 else self._positive_cusum
            opposite_score = negative_score if opposite < 0 else positive_score
            if (
                opposite_cusum >= self._config.reversal_cusum
                and opposite_score >= self._config.entry_score
            ):
                self._regime = opposite
                self._below_exit_events = 0
            elif opposite_cusum >= self._config.reversal_cusum:
                # Strong counter-evidence is already present, but the physical
                # windows have not yet established its structure. Keep the
                # current state until that evidence resolves into a direct
                # reversal or decays, instead of manufacturing a brief neutral
                # state solely because the long window still points backward.
                self._below_exit_events = 0
            else:
                current_score = positive_score if self._regime > 0 else negative_score
                if current_score < self._config.exit_score:
                    self._below_exit_events += 1
                else:
                    self._below_exit_events = 0
                if self._below_exit_events >= self._config.exit_confirmation_events:
                    self._regime = 0
                    self._below_exit_events = 0

        trend_score = (
            positive_score if self._regime > 0 else negative_score if self._regime < 0 else max(positive_score, negative_score)
        )
        return _SignalUpdate(
            True,
            standardized,
            previous_regime,
            self._regime,
            self._positive_cusum,
            self._negative_cusum,
            trend_score,
        )

    def _open_bar(self, tick: dict[str, Any], price: float, tick_time: datetime) -> dict[str, Any]:
        return {
            "time": tick_time,
            "end_time": tick_time,
            "availability_time": tick_time,
            "symbol": str(tick.get("symbol") or ""),
            "timeframe": A3C_TIMEFRAME,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "high_time": tick_time,
            "low_time": tick_time,
            "tick_volume": 0,
            "real_volume": 0,
            "mean_spread": 0.0,
            "duration_ms": 0,
            "price_event_count": 0,
            "path_log_return": 0.0,
            "realized_directional_efficiency": 0.0,
            "regime_at_open": self._regime,
            "regime_at_close": self._regime,
            "trend_score_at_close": self._direction_score(self._regime),
            "positive_cusum_at_close": self._positive_cusum,
            "negative_cusum_at_close": self._negative_cusum,
            "evidence_progress": 0.0,
            "max_evidence_progress": 0.0,
            "closure_reason": None,
            "gap_count_at_open": self._gap_count,
            "regime_warmed": self._observed_price_events >= self._config.warmup_price_events,
            "is_complete": False,
        }

    def _complete_bar(
        self,
        reason: str,
        signal: _SignalUpdate | None = None,
    ) -> dict[str, Any]:
        if self._bar is None:
            raise RuntimeError("cannot complete an absent A3C bar")
        bar = self._bar
        bar["regime_at_close"] = self._regime
        bar["trend_score_at_close"] = (
            signal.trend_score if signal is not None else self._direction_score(self._regime)
        )
        bar["positive_cusum_at_close"] = (
            signal.positive_cusum if signal is not None else self._positive_cusum
        )
        bar["negative_cusum_at_close"] = (
            signal.negative_cusum if signal is not None else self._negative_cusum
        )
        bar["evidence_progress"] = self._bar_progress
        bar["max_evidence_progress"] = max(
            float(bar["max_evidence_progress"]), self._bar_progress
        )
        bar["closure_reason"] = reason
        bar["availability_time"] = bar["end_time"]
        bar["is_complete"] = True
        self._bar = None
        self._bar_progress = 0.0
        return bar

    def _state_change_reason(self, previous: Regime, current: Regime) -> str:
        if previous == 0 and current != 0:
            return "trend_entry"
        if previous != 0 and current == 0:
            return "trend_exit"
        return "trend_reversal"

    def _process_tick(
        self,
        tick: dict[str, Any],
        price: float,
        tick_time: datetime,
    ) -> dict[str, Any] | None:
        if self._bar is None:
            self._bar = self._open_bar(tick, price, tick_time)
        bar = self._bar
        previous_price = self._previous_price
        count = int(bar["tick_volume"]) + 1
        if count > 1 and previous_price is not None and previous_price > 0:
            bar["path_log_return"] = float(bar["path_log_return"]) + abs(
                math.log(price / previous_price)
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

        signal = self._update_signal(price, tick_time)
        if signal.is_price_event:
            bar["price_event_count"] = int(bar["price_event_count"]) + 1
        if float(bar["path_log_return"]) > 1e-18:
            bar["realized_directional_efficiency"] = min(
                1.0,
                abs(math.log(float(bar["close"]) / float(bar["open"])))
                / float(bar["path_log_return"]),
            )

        reason: str | None = None
        if signal.state_changed:
            reason = self._state_change_reason(signal.previous_regime, signal.regime)
        elif self._regime != 0:
            if signal.is_price_event:
                along = float(self._regime) * signal.standardized_return
                if along > 0:
                    self._bar_progress += along * (0.5 + 0.5 * signal.trend_score)
                elif along < 0:
                    self._bar_progress = max(
                        0.0,
                        self._bar_progress
                        - self._config.counter_movement_penalty * abs(along),
                    )
                bar["max_evidence_progress"] = max(
                    float(bar["max_evidence_progress"]), self._bar_progress
                )
            if self._bar_progress >= self._config.evidence_budget:
                reason = "trend_evidence"
            elif int(bar["duration_ms"]) >= self._config.trend_max_duration_ms:
                reason = "trend_duration_guard"
            elif int(bar["price_event_count"]) >= self._config.trend_max_price_events:
                reason = "trend_event_guard"
        else:
            if int(bar["duration_ms"]) >= self._config.neutral_max_duration_ms:
                reason = "neutral_duration_guard"
            elif int(bar["price_event_count"]) >= self._config.neutral_max_price_events:
                reason = "neutral_event_guard"
        if count >= self._config.hard_max_raw_ticks:
            reason = reason or "raw_tick_safety_guard"

        if reason is None:
            return None
        completed = self._complete_bar(reason, signal)
        if signal.state_changed:
            self._positive_cusum = 0.0
            self._negative_cusum = 0.0
            self._below_exit_events = 0
        return completed

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

        gap_completed: dict[str, Any] | None = None
        if self._previous_time is not None:
            elapsed_ms = max(
                0.0,
                (tick_time - self._previous_time).total_seconds() * 1000.0,
            )
            if elapsed_ms > self._config.gap_reset_ms:
                if self._bar is not None and int(self._bar["tick_volume"]) > 0:
                    gap_completed = self._complete_bar("gap_reset")
                self._gap_count += 1
                self._reset_signal()

        completed = self._process_tick(tick, price, tick_time)
        if gap_completed is not None:
            if completed is not None:
                raise RuntimeError("first post-gap tick unexpectedly completed an A3C bar")
            return gap_completed, self._bar
        if completed is not None:
            return completed, completed
        return None, self._bar

    @property
    def current(self) -> dict[str, Any] | None:
        return self._bar

    @property
    def regime(self) -> Regime:
        return self._regime


def build_a3c_bars(
    ticks: list[dict[str, Any]],
    config: A3CConfig | None = None,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic A3C bars from one ascending causal tick prefix."""
    builder = A3CBuilder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
