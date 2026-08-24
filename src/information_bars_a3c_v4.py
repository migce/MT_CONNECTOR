"""Strictly causal continuous directional-evidence bars for A3C-v4 research.

Unlike A3C-v3, v4 has no binary trend-admission state. Two continuously
competing one-sided clocks operate on every meaningful price change. Coherent
movement repeatedly completes evidence budgets, while counter-movement
cancels the previously leading clock.

This module is intentionally offline-only and is not wired into REST,
WebSocket, Monitor, ATS, strategy, execution, database or production paths.
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

A3C_V4_ALGORITHM = "causal-continuous-directional-evidence-bars-v4-a3c"
A3C_V4_TIMEFRAME = "A3C4"
Direction = Literal[-1, 0, 1]


@dataclass(frozen=True)
class A3CV4Config:
    """Frozen common v4 configuration plus two calibration dimensions."""

    evidence_allowance: float = 0.20
    evidence_budget: float = 12.0
    window_seconds: tuple[int, int, int] = (60, 300, 900)
    window_weights: tuple[float, float, float] = (0.50, 0.30, 0.20)
    minimum_window_events: int = 8
    scale_span: int = 128
    standardized_return_clip: float = 4.0
    scale_floor: float = 1e-10
    quality_floor: float = 0.25
    quality_gain: float = 1.75
    counter_movement_penalty: float = 1.50
    swing_arm_fraction: float = 0.45
    swing_confirmation: float = 2.0
    max_duration_ms: int = 90 * 60 * 1000
    max_price_events: int = 3_000
    hard_max_raw_ticks: int = 100_000
    gap_reset_ms: int = 5 * 60 * 1000

    def __post_init__(self) -> None:
        if self.evidence_allowance < 0 or self.evidence_budget <= 0:
            raise ValueError("allowance must be non-negative and budget positive")
        if len(self.window_seconds) != 3 or len(self.window_weights) != 3:
            raise ValueError("A3C-v4 requires exactly three physical windows")
        if tuple(sorted(self.window_seconds)) != self.window_seconds:
            raise ValueError("window_seconds must be strictly ascending")
        if min(self.window_seconds) <= 0 or min(self.window_weights) <= 0:
            raise ValueError("window lengths and weights must be positive")
        if not math.isclose(sum(self.window_weights), 1.0, abs_tol=1e-12):
            raise ValueError("window_weights must sum to one")
        if min(self.minimum_window_events, self.scale_span) < 2:
            raise ValueError("window events and scale span must be at least two")
        if self.standardized_return_clip <= 0 or self.scale_floor <= 0:
            raise ValueError("standardized return bounds must be positive")
        if self.quality_floor < 0 or self.quality_gain < 0:
            raise ValueError("quality floor and gain must be non-negative")
        if self.counter_movement_penalty < 1:
            raise ValueError("counter movement penalty must be at least one")
        if not 0 < self.swing_arm_fraction < 1 or self.swing_confirmation <= 0:
            raise ValueError("swing thresholds are invalid")
        if self.swing_confirmation >= self.evidence_budget:
            raise ValueError("swing confirmation must be below evidence budget")
        if min(
            self.max_duration_ms,
            self.max_price_events,
            self.hard_max_raw_ticks,
            self.gap_reset_ms,
        ) <= 0:
            raise ValueError("liveness and gap bounds must be positive")

    @property
    def swing_arm_level(self) -> float:
        return self.evidence_budget * self.swing_arm_fraction

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": A3C_V4_ALGORITHM,
            "timeframe": A3C_V4_TIMEFRAME,
            "strategy_eligible": False,
            **asdict(self),
            "swing_arm_level": self.swing_arm_level,
        }


class _EfficiencyWindow:
    __slots__ = ("abs_sum", "entries", "return_sum", "seconds")

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self.entries: deque[tuple[datetime, float]] = deque()
        self.return_sum = 0.0
        self.abs_sum = 0.0

    def append(self, event_time: datetime, value: float) -> None:
        self.entries.append((event_time, value))
        self.return_sum += value
        self.abs_sum += abs(value)
        cutoff = event_time - timedelta(seconds=self.seconds)
        while self.entries and self.entries[0][0] < cutoff:
            _expired_time, expired = self.entries.popleft()
            self.return_sum -= expired
            self.abs_sum -= abs(expired)

    def signed_efficiency(self, minimum_events: int) -> tuple[bool, float]:
        if len(self.entries) < minimum_events:
            return False, 0.0
        if self.abs_sum <= 1e-18:
            return True, 0.0
        return True, max(-1.0, min(1.0, self.return_sum / self.abs_sum))


@dataclass(frozen=True)
class _EvidenceUpdate:
    is_price_event: bool
    standardized_return: float
    positive_quality: float
    negative_quality: float
    positive_clock: float
    negative_clock: float
    leader: Direction
    armed_direction: Direction


class A3CV4Builder:
    """One-pass strictly causal A3C-v4 builder."""

    __slots__ = (
        "_armed_direction",
        "_bar",
        "_config",
        "_gap_count",
        "_leader",
        "_negative_clock",
        "_negative_peak",
        "_positive_clock",
        "_positive_peak",
        "_previous_price",
        "_previous_time",
        "_price_field",
        "_return_scale",
        "_windows",
    )

    def __init__(
        self,
        config: A3CV4Config | None = None,
        price_field: PriceField = "bid",
    ) -> None:
        self._config = config or A3CV4Config()
        self._price_field = price_field
        self._bar: dict[str, Any] | None = None
        self._previous_price: float | None = None
        self._previous_time: datetime | None = None
        self._return_scale: float | None = None
        self._gap_count = 0
        self._windows = tuple(
            _EfficiencyWindow(seconds) for seconds in self._config.window_seconds
        )
        self._positive_clock = 0.0
        self._negative_clock = 0.0
        self._positive_peak = 0.0
        self._negative_peak = 0.0
        self._leader: Direction = 0
        self._armed_direction: Direction = 0

    @staticmethod
    def _alpha(span: int) -> float:
        return 2.0 / (span + 1.0)

    @staticmethod
    def _ewma(previous: float | None, value: float, alpha: float) -> float:
        return value if previous is None else previous + alpha * (value - previous)

    def _reset_bar_clocks(self) -> None:
        self._positive_clock = 0.0
        self._negative_clock = 0.0
        self._positive_peak = 0.0
        self._negative_peak = 0.0
        self._leader = 0
        self._armed_direction = 0

    def _reset_signal(self) -> None:
        self._previous_price = None
        self._previous_time = None
        self._return_scale = None
        self._windows = tuple(
            _EfficiencyWindow(seconds) for seconds in self._config.window_seconds
        )
        self._reset_bar_clocks()

    def _directional_qualities(self) -> tuple[float, float]:
        eligible_weight = 0.0
        positive = 0.0
        negative = 0.0
        for window, weight in zip(
            self._windows, self._config.window_weights, strict=True
        ):
            eligible, efficiency = window.signed_efficiency(
                self._config.minimum_window_events
            )
            if not eligible:
                continue
            eligible_weight += weight
            positive += weight * max(0.0, efficiency)
            negative += weight * max(0.0, -efficiency)
        if eligible_weight <= 0:
            return 0.0, 0.0
        return positive / eligible_weight, negative / eligible_weight

    def _current_leader(self) -> Direction:
        if self._positive_clock <= 0 and self._negative_clock <= 0:
            return 0
        if self._positive_clock > self._negative_clock:
            return 1
        if self._negative_clock > self._positive_clock:
            return -1
        return self._leader

    def _update_evidence(self, price: float, event_time: datetime) -> _EvidenceUpdate:
        previous_price = self._previous_price
        self._previous_price = price
        self._previous_time = event_time
        if previous_price is None or price == previous_price:
            positive_quality, negative_quality = self._directional_qualities()
            return _EvidenceUpdate(
                False,
                0.0,
                positive_quality,
                negative_quality,
                self._positive_clock,
                self._negative_clock,
                self._leader,
                self._armed_direction,
            )

        signed_return = math.log(price / previous_price) if previous_price > 0 else 0.0
        absolute_return = abs(signed_return)
        previous_scale = self._return_scale or absolute_return or self._config.scale_floor
        standardized = max(
            -self._config.standardized_return_clip,
            min(
                self._config.standardized_return_clip,
                signed_return / max(previous_scale, self._config.scale_floor),
            ),
        )
        self._return_scale = self._ewma(
            self._return_scale,
            absolute_return,
            self._alpha(self._config.scale_span),
        )
        for window in self._windows:
            window.append(event_time, signed_return)
        positive_quality, negative_quality = self._directional_qualities()

        allowance = self._config.evidence_allowance
        counter = self._config.counter_movement_penalty
        if standardized > 0:
            multiplier = (
                self._config.quality_floor
                + self._config.quality_gain * positive_quality
            )
            self._positive_clock = max(
                0.0,
                self._positive_clock + standardized * multiplier - allowance,
            )
            self._negative_clock = max(
                0.0,
                self._negative_clock - counter * standardized - allowance,
            )
        elif standardized < 0:
            magnitude = abs(standardized)
            multiplier = (
                self._config.quality_floor
                + self._config.quality_gain * negative_quality
            )
            self._negative_clock = max(
                0.0,
                self._negative_clock + magnitude * multiplier - allowance,
            )
            self._positive_clock = max(
                0.0,
                self._positive_clock - counter * magnitude - allowance,
            )

        self._positive_peak = max(self._positive_peak, self._positive_clock)
        self._negative_peak = max(self._negative_peak, self._negative_clock)
        self._leader = self._current_leader()
        if self._armed_direction == 0:
            arm = self._config.swing_arm_level
            if self._positive_peak >= arm or self._negative_peak >= arm:
                self._armed_direction = (
                    1 if self._positive_peak >= self._negative_peak else -1
                )

        return _EvidenceUpdate(
            True,
            standardized,
            positive_quality,
            negative_quality,
            self._positive_clock,
            self._negative_clock,
            self._leader,
            self._armed_direction,
        )

    def _open_bar(
        self, tick: dict[str, Any], price: float, tick_time: datetime
    ) -> dict[str, Any]:
        return {
            "time": tick_time,
            "end_time": tick_time,
            "availability_time": tick_time,
            "symbol": str(tick.get("symbol") or ""),
            "timeframe": A3C_V4_TIMEFRAME,
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
            "positive_quality_at_close": 0.0,
            "negative_quality_at_close": 0.0,
            "positive_clock_at_close": 0.0,
            "negative_clock_at_close": 0.0,
            "positive_clock_peak": 0.0,
            "negative_clock_peak": 0.0,
            "dominant_direction": 0,
            "armed_direction": 0,
            "closure_reason": None,
            "gap_count_at_open": self._gap_count,
            "is_complete": False,
        }

    def _complete_bar(
        self, reason: str, evidence: _EvidenceUpdate | None = None
    ) -> dict[str, Any]:
        if self._bar is None:
            raise RuntimeError("cannot complete an absent A3C-v4 bar")
        bar = self._bar
        positive_quality, negative_quality = self._directional_qualities()
        bar["positive_quality_at_close"] = (
            evidence.positive_quality if evidence is not None else positive_quality
        )
        bar["negative_quality_at_close"] = (
            evidence.negative_quality if evidence is not None else negative_quality
        )
        bar["positive_clock_at_close"] = self._positive_clock
        bar["negative_clock_at_close"] = self._negative_clock
        bar["positive_clock_peak"] = self._positive_peak
        bar["negative_clock_peak"] = self._negative_peak
        bar["dominant_direction"] = self._leader
        bar["armed_direction"] = self._armed_direction
        bar["closure_reason"] = reason
        bar["availability_time"] = bar["end_time"]
        bar["is_complete"] = True
        self._bar = None
        self._reset_bar_clocks()
        return bar

    def _process_tick(
        self, tick: dict[str, Any], price: float, tick_time: datetime
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

        evidence = self._update_evidence(price, tick_time)
        if evidence.is_price_event:
            bar["price_event_count"] = int(bar["price_event_count"]) + 1
        if float(bar["path_log_return"]) > 1e-18:
            bar["realized_directional_efficiency"] = min(
                1.0,
                abs(math.log(float(bar["close"]) / float(bar["open"])))
                / float(bar["path_log_return"]),
            )

        reason: str | None = None
        if max(self._positive_clock, self._negative_clock) >= self._config.evidence_budget:
            reason = "directional_evidence"
        elif (
            self._armed_direction != 0
            and self._leader == -self._armed_direction
            and (
                self._negative_clock
                if self._leader < 0
                else self._positive_clock
            )
            >= self._config.swing_confirmation
        ):
            reason = "swing_reversal"
        elif int(bar["duration_ms"]) >= self._config.max_duration_ms:
            reason = "duration_guard"
        elif int(bar["price_event_count"]) >= self._config.max_price_events:
            reason = "event_guard"
        if count >= self._config.hard_max_raw_ticks:
            reason = reason or "raw_tick_safety_guard"

        if reason is None:
            return None
        return self._complete_bar(reason, evidence)

    def update(
        self, tick: dict[str, Any]
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
                raise RuntimeError(
                    "first post-gap tick unexpectedly completed an A3C-v4 bar"
                )
            return gap_completed, self._bar
        if completed is not None:
            return completed, completed
        return None, self._bar

    @property
    def current(self) -> dict[str, Any] | None:
        return self._bar

    @property
    def positive_clock(self) -> float:
        return self._positive_clock

    @property
    def negative_clock(self) -> float:
        return self._negative_clock


def build_a3c_v4_bars(
    ticks: list[dict[str, Any]],
    config: A3CV4Config | None = None,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic A3C-v4 bars from one ascending causal tick prefix."""
    builder = A3CV4Builder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
