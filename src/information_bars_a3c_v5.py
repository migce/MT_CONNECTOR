"""Strictly causal prior-macro-drift clock bars for A3C-v5 research.

The current price change is scored against a 5/15/60-minute drift value that
existed immediately before that change. Aligned movement advances one bar-local
clock; counter-movement cancels it. Physical drift windows persist across
ordinary boundaries so sustained movement can emit repeated truthful bars.

This module is intentionally offline-only and is not wired into REST,
WebSocket, Monitor, ATS, strategy, execution, database or production paths.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from src.information_bars import PriceField, _as_datetime, tick_price

if TYPE_CHECKING:
    from datetime import datetime

A3C_V5_ALGORITHM = "causal-prior-macro-drift-clock-bars-v5-a3c"
A3C_V5_TIMEFRAME = "A3C5"


@dataclass(frozen=True)
class A3CV5Config:
    """Frozen common v5 configuration plus the calibration budget."""

    evidence_budget: float = 48.0
    window_seconds: tuple[int, int, int] = (300, 900, 3_600)
    window_weights: tuple[float, float, float] = (0.20, 0.30, 0.50)
    minimum_window_events: tuple[int, int, int] = (8, 16, 32)
    scale_span: int = 128
    standardized_return_clip: float = 4.0
    scale_floor: float = 1e-10
    aligned_quality_floor: float = 0.25
    aligned_quality_gain: float = 1.75
    counter_movement_penalty: float = 1.50
    max_duration_ms: int = 180 * 60 * 1000
    hard_max_raw_ticks: int = 100_000
    gap_reset_ms: int = 5 * 60 * 1000

    def __post_init__(self) -> None:
        if self.evidence_budget <= 0:
            raise ValueError("evidence_budget must be positive")
        if not (
            len(self.window_seconds)
            == len(self.window_weights)
            == len(self.minimum_window_events)
            == 3
        ):
            raise ValueError("A3C-v5 requires exactly three physical windows")
        if tuple(sorted(self.window_seconds)) != self.window_seconds:
            raise ValueError("window_seconds must be strictly ascending")
        if min(self.window_seconds) <= 0 or min(self.window_weights) <= 0:
            raise ValueError("window lengths and weights must be positive")
        if not math.isclose(sum(self.window_weights), 1.0, abs_tol=1e-12):
            raise ValueError("window_weights must sum to one")
        if min(self.minimum_window_events) < 2 or self.scale_span < 2:
            raise ValueError("window events and scale span must be at least two")
        if self.standardized_return_clip <= 0 or self.scale_floor <= 0:
            raise ValueError("standardized return bounds must be positive")
        if self.aligned_quality_floor < 0 or self.aligned_quality_gain < 0:
            raise ValueError("aligned quality terms must be non-negative")
        if self.counter_movement_penalty < 1:
            raise ValueError("counter movement penalty must be at least one")
        if min(
            self.max_duration_ms,
            self.hard_max_raw_ticks,
            self.gap_reset_ms,
        ) <= 0:
            raise ValueError("liveness and gap bounds must be positive")

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": A3C_V5_ALGORITHM,
            "timeframe": A3C_V5_TIMEFRAME,
            "strategy_eligible": False,
            **asdict(self),
        }


class _DriftWindow:
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
class _DriftUpdate:
    is_price_event: bool
    standardized_return: float
    prior_macro_drift: float
    current_macro_drift: float
    alignment: float
    progress: float


class A3CV5Builder:
    """One-pass strictly causal A3C-v5 builder."""

    __slots__ = (
        "_bar",
        "_config",
        "_gap_count",
        "_previous_price",
        "_previous_time",
        "_price_field",
        "_progress",
        "_progress_peak",
        "_return_scale",
        "_windows",
    )

    def __init__(
        self,
        config: A3CV5Config | None = None,
        price_field: PriceField = "bid",
    ) -> None:
        self._config = config or A3CV5Config()
        self._price_field = price_field
        self._bar: dict[str, Any] | None = None
        self._previous_price: float | None = None
        self._previous_time: datetime | None = None
        self._return_scale: float | None = None
        self._gap_count = 0
        self._windows = tuple(
            _DriftWindow(seconds) for seconds in self._config.window_seconds
        )
        self._progress = 0.0
        self._progress_peak = 0.0

    @staticmethod
    def _alpha(span: int) -> float:
        return 2.0 / (span + 1.0)

    @staticmethod
    def _ewma(previous: float | None, value: float, alpha: float) -> float:
        return value if previous is None else previous + alpha * (value - previous)

    def _reset_bar_progress(self) -> None:
        self._progress = 0.0
        self._progress_peak = 0.0

    def _reset_signal(self) -> None:
        self._previous_price = None
        self._previous_time = None
        self._return_scale = None
        self._windows = tuple(
            _DriftWindow(seconds) for seconds in self._config.window_seconds
        )
        self._reset_bar_progress()

    def _macro_drift(self) -> float:
        eligible_weight = 0.0
        weighted_score = 0.0
        for window, weight, minimum_events in zip(
            self._windows,
            self._config.window_weights,
            self._config.minimum_window_events,
            strict=True,
        ):
            eligible, efficiency = window.signed_efficiency(minimum_events)
            if not eligible:
                continue
            eligible_weight += weight
            weighted_score += weight * efficiency
        if eligible_weight <= 0:
            return 0.0
        return max(-1.0, min(1.0, weighted_score / eligible_weight))

    def _update_drift(self, price: float, event_time: datetime) -> _DriftUpdate:
        previous_price = self._previous_price
        self._previous_price = price
        self._previous_time = event_time
        prior_macro = self._macro_drift()
        if previous_price is None or price == previous_price:
            return _DriftUpdate(
                False, 0.0, prior_macro, prior_macro, 0.0, self._progress
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
        alignment = standardized * prior_macro
        if alignment > 0:
            multiplier = (
                self._config.aligned_quality_floor
                + self._config.aligned_quality_gain * abs(prior_macro)
            )
            self._progress += alignment * multiplier
        elif alignment < 0:
            self._progress = max(
                0.0,
                self._progress
                - self._config.counter_movement_penalty * abs(alignment),
            )
        self._progress_peak = max(self._progress_peak, self._progress)

        self._return_scale = self._ewma(
            self._return_scale,
            absolute_return,
            self._alpha(self._config.scale_span),
        )
        for window in self._windows:
            window.append(event_time, signed_return)
        return _DriftUpdate(
            True,
            standardized,
            prior_macro,
            self._macro_drift(),
            alignment,
            self._progress,
        )

    def _open_bar(
        self, tick: dict[str, Any], price: float, tick_time: datetime
    ) -> dict[str, Any]:
        return {
            "time": tick_time,
            "end_time": tick_time,
            "availability_time": tick_time,
            "symbol": str(tick.get("symbol") or ""),
            "timeframe": A3C_V5_TIMEFRAME,
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
            "aligned_price_events": 0,
            "counter_price_events": 0,
            "path_log_return": 0.0,
            "realized_directional_efficiency": 0.0,
            "prior_macro_drift_at_close": 0.0,
            "macro_drift_at_close": 0.0,
            "macro_direction_at_close": 0,
            "progress_at_close": 0.0,
            "progress_peak": 0.0,
            "closure_reason": None,
            "gap_count_at_open": self._gap_count,
            "is_complete": False,
        }

    def _complete_bar(
        self, reason: str, drift: _DriftUpdate | None = None
    ) -> dict[str, Any]:
        if self._bar is None:
            raise RuntimeError("cannot complete an absent A3C-v5 bar")
        bar = self._bar
        current_macro = self._macro_drift()
        prior_macro = drift.prior_macro_drift if drift is not None else current_macro
        bar["prior_macro_drift_at_close"] = prior_macro
        bar["macro_drift_at_close"] = (
            drift.current_macro_drift if drift is not None else current_macro
        )
        score = float(bar["macro_drift_at_close"])
        bar["macro_direction_at_close"] = 1 if score > 0 else -1 if score < 0 else 0
        bar["progress_at_close"] = self._progress
        bar["progress_peak"] = self._progress_peak
        bar["closure_reason"] = reason
        bar["availability_time"] = bar["end_time"]
        bar["is_complete"] = True
        self._bar = None
        self._reset_bar_progress()
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

        drift = self._update_drift(price, tick_time)
        if drift.is_price_event:
            bar["price_event_count"] = int(bar["price_event_count"]) + 1
            if drift.alignment > 0:
                bar["aligned_price_events"] = int(bar["aligned_price_events"]) + 1
            elif drift.alignment < 0:
                bar["counter_price_events"] = int(bar["counter_price_events"]) + 1
        if float(bar["path_log_return"]) > 1e-18:
            bar["realized_directional_efficiency"] = min(
                1.0,
                abs(math.log(float(bar["close"]) / float(bar["open"])))
                / float(bar["path_log_return"]),
            )

        reason: str | None = None
        if self._progress >= self._config.evidence_budget:
            reason = "macro_drift_evidence"
        elif int(bar["duration_ms"]) >= self._config.max_duration_ms:
            reason = "neutral_duration_guard"
        if count >= self._config.hard_max_raw_ticks:
            reason = reason or "raw_tick_safety_guard"
        if reason is None:
            return None
        return self._complete_bar(reason, drift)

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
                    "first post-gap tick unexpectedly completed an A3C-v5 bar"
                )
            return gap_completed, self._bar
        if completed is not None:
            return completed, completed
        return None, self._bar

    @property
    def current(self) -> dict[str, Any] | None:
        return self._bar

    @property
    def progress(self) -> float:
        return self._progress

    @property
    def macro_drift(self) -> float:
        return self._macro_drift()


def build_a3c_v5_bars(
    ticks: list[dict[str, Any]],
    config: A3CV5Config | None = None,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic A3C-v5 bars from one ascending causal tick prefix."""
    builder = A3CV5Builder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
