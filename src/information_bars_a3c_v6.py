"""Strictly causal completed-minute endpoint drift bars for A3C-v6 research.

Macro direction and volatility use only finalized UTC-minute endpoints. Current
ticks can advance a bar-local net-displacement clock but cannot enter their own
macro state. This removes raw quote count and tick-path efficiency from the
direction estimator.

This module is intentionally offline-only and is not wired into REST,
WebSocket, Monitor, ATS, strategy, execution, database or production paths.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from src.information_bars import PriceField, _as_datetime, tick_price

if TYPE_CHECKING:
    from datetime import datetime

A3C_V6_ALGORITHM = "causal-completed-minute-endpoint-drift-bars-v6-a3c"
A3C_V6_TIMEFRAME = "A3C6"


@dataclass(frozen=True)
class A3CV6Config:
    """Frozen common v6 configuration plus the calibration budget."""

    evidence_budget: float = 4.0
    drift_horizons: tuple[int, int, int] = (5, 15, 60)
    drift_weights: tuple[float, float, float] = (0.20, 0.30, 0.50)
    scale_lookback: int = 60
    minimum_scale_returns: int = 5
    scale_floor: float = 1e-10
    normalized_minute_return_clip: float = 2.0
    max_duration_ms: int = 120 * 60 * 1000
    hard_max_raw_ticks: int = 100_000
    gap_reset_ms: int = 5 * 60 * 1000

    def __post_init__(self) -> None:
        if self.evidence_budget <= 0:
            raise ValueError("evidence_budget must be positive")
        if len(self.drift_horizons) != 3 or len(self.drift_weights) != 3:
            raise ValueError("A3C-v6 requires exactly three drift horizons")
        if tuple(sorted(self.drift_horizons)) != self.drift_horizons:
            raise ValueError("drift_horizons must be strictly ascending")
        if min(self.drift_horizons) <= 0 or min(self.drift_weights) <= 0:
            raise ValueError("drift horizons and weights must be positive")
        if not math.isclose(sum(self.drift_weights), 1.0, abs_tol=1e-12):
            raise ValueError("drift_weights must sum to one")
        if self.scale_lookback < max(self.drift_horizons):
            raise ValueError("scale_lookback must cover the longest horizon")
        if not 2 <= self.minimum_scale_returns <= self.scale_lookback:
            raise ValueError("minimum_scale_returns is invalid")
        if self.scale_floor <= 0 or self.normalized_minute_return_clip <= 0:
            raise ValueError("scale and clip must be positive")
        if min(
            self.max_duration_ms,
            self.hard_max_raw_ticks,
            self.gap_reset_ms,
        ) <= 0:
            raise ValueError("liveness and gap bounds must be positive")

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": A3C_V6_ALGORITHM,
            "timeframe": A3C_V6_TIMEFRAME,
            "strategy_eligible": False,
            **asdict(self),
        }


@dataclass(frozen=True)
class _MinuteUpdate:
    completed_return: bool
    normalized_minute_return: float
    prior_macro_drift: float
    current_macro_drift: float
    prior_minute_rms: float | None
    current_minute_rms: float | None
    evidence_direction: int
    direction_reset: bool
    delta: float
    progress: float


class A3CV6Builder:
    """One-pass strictly causal A3C-v6 builder."""

    __slots__ = (
        "_bar",
        "_completed_endpoint",
        "_config",
        "_evidence_direction",
        "_gap_count",
        "_minute_bucket",
        "_minute_endpoint",
        "_minute_returns",
        "_previous_price",
        "_previous_time",
        "_price_field",
        "_progress",
        "_progress_peak",
    )

    def __init__(
        self,
        config: A3CV6Config | None = None,
        price_field: PriceField = "bid",
    ) -> None:
        self._config = config or A3CV6Config()
        self._price_field = price_field
        self._bar: dict[str, Any] | None = None
        self._previous_price: float | None = None
        self._previous_time: datetime | None = None
        self._minute_bucket: datetime | None = None
        self._minute_endpoint: float | None = None
        self._completed_endpoint: float | None = None
        self._minute_returns: deque[float] = deque(
            maxlen=self._config.scale_lookback
        )
        self._gap_count = 0
        self._progress = 0.0
        self._progress_peak = 0.0
        self._evidence_direction = 0

    @staticmethod
    def _bucket(event_time: datetime) -> datetime:
        return event_time.replace(second=0, microsecond=0)

    def _reset_bar_progress(self) -> None:
        self._progress = 0.0
        self._progress_peak = 0.0
        self._evidence_direction = 0

    def _reset_signal(self) -> None:
        self._previous_price = None
        self._previous_time = None
        self._minute_bucket = None
        self._minute_endpoint = None
        self._completed_endpoint = None
        self._minute_returns = deque(maxlen=self._config.scale_lookback)
        self._reset_bar_progress()

    def _empty_minute_update(self) -> _MinuteUpdate:
        macro = self._macro_drift()
        minute_rms = self._minute_rms()
        return _MinuteUpdate(
            False,
            0.0,
            macro,
            macro,
            minute_rms,
            minute_rms,
            self._evidence_direction,
            False,
            0.0,
            self._progress,
        )

    def _advance_completed_minute(
        self, price: float, event_time: datetime
    ) -> _MinuteUpdate:
        bucket = self._bucket(event_time)
        if self._minute_bucket is None:
            self._minute_bucket = bucket
            self._minute_endpoint = price
            return self._empty_minute_update()
        if bucket == self._minute_bucket:
            self._minute_endpoint = price
            return self._empty_minute_update()
        if bucket < self._minute_bucket:
            return self._empty_minute_update()

        endpoint = self._minute_endpoint
        prior_macro = self._macro_drift()
        prior_rms = self._minute_rms()
        normalized = 0.0
        delta = 0.0
        direction_reset = False
        completed_return = False
        if endpoint is not None:
            if self._completed_endpoint is not None and self._completed_endpoint > 0:
                completed_return = True
                minute_return = math.log(endpoint / self._completed_endpoint)
                if prior_rms is not None and prior_macro != 0:
                    normalized = max(
                        -self._config.normalized_minute_return_clip,
                        min(
                            self._config.normalized_minute_return_clip,
                            minute_return
                            / max(prior_rms, self._config.scale_floor),
                        ),
                    )
                    direction = 1 if prior_macro > 0 else -1
                    if self._evidence_direction == 0:
                        self._evidence_direction = direction
                    elif self._evidence_direction != direction:
                        self._reset_bar_progress()
                        self._evidence_direction = direction
                        direction_reset = True
                    delta = direction * normalized * abs(prior_macro)
                    self._progress = max(0.0, self._progress + delta)
                    self._progress_peak = max(self._progress_peak, self._progress)
                self._minute_returns.append(minute_return)
            self._completed_endpoint = endpoint

        current_macro = self._macro_drift()
        current_rms = self._minute_rms()
        current_direction = 1 if current_macro > 0 else -1 if current_macro < 0 else 0
        if current_direction != 0:
            if self._evidence_direction == 0:
                self._evidence_direction = current_direction
            elif self._evidence_direction != current_direction:
                self._reset_bar_progress()
                self._evidence_direction = current_direction
                direction_reset = True

        self._minute_bucket = bucket
        self._minute_endpoint = price
        return _MinuteUpdate(
            completed_return,
            normalized,
            prior_macro,
            current_macro,
            prior_rms,
            current_rms,
            self._evidence_direction,
            direction_reset,
            delta,
            self._progress,
        )

    def _minute_rms(self) -> float | None:
        if len(self._minute_returns) < self._config.minimum_scale_returns:
            return None
        mean_square = sum(value * value for value in self._minute_returns) / len(
            self._minute_returns
        )
        return max(self._config.scale_floor, math.sqrt(mean_square))

    def _macro_drift(self) -> float:
        values = tuple(self._minute_returns)
        eligible_weight = 0.0
        weighted_score = 0.0
        for horizon, weight in zip(
            self._config.drift_horizons,
            self._config.drift_weights,
            strict=True,
        ):
            if len(values) < horizon:
                continue
            returns = values[-horizon:]
            energy = math.sqrt(horizon * sum(value * value for value in returns))
            drift = sum(returns) / energy if energy > 1e-18 else 0.0
            eligible_weight += weight
            weighted_score += weight * max(-1.0, min(1.0, drift))
        if eligible_weight <= 0:
            return 0.0
        return max(-1.0, min(1.0, weighted_score / eligible_weight))

    def _open_bar(
        self, tick: dict[str, Any], price: float, tick_time: datetime
    ) -> dict[str, Any]:
        return {
            "time": tick_time,
            "end_time": tick_time,
            "availability_time": tick_time,
            "symbol": str(tick.get("symbol") or ""),
            "timeframe": A3C_V6_TIMEFRAME,
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
            "aligned_minute_returns": 0,
            "counter_minute_returns": 0,
            "macro_direction_resets": 0,
            "path_log_return": 0.0,
            "realized_directional_efficiency": 0.0,
            "macro_drift_at_close": 0.0,
            "minute_rms_at_close": None,
            "evidence_direction_at_close": 0,
            "progress_at_close": 0.0,
            "progress_peak": 0.0,
            "completed_minute_returns_at_close": 0,
            "closure_reason": None,
            "gap_count_at_open": self._gap_count,
            "is_complete": False,
        }

    def _complete_bar(
        self, reason: str, minute_update: _MinuteUpdate | None = None
    ) -> dict[str, Any]:
        if self._bar is None:
            raise RuntimeError("cannot complete an absent A3C-v6 bar")
        bar = self._bar
        macro = (
            minute_update.current_macro_drift
            if minute_update is not None
            else self._macro_drift()
        )
        minute_rms = (
            minute_update.current_minute_rms
            if minute_update is not None
            else self._minute_rms()
        )
        bar["macro_drift_at_close"] = macro
        bar["minute_rms_at_close"] = minute_rms
        bar["evidence_direction_at_close"] = self._evidence_direction
        bar["progress_at_close"] = self._progress
        bar["progress_peak"] = self._progress_peak
        bar["completed_minute_returns_at_close"] = len(self._minute_returns)
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

        minute_update = self._advance_completed_minute(price, tick_time)
        is_price_event = previous_price is not None and price != previous_price
        self._previous_price = price
        self._previous_time = tick_time
        if is_price_event:
            bar["price_event_count"] = int(bar["price_event_count"]) + 1
        if minute_update.completed_return:
            if minute_update.delta > 0:
                bar["aligned_minute_returns"] = (
                    int(bar["aligned_minute_returns"]) + 1
                )
            elif minute_update.delta < 0:
                bar["counter_minute_returns"] = (
                    int(bar["counter_minute_returns"]) + 1
                )
        if minute_update.direction_reset:
            bar["macro_direction_resets"] = int(bar["macro_direction_resets"]) + 1
        if float(bar["path_log_return"]) > 1e-18:
            bar["realized_directional_efficiency"] = min(
                1.0,
                abs(math.log(float(bar["close"]) / float(bar["open"])))
                / float(bar["path_log_return"]),
            )

        reason: str | None = None
        if self._progress >= self._config.evidence_budget:
            reason = "completed_minute_drift_evidence"
        elif int(bar["duration_ms"]) >= self._config.max_duration_ms:
            reason = "neutral_duration_guard"
        if count >= self._config.hard_max_raw_ticks:
            reason = reason or "raw_tick_safety_guard"
        if reason is None:
            return None
        return self._complete_bar(reason, minute_update)

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
                    "first post-gap tick unexpectedly completed an A3C-v6 bar"
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

    @property
    def minute_rms(self) -> float | None:
        return self._minute_rms()

    @property
    def completed_minute_returns(self) -> int:
        return len(self._minute_returns)


def build_a3c_v6_bars(
    ticks: list[dict[str, Any]],
    config: A3CV6Config | None = None,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic A3C-v6 bars from one ascending causal tick prefix."""
    builder = A3CV6Builder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
