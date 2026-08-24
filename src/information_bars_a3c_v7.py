"""Causal completed-minute dual-clock bars for offline A3C-v7 research."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from src.information_bars import PriceField, _as_datetime
from src.information_bars_a3c_v6 import A3CV6Builder, _MinuteUpdate

if TYPE_CHECKING:
    from datetime import datetime

A3C_V7_ALGORITHM = "causal-completed-minute-dual-clock-bars-v7-a3c"
A3C_V7_TIMEFRAME = "A3C7"


@dataclass(frozen=True)
class A3CV7Config:
    """Frozen common v7 parameters plus calibration budget/duration pair."""

    evidence_budget: float = 1.0
    trend_max_duration_ms: int = 45 * 60 * 1000
    drift_horizons: tuple[int, int, int] = (5, 15, 60)
    drift_weights: tuple[float, float, float] = (0.20, 0.30, 0.50)
    scale_lookback: int = 60
    minimum_scale_returns: int = 5
    scale_floor: float = 1e-10
    normalized_minute_return_clip: float = 2.0
    confidence_floor: float = 0.35
    counter_return_penalty: float = 1.25
    trend_drift_threshold: float = 0.25
    trend_minimum_returns: int = 5
    max_duration_ms: int = 120 * 60 * 1000
    hard_max_raw_ticks: int = 100_000
    gap_reset_ms: int = 5 * 60 * 1000

    def __post_init__(self) -> None:
        if self.evidence_budget <= 0:
            raise ValueError("evidence_budget must be positive")
        if len(self.drift_horizons) != 3 or len(self.drift_weights) != 3:
            raise ValueError("A3C-v7 requires exactly three drift horizons")
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
        if self.trend_minimum_returns < self.minimum_scale_returns:
            raise ValueError("trend_minimum_returns cannot precede scale readiness")
        if self.scale_floor <= 0 or self.normalized_minute_return_clip <= 0:
            raise ValueError("scale and clip must be positive")
        if not 0 < self.confidence_floor <= 1:
            raise ValueError("confidence_floor must be in (0, 1]")
        if self.counter_return_penalty < 1:
            raise ValueError("counter_return_penalty must be at least one")
        if not 0 < self.trend_drift_threshold <= 1:
            raise ValueError("trend_drift_threshold must be in (0, 1]")
        if not 0 < self.trend_max_duration_ms <= self.max_duration_ms:
            raise ValueError("trend duration must be within neutral duration")
        if (
            min(
                self.max_duration_ms,
                self.hard_max_raw_ticks,
                self.gap_reset_ms,
            )
            <= 0
        ):
            raise ValueError("liveness and gap bounds must be positive")

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": A3C_V7_ALGORITHM,
            "timeframe": A3C_V7_TIMEFRAME,
            "strategy_eligible": False,
            **asdict(self),
        }


class A3CV7Builder(A3CV6Builder):
    """One-pass v7 builder extending v6's completed-minute state contract."""

    __slots__ = ()

    def __init__(
        self,
        config: A3CV7Config | None = None,
        price_field: PriceField = "bid",
    ) -> None:
        super().__init__(config or A3CV7Config(), price_field)

    def _confidence(self, macro: float) -> float:
        return self._config.confidence_floor + (1.0 - self._config.confidence_floor) * abs(macro)

    def _advance_completed_minute(self, price: float, event_time: datetime) -> _MinuteUpdate:
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
                            minute_return / max(prior_rms, self._config.scale_floor),
                        ),
                    )
                    direction = 1 if prior_macro > 0 else -1
                    if self._evidence_direction == 0:
                        self._evidence_direction = direction
                    elif self._evidence_direction != direction:
                        self._reset_bar_progress()
                        self._evidence_direction = direction
                        direction_reset = True
                    aligned = direction * normalized
                    confidence = self._confidence(prior_macro)
                    delta = confidence * (max(aligned, 0.0) - self._config.counter_return_penalty * max(-aligned, 0.0))
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

    def _trend_active(self, minute_update: _MinuteUpdate) -> bool:
        macro = minute_update.current_macro_drift
        direction = 1 if macro > 0 else -1 if macro < 0 else 0
        return bool(
            len(self._minute_returns) >= self._config.trend_minimum_returns
            and abs(macro) >= self._config.trend_drift_threshold
            and direction != 0
            and self._evidence_direction == direction
            and self._progress > 0.0
        )

    def _open_bar(self, tick: dict[str, Any], price: float, tick_time: datetime) -> dict[str, Any]:
        bar = super()._open_bar(tick, price, tick_time)
        bar["timeframe"] = A3C_V7_TIMEFRAME
        bar["confidence_at_close"] = None
        bar["trend_active_at_close"] = False
        bar["trend_elapsed_completed_minutes"] = 0
        return bar

    def _complete_bar(self, reason: str, minute_update: _MinuteUpdate | None = None) -> dict[str, Any]:
        if self._bar is None:
            raise RuntimeError("cannot complete an absent A3C-v7 bar")
        macro = minute_update.current_macro_drift if minute_update is not None else self._macro_drift()
        self._bar["confidence_at_close"] = self._confidence(macro)
        self._bar["trend_active_at_close"] = bool(minute_update is not None and self._trend_active(minute_update))
        return super()._complete_bar(reason, minute_update)

    def _process_tick(self, tick: dict[str, Any], price: float, tick_time: datetime) -> dict[str, Any] | None:
        if self._bar is None:
            self._bar = self._open_bar(tick, price, tick_time)
        bar = self._bar
        previous_price = self._previous_price
        count = int(bar["tick_volume"]) + 1
        if count > 1 and previous_price is not None and previous_price > 0:
            bar["path_log_return"] = float(bar["path_log_return"]) + abs(math.log(price / previous_price))
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
        bar["mean_spread"] = (float(bar["mean_spread"]) * (count - 1) + max(0.0, spread)) / count
        bar["duration_ms"] = max(
            0,
            round((tick_time - _as_datetime(bar["time"])).total_seconds() * 1000),
        )

        minute_update = self._advance_completed_minute(price, tick_time)
        current_bucket = self._minute_bucket
        if current_bucket is not None:
            start_bucket = self._bucket(_as_datetime(bar["time"]))
            bar["trend_elapsed_completed_minutes"] = max(
                0,
                int((current_bucket - start_bucket).total_seconds() // 60),
            )
        is_price_event = previous_price is not None and price != previous_price
        self._previous_price = price
        self._previous_time = tick_time
        if is_price_event:
            bar["price_event_count"] = int(bar["price_event_count"]) + 1
        if minute_update.completed_return:
            if minute_update.delta > 0:
                bar["aligned_minute_returns"] = int(bar["aligned_minute_returns"]) + 1
            elif minute_update.delta < 0:
                bar["counter_minute_returns"] = int(bar["counter_minute_returns"]) + 1
        if minute_update.direction_reset:
            bar["macro_direction_resets"] = int(bar["macro_direction_resets"]) + 1
        if float(bar["path_log_return"]) > 1e-18:
            bar["realized_directional_efficiency"] = min(
                1.0,
                abs(math.log(float(bar["close"]) / float(bar["open"]))) / float(bar["path_log_return"]),
            )

        reason: str | None = None
        if self._progress >= self._config.evidence_budget:
            reason = "completed_minute_dual_evidence"
        elif int(
            bar["trend_elapsed_completed_minutes"]
        ) * 60_000 >= self._config.trend_max_duration_ms and self._trend_active(minute_update):
            reason = "completed_minute_trend_duration"
        elif int(bar["duration_ms"]) >= self._config.max_duration_ms:
            reason = "neutral_duration_guard"
        if count >= self._config.hard_max_raw_ticks:
            reason = reason or "raw_tick_safety_guard"
        if reason is None:
            return None
        return self._complete_bar(reason, minute_update)


def build_a3c_v7_bars(
    ticks: list[dict[str, Any]],
    config: A3CV7Config | None = None,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic A3C-v7 bars from one ascending causal tick prefix."""
    builder = A3CV7Builder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
