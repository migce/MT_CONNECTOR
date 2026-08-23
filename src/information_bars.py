"""Causal adaptive information-time bars built from stored or live ticks.

The builder deliberately knows nothing about downstream indicators.  It uses
only the selected tick-price path.  A fast/slow EWMA ratio measures how much
absolute return activity has accelerated, while signed-vs-absolute EWMA return
coherence discounts two-sided chop.  Their bounded product is the information
weight contributed by the current tick.

Version 1 is a research/charting contract.  Query-prefix anchoring and the
current tick-store identity are not sufficient for strategy execution.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Literal

PriceField = Literal["bid", "ask", "last", "mid"]

INFORMATION_BAR_ALGORITHM = "adaptive-information-bars-v1"


@dataclass(frozen=True)
class InformationBarConfig:
    """Frozen parameters for the first adaptive information-clock version."""

    budget: int
    fast_span: int = 64
    slow_span: int = 1024
    coherence_span: int = 64
    directional_floor: float = 0.25
    min_weight: float = 0.25
    max_weight: float = 4.0

    def __post_init__(self) -> None:
        if self.budget < 2:
            raise ValueError("Information bar budget must be >= 2.")
        if min(self.fast_span, self.slow_span, self.coherence_span) < 2:
            raise ValueError("Information-clock EWMA spans must be >= 2.")
        if self.fast_span >= self.slow_span:
            raise ValueError("Information-clock fast_span must be below slow_span.")
        if not 0 < self.directional_floor <= 1:
            raise ValueError("directional_floor must be in (0, 1].")
        if not 0 < self.min_weight <= 1 <= self.max_weight:
            raise ValueError("Weights must satisfy 0 < min_weight <= 1 <= max_weight.")

    @property
    def max_ticks_per_bar(self) -> int:
        """Hard upper bound implied by ``min_weight``."""
        return math.ceil(self.budget / self.min_weight)

    def metadata(self) -> dict[str, Any]:
        return {
            "algorithm": INFORMATION_BAR_ALGORITHM,
            **asdict(self),
            "max_ticks_per_bar": self.max_ticks_per_bar,
        }


def information_source_limit(
    config: InformationBarConfig,
    bar_limit: int,
    *,
    hard_cap: int = 1_000_000,
) -> int:
    """Return a bounded source window for an on-demand chart request."""
    warmup_ticks = config.slow_span * 4
    requested = config.max_ticks_per_bar * (max(1, bar_limit) + 2) + warmup_ticks
    return min(hard_cap, requested)


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000 if float(value) >= 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def tick_price(tick: dict[str, Any], price_field: PriceField) -> float | None:
    """Resolve a usable positive selected price from a raw tick."""
    if price_field == "mid":
        bid = float(tick.get("bid") or 0)
        ask = float(tick.get("ask") or 0)
        price = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
    else:
        price = float(tick.get(price_field) or 0)
    return price if math.isfinite(price) and price > 0 else None


class InformationBarBuilder:
    """Stateful causal builder shared by historical and live aggregation."""

    __slots__ = (
        "_activity_sum",
        "_bar",
        "_coherence_abs",
        "_coherence_sum",
        "_config",
        "_fast_abs",
        "_information",
        "_previous_price",
        "_price_field",
        "_signed_return",
        "_slow_abs",
        "_weight_sum",
    )

    def __init__(
        self,
        config: InformationBarConfig,
        price_field: PriceField = "bid",
    ) -> None:
        self._config = config
        self._price_field = price_field
        self._previous_price: float | None = None
        self._fast_abs: float | None = None
        self._slow_abs: float | None = None
        self._signed_return: float | None = None
        self._coherence_abs: float | None = None
        self._bar: dict[str, Any] | None = None
        self._information = 0.0
        self._activity_sum = 0.0
        self._coherence_sum = 0.0
        self._weight_sum = 0.0

    @staticmethod
    def _alpha(span: int) -> float:
        return 2.0 / (span + 1.0)

    @staticmethod
    def _ewma(previous: float | None, value: float, alpha: float) -> float:
        return value if previous is None else previous + alpha * (value - previous)

    def _clock(self, price: float) -> tuple[float, float, float]:
        previous = self._previous_price
        self._previous_price = price
        if previous is None or previous <= 0:
            return self._config.min_weight, 0.0, 0.0

        signed_return = math.log(price / previous)
        absolute_return = abs(signed_return)
        self._fast_abs = self._ewma(
            self._fast_abs,
            absolute_return,
            self._alpha(self._config.fast_span),
        )
        self._slow_abs = self._ewma(
            self._slow_abs,
            absolute_return,
            self._alpha(self._config.slow_span),
        )
        self._signed_return = self._ewma(
            self._signed_return,
            signed_return,
            self._alpha(self._config.coherence_span),
        )
        self._coherence_abs = self._ewma(
            self._coherence_abs,
            absolute_return,
            self._alpha(self._config.coherence_span),
        )

        activity = (
            0.0
            if not self._slow_abs or self._slow_abs <= 1e-15
            else self._fast_abs / self._slow_abs
        )
        if not self._coherence_abs or self._coherence_abs <= 1e-15:
            coherence = 0.0
        else:
            coherence = min(1.0, abs(self._signed_return or 0.0) / self._coherence_abs)

        directional_weight = self._config.directional_floor + (
            1.0 - self._config.directional_floor
        ) * coherence
        raw_weight = activity * directional_weight
        weight = min(self._config.max_weight, max(self._config.min_weight, raw_weight))
        return weight, activity, coherence

    def update(
        self,
        tick: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Feed one tick and return ``(completed, current)``."""
        price = tick_price(tick, self._price_field)
        if price is None:
            return None, self._bar

        raw_time = tick.get("time_msc", tick.get("time"))
        if raw_time is None:
            return None, self._bar
        tick_time = _as_datetime(raw_time)
        weight, activity, coherence = self._clock(price)
        bid = float(tick.get("bid") or 0)
        ask = float(tick.get("ask") or 0)
        spread = ask - bid if bid > 0 and ask > 0 else 0.0

        if self._bar is None:
            self._bar = {
                "time": tick_time,
                "end_time": tick_time,
                "symbol": str(tick.get("symbol") or ""),
                "timeframe": f"I{self._config.budget}",
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
                "information_value": 0.0,
                "mean_activity": 0.0,
                "mean_coherence": 0.0,
                "mean_information_weight": 0.0,
                "is_complete": False,
            }
            self._information = 0.0
            self._activity_sum = 0.0
            self._coherence_sum = 0.0
            self._weight_sum = 0.0

        bar = self._bar
        count = int(bar["tick_volume"]) + 1
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
        bar["mean_spread"] = (
            (float(bar["mean_spread"]) * (count - 1) + max(0.0, spread)) / count
        )
        bar["duration_ms"] = max(
            0,
            round((tick_time - _as_datetime(bar["time"])).total_seconds() * 1000),
        )

        self._information += weight
        self._activity_sum += activity
        self._coherence_sum += coherence
        self._weight_sum += weight
        bar["information_value"] = self._information
        bar["mean_activity"] = self._activity_sum / count
        bar["mean_coherence"] = self._coherence_sum / count
        bar["mean_information_weight"] = self._weight_sum / count

        if count >= 2 and self._information >= self._config.budget:
            bar["is_complete"] = True
            completed = bar
            self._bar = None
            return completed, completed
        return None, bar

    @property
    def current(self) -> dict[str, Any] | None:
        return self._bar


def build_information_bars(
    ticks: list[dict[str, Any]],
    config: InformationBarConfig,
    *,
    price_field: PriceField = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """Build deterministic bars from an ascending tick prefix."""
    builder = InformationBarBuilder(config, price_field)
    bars: list[dict[str, Any]] = []
    for tick in ticks:
        completed, _current = builder.update(tick)
        if completed is not None:
            bars.append(completed)
    if include_incomplete and builder.current is not None:
        bars.append(builder.current)
    return bars
