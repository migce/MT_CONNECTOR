"""
Real-time candle aggregators for WebSocket streaming.

Two aggregator types:
  - ``CandleAggregator`` — merges source candles (M1/H1) into arbitrary
    time-based buckets (M2, M3, H2, H6, …).
  - ``TickBarAggregator`` — groups raw ticks into N-tick OHLCV bars.

Both follow the same protocol:
    completed, current = aggregator.update(source_message)
    # completed: a finished bar (or None)
    # current:   the live (incomplete) bar — always returned
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class CandleAggregator:
    """Aggregate source candles into a custom time-based bucket."""

    __slots__ = ("_bucket_sec", "_tf_label", "_bar", "_bucket_ts")

    def __init__(self, bucket_seconds: int, tf_label: str) -> None:
        self._bucket_sec = bucket_seconds
        self._tf_label = tf_label
        self._bar: dict[str, Any] | None = None
        self._bucket_ts: int | None = None  # epoch seconds of current bucket

    # ------------------------------------------------------------------

    def _bucket_start(self, epoch: int) -> int:
        return (epoch // self._bucket_sec) * self._bucket_sec

    def update(
        self, candle: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """
        Feed a source candle.  Returns ``(completed_bar | None, live_bar)``.
        """
        t = candle.get("time")
        if isinstance(t, str):
            t = int(datetime.fromisoformat(t).replace(tzinfo=timezone.utc).timestamp())
        elif isinstance(t, datetime):
            t = int(t.replace(tzinfo=timezone.utc).timestamp())
        else:
            t = int(t)

        bucket = self._bucket_start(t)
        completed: dict[str, Any] | None = None

        if self._bar is None or bucket != self._bucket_ts:
            # New bucket → emit previous bar as completed
            if self._bar is not None:
                completed = self._bar
            self._bucket_ts = bucket
            self._bar = {
                "time": datetime.fromtimestamp(bucket, tz=timezone.utc).isoformat(),
                "symbol": candle["symbol"],
                "timeframe": self._tf_label,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "tick_volume": candle.get("tick_volume", 0),
                "real_volume": candle.get("real_volume", 0),
                "spread": candle.get("spread", 0),
            }
        else:
            # Same bucket → merge
            self._bar["high"] = max(self._bar["high"], candle["high"])
            self._bar["low"] = min(self._bar["low"], candle["low"])
            self._bar["close"] = candle["close"]
            self._bar["tick_volume"] += candle.get("tick_volume", 0)
            self._bar["real_volume"] += candle.get("real_volume", 0)
            self._bar["spread"] = candle.get("spread", 0)

        return completed, self._bar


class TickBarAggregator:
    """Aggregate raw ticks into N-tick OHLCV bars."""

    __slots__ = ("_n", "_tf_label", "_price_field", "_bar", "_count")

    def __init__(
        self,
        tick_count: int,
        tf_label: str,
        price_field: str = "bid",
    ) -> None:
        self._n = tick_count
        self._tf_label = tf_label
        self._price_field = price_field
        self._bar: dict[str, Any] | None = None
        self._count = 0

    def _price(self, tick: dict[str, Any]) -> float:
        if self._price_field == "mid":
            return (tick.get("bid", 0) + tick.get("ask", 0)) / 2.0
        return tick.get(self._price_field, tick.get("bid", 0))

    def update(
        self, tick: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """
        Feed a tick.  Returns ``(completed_bar | None, live_bar)``.
        """
        price = self._price(tick)
        completed: dict[str, Any] | None = None

        if self._bar is not None and self._count >= self._n:
            # Current bar is full → emit it and start fresh
            completed = self._bar
            self._bar = None
            self._count = 0

        if self._bar is None:
            t = tick.get("time_msc") or tick.get("time", "")
            if isinstance(t, datetime):
                t = t.isoformat()
            self._bar = {
                "time": t if isinstance(t, str) else str(t),
                "symbol": tick["symbol"],
                "timeframe": self._tf_label,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "tick_volume": 1,
                "real_volume": tick.get("volume", 0),
                "spread": round(abs(tick.get("ask", 0) - tick.get("bid", 0)), 6),
            }
            self._count = 1
        else:
            self._bar["high"] = max(self._bar["high"], price)
            self._bar["low"] = min(self._bar["low"], price)
            self._bar["close"] = price
            self._count += 1
            self._bar["tick_volume"] = self._count
            self._bar["real_volume"] += tick.get("volume", 0)
            self._bar["spread"] = round(
                abs(tick.get("ask", 0) - tick.get("bid", 0)), 6
            )

        return completed, self._bar
