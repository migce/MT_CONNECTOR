"""
Tests for src.metrics.PollerMetrics.
"""

from __future__ import annotations

import time
from unittest.mock import patch

from src.metrics import PollerMetrics


class TestPollerMetrics:
    def test_singleton(self):
        PollerMetrics.reset()
        a = PollerMetrics()
        b = PollerMetrics()
        assert a is b
        PollerMetrics.reset()

    def test_reset_creates_new_instance(self):
        PollerMetrics.reset()
        a = PollerMetrics()
        a.ticks_total = 42
        PollerMetrics.reset()
        b = PollerMetrics()
        assert b.ticks_total == 0
        assert a is not b
        PollerMetrics.reset()

    def test_record_tick(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        m.record_tick("EURUSD", 1.1, 1.2)
        assert m.ticks_total == 1
        assert m.symbol_ticks["EURUSD"].count == 1
        PollerMetrics.reset()

    def test_record_error(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        m.record_error("test_cat")
        assert m.errors["test_cat"] == 1
        assert m.total_errors() == 1
        PollerMetrics.reset()

    def test_tick_minute_buckets(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        m.record_tick("EURUSD", 1.1, 1.2)
        m.record_tick("EURUSD", 1.1, 1.2)
        m.record_tick("GBPUSD", 1.3, 1.4)
        # All 3 ticks should be in the current minute bucket
        assert m.ticks_in_window(3600) == 3
        assert m.ticks_in_window(86400) == 3
        PollerMetrics.reset()

    def test_candle_minute_buckets(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        m.record_candle_upsert(5)
        m.record_candle_upsert(3)
        assert m.candles_in_window(3600) == 8
        assert m.candles_total == 8
        PollerMetrics.reset()

    def test_prune_minute_buckets(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        # Insert a bucket far in the past (10 days ago)
        old_minute = m._current_minute() - (10 * 24 * 60)
        m._tick_minute_buckets[old_minute] = 100
        m._candle_minute_buckets[old_minute] = 50
        m.prune_minute_buckets()
        assert old_minute not in m._tick_minute_buckets
        assert old_minute not in m._candle_minute_buckets
        PollerMetrics.reset()

    def test_update_api_health(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        m.update_api_health(
            healthy=True,
            latency_ms=12.5,
            requests_1h=100,
            requests_12h=500,
            requests_24h=1000,
            errors_1h=2,
            avg_latency_ms=15.0,
        )
        assert m.api_healthy is True
        assert m.api_latency_ms == 12.5
        assert m.api_requests_1h == 100
        assert m.api_errors_1h == 2
        PollerMetrics.reset()

    def test_update_infra_health(self):
        PollerMetrics.reset()
        m = PollerMetrics()
        m.update_infra_health(db_ok=True, redis_ok=False, db_latency_ms=3.2, redis_latency_ms=0.0)
        assert m.db_healthy is True
        assert m.redis_healthy is False
        assert m.db_latency_ms == 3.2
        PollerMetrics.reset()
