"""
Tests for src.metrics.PollerMetrics.
"""

from __future__ import annotations

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
