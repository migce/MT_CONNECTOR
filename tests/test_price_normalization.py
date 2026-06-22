"""Tests for IEEE-754 float artifact elimination.

Covers:
- normalize_price / normalize_money utility functions
- Pydantic schema validators for all response models
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.api.digits import normalize_money, normalize_price

# ---------------------------------------------------------------
# Unit tests: normalize_price
# ---------------------------------------------------------------


class TestNormalizePrice:
    def test_removes_float_artifact(self):
        assert normalize_price(1.1521100000000002, 5) == 1.15211

    def test_removes_float_artifact_3_digits(self):
        assert normalize_price(110.85200000000003, 3) == 110.852

    def test_zero_returns_zero(self):
        assert normalize_price(0.0, 5) == 0.0

    def test_already_clean(self):
        assert normalize_price(1.12345, 5) == 1.12345

    def test_rounds_half_even(self):
        # Banker's rounding: 1.123455 → 1.12346 (round to even)
        assert normalize_price(1.123455, 5) == 1.12346

    def test_negative_price(self):
        assert normalize_price(-0.50000000000000004, 2) == -0.5

    def test_integer_value(self):
        assert normalize_price(100.0, 2) == 100.0

    def test_small_digits_0(self):
        assert normalize_price(12345.6789, 0) == 12346.0


# ---------------------------------------------------------------
# Unit tests: normalize_money
# ---------------------------------------------------------------


class TestNormalizeMoney:
    def test_removes_float_artifact(self):
        assert normalize_money(50.0300000000001) == 50.03

    def test_zero_returns_zero(self):
        assert normalize_money(0.0) == 0.0

    def test_negative_money(self):
        assert normalize_money(-123.4567) == -123.46

    def test_custom_digits(self):
        assert normalize_money(99.9996, digits=3) == 100.0

    def test_large_amount(self):
        assert normalize_money(1_000_000.009999999) == 1_000_000.01


# ---------------------------------------------------------------
# Schema integration tests
# ---------------------------------------------------------------

# Patch get_digits to return a known value for test symbols
_TEST_DIGITS = {"EURUSD": 5, "USDJPY": 3}


@pytest.fixture(autouse=True)
def _mock_digits():
    with patch("src.api.schemas.get_digits", side_effect=lambda s: _TEST_DIGITS.get(s, 5)):
        yield


class TestCandleResponseRounding:
    def test_price_fields_cleaned(self):
        from src.api.schemas import CandleResponse

        c = CandleResponse(
            symbol="EURUSD",
            time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            timeframe="M1",
            open=1.1521100000000002,
            high=1.1535000000000001,
            low=1.1510999999999998,
            close=1.1525300000000003,
            tick_volume=100,
            spread=10,
            real_volume=0,
        )
        assert c.open == 1.15211
        assert c.high == 1.1535
        assert c.low == 1.1511
        assert c.close == 1.15253


class TestTickResponseRounding:
    def test_price_fields_cleaned(self):
        from src.api.schemas import TickResponse

        t = TickResponse(
            symbol="USDJPY",
            time_msc=datetime(2025, 1, 1, tzinfo=timezone.utc),
            bid=110.85200000000003,
            ask=110.85500000000001,
            last=0.0,
            volume=0,
            flags=0,
        )
        assert t.bid == 110.852
        assert t.ask == 110.855
        assert t.last == 0.0


class TestDealResponseRounding:
    def test_price_and_money_cleaned(self):
        from src.api.schemas import DealResponse

        d = DealResponse(
            ticket=1,
            account_id=100,
            order=1,
            time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            time_msc=0,
            type=0,
            entry=0,
            symbol="EURUSD",
            volume=0.10000000000000001,
            price=1.1521100000000002,
            commission=-3.500000000000001,
            swap=0.0,
            profit=25.300000000000004,
            fee=0.0,
        )
        assert d.price == 1.15211
        assert d.volume == 0.1
        assert d.commission == -3.5
        assert d.profit == 25.3


class TestPositionResponseRounding:
    def test_price_and_money_cleaned(self):
        from src.api.schemas import PositionResponse

        p = PositionResponse(
            ticket=2,
            account_id=100,
            time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            type=0,
            symbol="EURUSD",
            volume=1.0,
            price_open=1.1521100000000002,
            price_current=1.1535000000000001,
            sl=1.1500000000000001,
            tp=1.1600000000000001,
            swap=-0.50000000000000004,
            profit=13.900000000000002,
        )
        assert p.price_open == 1.15211
        assert p.price_current == 1.1535
        assert p.sl == 1.15
        assert p.tp == 1.16
        assert p.swap == -0.5
        assert p.profit == 13.9


class TestAccountInfoResponseRounding:
    def test_money_fields_cleaned(self):
        from src.api.schemas import AccountInfoResponse

        a = AccountInfoResponse(
            account_id=100,
            balance=10000.0000000001,
            equity=10050.3000000002,
            margin=500.1500000000001,
            margin_free=9550.1500000000001,
            margin_level=2000.0600000000001,
            leverage=100,
            currency="USD",
            profit=50.3000000000001,
        )
        assert a.balance == 10000.0
        assert a.equity == 10050.3
        assert a.margin == 500.15
        assert a.margin_free == 9550.15
        assert a.margin_level == 2000.06
        assert a.profit == 50.3


class TestSpreadPointRounding:
    def test_spread_cleaned(self):
        from src.api.schemas import SpreadPoint

        s = SpreadPoint(
            time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            spread=0.000120000000000001,
        )
        assert s.spread == 0.00012


class TestSpreadAggPointRounding:
    def test_agg_spread_cleaned(self):
        from src.api.schemas import SpreadAggPoint

        s = SpreadAggPoint(
            time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            spread_avg=0.000150000000000001,
            spread_min=0.000100000000000001,
            spread_max=0.000200000000000001,
        )
        assert s.spread_avg == 0.00015
        assert s.spread_min == 0.0001
        assert s.spread_max == 0.0002
