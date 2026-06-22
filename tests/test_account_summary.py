"""
Tests for the account-info position summary fields.

Covers:
  - AccountInfoResponse schema: flat account (no positions), account with positions
  - Volume rounding and has_open_positions derivation
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("MT5_LOGIN", "0")
os.environ.setdefault("MT5_PASSWORD", "test")
os.environ.setdefault("MT5_SERVER", "test")
os.environ.setdefault("DB_PASSWORD", "test")

from datetime import datetime, timezone

from src.api.schemas import AccountInfoResponse


class TestAccountInfoResponseFlat:
    """Account with zero open positions (flat)."""

    def test_defaults(self):
        resp = AccountInfoResponse(
            account_id=1,
            balance=1000.0,
            equity=1000.0,
            margin=0.0,
            margin_free=1000.0,
            margin_level=0.0,
            leverage=100,
            currency="USD",
            profit=0.0,
        )
        assert resp.open_positions_count == 0
        assert resp.open_volume_lots == 0.0
        assert resp.has_open_positions is False

    def test_explicit_zero(self):
        resp = AccountInfoResponse(
            account_id=2,
            balance=500.0,
            equity=500.0,
            margin=0.0,
            margin_free=500.0,
            margin_level=0.0,
            leverage=200,
            currency="EUR",
            profit=0.0,
            open_positions_count=0,
            open_volume_lots=0.0,
        )
        assert resp.has_open_positions is False
        assert resp.open_volume_lots == 0.0


class TestAccountInfoResponseWithPositions:
    """Account with open positions."""

    def test_basic(self):
        resp = AccountInfoResponse(
            account_id=3,
            balance=10000.0,
            equity=10050.5,
            margin=120.0,
            margin_free=9930.5,
            margin_level=8375.42,
            leverage=1000,
            currency="USD",
            profit=50.5,
            open_positions_count=3,
            open_volume_lots=0.15,
        )
        assert resp.open_positions_count == 3
        assert resp.open_volume_lots == 0.15
        assert resp.has_open_positions is True

    def test_volume_rounding(self):
        """open_volume_lots should be rounded to 2 decimals via normalize_money."""
        resp = AccountInfoResponse(
            account_id=4,
            balance=5000.0,
            equity=5100.0,
            margin=50.0,
            margin_free=5050.0,
            margin_level=10200.0,
            leverage=500,
            currency="USD",
            profit=100.0,
            open_positions_count=5,
            open_volume_lots=1.23456789,
        )
        assert resp.open_volume_lots == 1.23
        assert resp.has_open_positions is True

    def test_single_position(self):
        resp = AccountInfoResponse(
            account_id=5,
            balance=2000.0,
            equity=2010.0,
            margin=20.0,
            margin_free=1990.0,
            margin_level=10050.0,
            leverage=100,
            currency="USD",
            profit=10.0,
            open_positions_count=1,
            open_volume_lots=0.01,
        )
        assert resp.open_positions_count == 1
        assert resp.has_open_positions is True
        assert resp.open_volume_lots == 0.01

    def test_has_open_positions_derived_from_count(self):
        """has_open_positions=False input should be overridden by count > 0."""
        resp = AccountInfoResponse(
            account_id=6,
            balance=1000.0,
            equity=1000.0,
            margin=0.0,
            margin_free=1000.0,
            margin_level=0.0,
            leverage=100,
            currency="USD",
            profit=0.0,
            open_positions_count=2,
            open_volume_lots=0.02,
            has_open_positions=False,  # should be overridden
        )
        assert resp.has_open_positions is True
