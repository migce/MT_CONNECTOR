"""
Tests for API validation helpers.
"""

from __future__ import annotations

import os

import pytest

# Ensure test settings load
os.environ.setdefault("MT5_LOGIN", "0")
os.environ.setdefault("MT5_PASSWORD", "test")
os.environ.setdefault("MT5_SERVER", "test")
os.environ.setdefault("DB_PASSWORD", "test")

from fastapi import HTTPException

from src.api.services.validation import validate_symbol


class TestValidateSymbol:
    def test_valid_symbol(self):
        # Default symbols_csv is "EURUSD"
        result = validate_symbol("eurusd")
        assert result == "EURUSD"

    def test_invalid_symbol(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_symbol("INVALID")
        assert exc_info.value.status_code == 404
