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
        # Populate registry so validation works against it
        import src.api.symbol_registry as reg
        reg._mt5_symbols["EURUSD"] = "Euro vs US Dollar"
        reg._loaded = True
        try:
            result = validate_symbol("eurusd")
            assert result == "EURUSD"
        finally:
            reg._mt5_symbols.clear()
            reg._loaded = False

    def test_invalid_symbol(self):
        import src.api.symbol_registry as reg
        reg._mt5_symbols["EURUSD"] = "Euro vs US Dollar"
        reg._loaded = True
        try:
            with pytest.raises(HTTPException) as exc_info:
                validate_symbol("INVALID")
            assert exc_info.value.status_code == 404
        finally:
            reg._mt5_symbols.clear()
            reg._loaded = False

    def test_fallback_when_registry_not_loaded(self):
        """When registry hasn't been populated, any symbol is accepted."""
        import src.api.symbol_registry as reg
        reg._loaded = False
        reg._mt5_symbols.clear()
        result = validate_symbol("ANYTHING")
        assert result == "ANYTHING"
