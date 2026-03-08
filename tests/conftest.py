"""
Shared test fixtures.
"""

from __future__ import annotations

import os

import pytest

# Ensure settings can load without a real .env in CI
os.environ.setdefault("MT5_LOGIN", "0")
os.environ.setdefault("MT5_PASSWORD", "test")
os.environ.setdefault("MT5_SERVER", "test")
os.environ.setdefault("DB_PASSWORD", "test")


@pytest.fixture(autouse=True)
def _reset_settings():
    """Reset the settings singleton between tests."""
    from src.config import reload_settings
    reload_settings()
    yield
    reload_settings()
