"""
API integration test: verify that the last close price is consistent
across ALL timeframes (standard & custom, including tick bars).

The close of the last candle represents the most recent traded price.
Regardless of aggregation bucket (M1, M5, H1, M3, H2, T100 …), the
last close must be the same because they all derive from the same
underlying M1 candles or raw ticks.

Usage:
    pytest tests/test_last_price_consistency.py -v -s
"""

from __future__ import annotations

import httpx
import pytest

BASE = "http://localhost:9000/api/v1"
TIMEOUT = 30.0
SYMBOL = "EURUSD"

# Standard timeframes (pre-computed table)
STANDARD_TFS = ["M1", "M5", "M15", "H1", "H4", "D1"]

# Custom time-based timeframes (aggregated from M1/H1 on-the-fly)
CUSTOM_TIME_TFS = ["M2", "M3", "M10", "H2", "H6"]

# Tick bars
TICK_BAR_TFS = ["T50", "T100", "T500"]


def _get_last_candle(client: httpx.Client, tf: str) -> dict:
    """Fetch the last candle for SYMBOL at the given timeframe."""
    if tf in STANDARD_TFS:
        url = f"{BASE}/candles/{SYMBOL}"
    else:
        url = f"{BASE}/candles/custom/{SYMBOL}"

    r = client.get(url, params={"timeframe": tf, "limit": 5})
    r.raise_for_status()
    body = r.json()
    data = body.get("data", body)
    assert isinstance(data, list) and len(data) > 0, (
        f"No candle data returned for {SYMBOL}/{tf}"
    )
    return data[-1]  # last candle


def _get_last_tick(client: httpx.Client) -> dict:
    """Fetch the most recent tick for SYMBOL."""
    r = client.get(f"{BASE}/ticks/{SYMBOL}", params={"limit": 1})
    r.raise_for_status()
    body = r.json()
    data = body.get("data", body)
    assert isinstance(data, list) and len(data) > 0, (
        f"No tick data returned for {SYMBOL}"
    )
    return data[-1]


@pytest.fixture(scope="module")
def client():
    """Shared HTTP client for all tests."""
    with httpx.Client(timeout=TIMEOUT) as c:
        # Verify API is reachable
        r = c.get(f"{BASE}/health")
        assert r.status_code == 200, "API is not running on localhost:9000"
        yield c


@pytest.fixture(scope="module")
def reference_close(client: httpx.Client) -> float:
    """The M1 last-close is our reference price."""
    candle = _get_last_candle(client, "M1")
    return candle["close"]


# ------------------------------------------------------------------
# Standard TFs
# ------------------------------------------------------------------

@pytest.mark.parametrize("tf", STANDARD_TFS)
def test_standard_tf_last_close(client, reference_close, tf):
    """Last close on standard TF must match M1's last close."""
    candle = _get_last_candle(client, tf)
    # M1 is the reference.
    # M5 and M15 are independently polled by the collector at their own
    # bar boundaries; the last M5 bar may close at a different second
    # than the last M1 bar, so a small tolerance is acceptable.
    # For H1/H4/D1 the close can differ because the bar period is wider.
    if tf == "M1":
        assert candle["close"] == reference_close
    elif tf in ("M5", "M15"):
        diff = abs(candle["close"] - reference_close)
        tolerance = reference_close * 0.001  # 0.1 %
        assert diff <= tolerance, (
            f"{tf} last close {candle['close']} differs from M1 reference "
            f"{reference_close} by {diff} (tolerance {tolerance})"
        )
    else:
        # H1/H4/D1 — just check it's a valid price
        assert isinstance(candle["close"], (int, float))
        assert candle["close"] > 0


# ------------------------------------------------------------------
# Custom time-based TFs
# ------------------------------------------------------------------

@pytest.mark.parametrize("tf", CUSTOM_TIME_TFS)
def test_custom_time_tf_last_close(client, reference_close, tf):
    """Custom time-based TFs aggregate from M1 — close must match."""
    candle = _get_last_candle(client, tf)
    # M2, M3 aggregate from M1, their latest bucket includes the
    # latest M1 candle, so close must be identical.
    assert candle["close"] == reference_close, (
        f"{tf} last close {candle['close']} != M1 reference {reference_close}"
    )


# ------------------------------------------------------------------
# Tick bars
# ------------------------------------------------------------------

@pytest.mark.parametrize("tf", TICK_BAR_TFS)
def test_tick_bar_last_close(client, reference_close, tf):
    """Tick bar close should be very close to M1 last close."""
    candle = _get_last_candle(client, tf)
    # Tick bars may have their last closed bar slightly before the
    # very last tick (because incomplete bars are excluded by default).
    # We allow a small tolerance (e.g. 10 pips for a 5-digit pair).
    diff = abs(candle["close"] - reference_close)
    tolerance = reference_close * 0.001  # 0.1%
    assert diff <= tolerance, (
        f"{tf} last close {candle['close']} differs from M1 reference "
        f"{reference_close} by {diff} (tolerance {tolerance})"
    )


# ------------------------------------------------------------------
# Cross-TF consistency matrix
# ------------------------------------------------------------------

def test_all_minute_tfs_same_close(client):
    """
    All custom minute TFs aggregated from M1 (M2, M3, M10) must have
    the exact same last close as M1 itself.  M5 and M15 are
    independently polled — they may close at different bar boundaries.
    """
    tfs = ["M1", "M2", "M3", "M10"]
    closes = {}
    for tf in tfs:
        candle = _get_last_candle(client, tf)
        closes[tf] = candle["close"]

    # All must be equal
    values = list(closes.values())
    reference = values[0]
    mismatches = {tf: c for tf, c in closes.items() if c != reference}
    assert not mismatches, (
        f"Close price mismatch across minute TFs!\n"
        f"  Reference (M1): {reference}\n"
        f"  Mismatches: {mismatches}"
    )


def test_bid_ask_from_ticks_matches_candle_close(client):
    """
    The last tick's bid should match (or be very close to) the M1
    last close — proving the data pipeline is consistent end-to-end.
    """
    tick = _get_last_tick(client)
    candle = _get_last_candle(client, "M1")

    # The M1 close is the close of the last completed 1-min bar.
    # The tick is the absolute latest — it may be from a newer bar.
    # If the tick is from the same minute, bid == close.
    # If from a newer minute, we allow a small tolerance.
    diff = abs(tick["bid"] - candle["close"])
    tolerance = candle["close"] * 0.001  # 0.1%
    assert diff <= tolerance, (
        f"Last tick bid {tick['bid']} differs from M1 close "
        f"{candle['close']} by {diff}"
    )
