"""
Tests for crash-safe trader deal sync window selection.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.trader_main import _EPOCH, _select_deal_sync_window


def test_force_full_uses_epoch():
    now = datetime(2026, 6, 22, 9, tzinfo=timezone.utc)
    last = datetime(2026, 6, 22, 8, tzinfo=timezone.utc)

    start, phase = _select_deal_sync_window(
        now=now,
        last_deal_time=last,
        force_full=True,
        first_run=False,
        deep_resync=False,
        incremental_overlap_days=3,
        startup_catchup_days=14,
        deep_resync_days=14,
    )

    assert start == _EPOCH
    assert phase == "force_full"


def test_startup_catchup_ignores_late_open_deal_watermark():
    now = datetime(2026, 6, 22, 9, tzinfo=timezone.utc)
    last = datetime(2026, 6, 22, 8, 20, tzinfo=timezone.utc)

    start, phase = _select_deal_sync_window(
        now=now,
        last_deal_time=last,
        force_full=False,
        first_run=True,
        deep_resync=False,
        incremental_overlap_days=3,
        startup_catchup_days=14,
        deep_resync_days=14,
    )

    assert start == now - timedelta(days=14)
    assert phase == "startup_catchup"


def test_incremental_uses_configured_overlap():
    now = datetime(2026, 6, 22, 9, tzinfo=timezone.utc)
    last = datetime(2026, 6, 22, 8, 20, tzinfo=timezone.utc)

    start, phase = _select_deal_sync_window(
        now=now,
        last_deal_time=last,
        force_full=False,
        first_run=False,
        deep_resync=False,
        incremental_overlap_days=3,
        startup_catchup_days=14,
        deep_resync_days=14,
    )

    assert start == last - timedelta(days=3)
    assert phase == "incremental"


def test_deep_resync_uses_wider_window():
    now = datetime(2026, 6, 22, 9, tzinfo=timezone.utc)
    last = datetime(2026, 6, 22, 8, 20, tzinfo=timezone.utc)

    start, phase = _select_deal_sync_window(
        now=now,
        last_deal_time=last,
        force_full=False,
        first_run=False,
        deep_resync=True,
        incremental_overlap_days=3,
        startup_catchup_days=14,
        deep_resync_days=14,
    )

    assert start == now - timedelta(days=14)
    assert phase == "deep_resync"


def test_empty_account_uses_initial_full():
    now = datetime(2026, 6, 22, 9, tzinfo=timezone.utc)

    start, phase = _select_deal_sync_window(
        now=now,
        last_deal_time=None,
        force_full=False,
        first_run=True,
        deep_resync=False,
        incremental_overlap_days=3,
        startup_catchup_days=14,
        deep_resync_days=14,
    )

    assert start == _EPOCH
    assert phase == "initial_full"
