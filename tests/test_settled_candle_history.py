"""Regression coverage for completed native-candle self-healing."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ.setdefault("MT5_LOGIN", "0")
os.environ.setdefault("MT5_PASSWORD", "test")
os.environ.setdefault("MT5_SERVER", "test")
os.environ.setdefault("DB_PASSWORD", "test")

from src.config import Timeframe
from src.mt5.backfill import Backfiller


def _settings(*, symbols: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        symbols=symbols or ["EURUSD"],
        timeframes=[Timeframe.H1, Timeframe.M15, Timeframe.M1, Timeframe.M5],
        backfill_days=30,
        backfill_candle_batch_rows=2_000,
        candle_settlement_refresh_hours=24,
    )


def test_last_completed_open_excludes_every_developing_timeframe() -> None:
    reference = datetime(2026, 8, 27, 10, 37, 42, tzinfo=timezone.utc)

    assert Backfiller._last_completed_open(reference, Timeframe.M1) == datetime(
        2026, 8, 27, 10, 36, tzinfo=timezone.utc
    )
    assert Backfiller._last_completed_open(reference, Timeframe.M5) == datetime(
        2026, 8, 27, 10, 30, tzinfo=timezone.utc
    )
    assert Backfiller._last_completed_open(reference, Timeframe.M15) == datetime(
        2026, 8, 27, 10, 15, tzinfo=timezone.utc
    )
    assert Backfiller._last_completed_open(reference, Timeframe.H1) == datetime(
        2026, 8, 27, 9, 0, tzinfo=timezone.utc
    )


def test_tick_repair_range_excludes_partial_edge_buckets() -> None:
    result = Backfiller._closed_tick_repair_range(
        datetime(2026, 8, 31, 11, 6, 15, tzinfo=timezone.utc),
        datetime(2026, 8, 31, 11, 13, 8, tzinfo=timezone.utc),
        Timeframe.M1,
    )

    assert result == (
        datetime(2026, 8, 31, 11, 7, tzinfo=timezone.utc),
        datetime(2026, 8, 31, 11, 13, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_explicit_tick_repair_rebuilds_only_absent_closed_candles() -> None:
    backfiller = Backfiller(MagicMock(), settings=_settings())
    backfiller.on_demand_ticks = AsyncMock(return_value=600)
    start = datetime(2026, 8, 31, 11, 6, tzinfo=timezone.utc)
    end = datetime(2026, 8, 31, 11, 13, tzinfo=timezone.utc)

    with (
        patch("src.mt5.backfill.run_in_mt5", new=AsyncMock(return_value=None)),
        patch("src.mt5.backfill.get_digits", return_value=5),
        patch(
            "src.mt5.backfill.repo.insert_missing_candles_from_ticks",
            new=AsyncMock(return_value=6),
        ) as rebuild,
        patch(
            "src.mt5.backfill.repo.get_latest_candle_time",
            new=AsyncMock(return_value=datetime(2026, 8, 31, 11, 12, tzinfo=timezone.utc)),
        ),
        patch(
            "src.mt5.backfill.repo.update_sync_state",
            new=AsyncMock(),
        ) as update_sync,
    ):
        rows = await backfiller.on_demand_candles(
            "EURUSD",
            "M1",
            start,
            end,
            repair_from_ticks=True,
        )

    assert rows == 6
    backfiller.on_demand_ticks.assert_awaited_once_with("EURUSD", start, end)
    rebuild.assert_awaited_once_with(
        symbol="EURUSD",
        timeframe="M1",
        bucket_seconds=60,
        dt_from=start,
        dt_to=end,
        spread_scale=100_000,
    )
    update_sync.assert_awaited_once_with(
        "EURUSD",
        "M1",
        datetime(2026, 8, 31, 11, 12, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_settlement_refresh_is_base_first_and_rereads_existing_range() -> None:
    backfiller = Backfiller(MagicMock(), settings=_settings())
    backfiller.on_demand_candles = AsyncMock(side_effect=[60, 12, 4, 1])
    start = datetime(2026, 8, 27, 8, 0, tzinfo=timezone.utc)
    end = datetime(2026, 8, 27, 10, 37, 42, tzinfo=timezone.utc)

    result = await backfiller.refresh_settled_candles("EURUSD", start, end)

    assert result == {"M1": 60, "M5": 12, "M15": 4, "H1": 1}
    assert [call.args[1] for call in backfiller.on_demand_candles.await_args_list] == [
        "M1",
        "M5",
        "M15",
        "H1",
    ]
    assert [call.args[3] for call in backfiller.on_demand_candles.await_args_list] == [
        datetime(2026, 8, 27, 10, 36, tzinfo=timezone.utc),
        datetime(2026, 8, 27, 10, 30, tzinfo=timezone.utc),
        datetime(2026, 8, 27, 10, 15, tzinfo=timezone.utc),
        datetime(2026, 8, 27, 9, 0, tzinfo=timezone.utc),
    ]


@pytest.mark.asyncio
async def test_initial_backfill_force_refreshes_overlap_after_sync_backfill() -> None:
    backfiller = Backfiller(MagicMock(), settings=_settings())
    backfiller._backfill_candles = AsyncMock()
    backfiller.refresh_settled_candles = AsyncMock(return_value={})
    backfiller._backfill_ticks = AsyncMock()

    await backfiller.run_initial_backfill()

    assert backfiller._backfill_candles.await_count == 4
    refresh = backfiller.refresh_settled_candles.await_args
    assert refresh.args[0] == "EURUSD"
    assert timedelta(hours=23, minutes=59) <= refresh.args[2] - refresh.args[1] <= timedelta(
        hours=24, minutes=1
    )
    backfiller._backfill_ticks.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconnect_refreshes_existing_rows_without_absence_scan() -> None:
    backfiller = Backfiller(MagicMock(), settings=_settings())
    backfiller.refresh_settled_candles = AsyncMock(return_value={})
    backfiller.on_demand_ticks = AsyncMock(return_value=0)

    with patch("src.mt5.backfill.repo.find_candle_gaps", new_callable=AsyncMock) as gaps:
        await backfiller.run_reconnect_backfill()

    gaps.assert_not_awaited()
    backfiller.refresh_settled_candles.assert_awaited_once()
    refresh = backfiller.refresh_settled_candles.await_args
    assert refresh.args[0] == "EURUSD"
    assert timedelta(hours=23, minutes=59) <= refresh.args[2] - refresh.args[1] <= timedelta(
        hours=24, minutes=1
    )
    backfiller.on_demand_ticks.assert_awaited_once()
