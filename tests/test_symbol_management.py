"""Focused contracts for Connector-owned Symbol Management."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.api.routes.symbol_management import (
    BackfillJobCreate,
    CustomTimeframeBinding,
    ManagedSymbolUpdate,
    bind_timeframe,
    create_job,
    update_symbol,
)
from src.redis_bus.backfill_manager import BackfillListener


@pytest.mark.asyncio
async def test_cannot_enable_symbol_outside_broker_catalog() -> None:
    with (
        patch(
            "src.api.routes.symbol_management.validate_symbol",
            return_value="NOTREAL",
        ),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        pytest.raises(HTTPException) as exc_info,
    ):
        await update_symbol("NOTREAL", ManagedSymbolUpdate(active=True))

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_tick_timeframes_cannot_be_materialized() -> None:
    with (
        patch(
            "src.api.routes.symbol_management.validate_symbol",
            return_value="EURUSD",
        ),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        pytest.raises(HTTPException) as exc_info,
    ):
        await bind_timeframe(
            "EURUSD",
            "T500",
            CustomTimeframeBinding(mode="materialized", enabled=True),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Tick bars are virtual-only"


@pytest.mark.asyncio
async def test_job_create_enqueues_persistent_job() -> None:
    now = datetime.now(UTC)
    stored = {
        "id": "job-1",
        "symbol": "EURUSD",
        "target_type": "candles",
        "timeframe": "M1",
        "source_type": "candles",
        "source_timeframe": "M1",
        "mode": "fill_missing",
        "range_from": now - timedelta(days=1),
        "range_to": now - timedelta(hours=1),
    }
    requester = MagicMock()
    requester.enqueue_job = AsyncMock(return_value="request-1")

    with (
        patch(
            "src.api.routes.symbol_management.validate_symbol",
            return_value="EURUSD",
        ),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        patch(
            "src.api.routes.symbol_management.sm.create_job",
            new=AsyncMock(return_value=(stored, True)),
        ),
        patch(
            "src.api.app.get_backfill_requester",
            return_value=requester,
        ),
    ):
        result = await create_job(BackfillJobCreate(
            symbol="EURUSD",
            target_type="candles",
            timeframe="M1",
            mode="fill_missing",
            **{"from": stored["range_from"], "to": stored["range_to"]},
        ))

    assert result["deduplicated"] is False
    requester.enqueue_job.assert_awaited_once_with(stored)


@pytest.mark.asyncio
async def test_listener_marks_short_broker_result_partial() -> None:
    now = datetime.now(UTC)
    backfiller = MagicMock()

    async def short_result(*_args, progress_callback=None, **_kwargs):
        assert progress_callback is not None
        await progress_callback(now - timedelta(hours=12), 120)
        return 120

    backfiller.on_demand_candles = AsyncMock(side_effect=short_result)
    listener = BackfillListener(backfiller, settings=MagicMock())
    listener._redis = MagicMock()
    listener._redis.delete = AsyncMock()
    listener._redis.publish = AsyncMock()
    updates: list[dict] = []

    async def update_job(_job_id, **changes):
        updates.append(changes)
        return changes

    with (
        patch(
            "src.db.symbol_management.get_job",
            new=AsyncMock(return_value={"id": "job-1", "status": "queued"}),
        ),
        patch(
            "src.db.symbol_management.update_job",
            new=update_job,
        ),
    ):
        await listener._handle_request({
            "request_id": "request-1",
            "job_id": "job-1",
            "symbol": "EURUSD",
            "data_type": "candles",
            "timeframe": "H1",
            "target_type": "candles",
            "target_timeframe": "H1",
            "mode": "fill_missing",
            "from": (now - timedelta(days=1)).isoformat(),
            "to": now.isoformat(),
        })

    terminal = updates[-1]
    assert terminal["status"] == "partial"
    assert terminal["covered_to"] == now - timedelta(hours=12)
    assert terminal["error"] == "Broker returned only part of the requested range"


@pytest.mark.asyncio
async def test_listener_ignores_duplicate_nonqueued_job() -> None:
    listener = BackfillListener(MagicMock(), settings=MagicMock())
    listener._redis = MagicMock()
    with patch(
        "src.db.symbol_management.get_job",
        new=AsyncMock(return_value={"id": "job-1", "status": "running"}),
    ):
        await listener._handle_request({
            "request_id": "request-duplicate",
            "job_id": "job-1",
            "symbol": "EURUSD",
            "data_type": "candles",
            "timeframe": "M1",
            "from": datetime.now(UTC).isoformat(),
            "to": datetime.now(UTC).isoformat(),
        })

    listener._backfiller.on_demand_candles.assert_not_called()
