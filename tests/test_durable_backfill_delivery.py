import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.redis_bus.backfill_manager import BackfillListener, durable_job_request


@pytest.mark.asyncio
async def test_durable_job_runs_even_when_redis_hint_is_missing():
    now = datetime.now(UTC)
    job = {"id": "job-1", "symbol": "EURUSD", "source_type": "ticks", "target_type": "ticks",
           "mode": "fill_missing", "range_from": now, "range_to": now}
    listener = BackfillListener(MagicMock(), settings=MagicMock())
    listener._redis = MagicMock()
    listener._redis.blpop = AsyncMock(side_effect=AssertionError("Must use durable queue"))
    listener._handle_request = AsyncMock()
    with patch("src.db.symbol_management.queued_jobs", new=AsyncMock(
        side_effect=[[job], asyncio.CancelledError()]
    )):
        await listener.run_forever()
    listener._handle_request.assert_awaited_once()
    request = listener._handle_request.call_args.args[0]
    assert request["job_id"] == "job-1"
    assert request["from"] == job["range_from"].isoformat()


@pytest.mark.asyncio
async def test_duplicate_durable_hint_cannot_start_work():
    now = datetime.now(UTC)
    job = {"id": "job-1", "symbol": "EURUSD", "source_type": "ticks", "target_type": "ticks",
           "mode": "fill_missing", "range_from": now, "range_to": now}
    backfiller = MagicMock()
    backfiller.on_demand_ticks = AsyncMock()
    listener = BackfillListener(backfiller, settings=MagicMock())
    with patch("src.db.symbol_management.claim_queued_job", new=AsyncMock(return_value=None)):
        await listener._handle_request(durable_job_request(job))
    backfiller.on_demand_ticks.assert_not_awaited()
