from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.mt5 import backfill
from src.redis_bus.backfill_manager import BackfillListener


@pytest.mark.asyncio
@pytest.mark.parametrize("fatal", [False, True])
async def test_listener_preserves_native_history_timeout_callback(monkeypatch, fatal):
    class NativeTimeout(TimeoutError):
        pass

    monkeypatch.setattr(backfill, "MT5HistoryCallTimeoutError", NativeTimeout, raising=False)
    worker = MagicMock()
    worker.on_demand_ticks = AsyncMock(side_effect=NativeTimeout() if fatal else TimeoutError())
    callback = MagicMock()
    listener = BackfillListener(
        worker, settings=MagicMock(backfill_job_timeout_sec=30), fatal_history_timeout=callback,
    )
    listener._redis = AsyncMock()
    now = datetime.now(UTC)
    await listener._handle_request({
        "request_id": "fixture", "symbol": "TEST", "data_type": "ticks",
        "from": (now - timedelta(hours=1)).isoformat(), "to": now.isoformat(),
    })
    assert callback.call_count == int(fatal)
    listener._redis.set.assert_awaited()
