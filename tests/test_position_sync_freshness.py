from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import orjson
import pytest
from fastapi import HTTPException

from src.api.routes.trading import get_position_sync_status

# The production Trader runs on Windows. These policy/loop tests run on macOS
# and do not exercise Win32 terminal window management.
sys.modules.setdefault(
    "src.mt5.portable",
    SimpleNamespace(ensure_portable_terminal=lambda *args, **kwargs: None),
)

from src.trader_main import _publish_position_sync_status, _sync_positions


@pytest.mark.asyncio
async def test_position_sync_publishes_complete_snapshot_heartbeat() -> None:
    redis = SimpleNamespace(set=AsyncMock())
    session = SimpleNamespace(account_id=68, login=8042325)

    with patch("src.trader_main.get_redis_pool", return_value=redis):
        await _publish_position_sync_status(
            session,
            positions=[{"ticket": 12}, {"ticket": 10}],
            started_at=datetime(2026, 8, 28, 12, 5, tzinfo=timezone.utc),
            interval_sec=1.0,
        )

    redis.set.assert_awaited_once()
    key, raw = redis.set.await_args.args
    payload = orjson.loads(raw)
    assert key == "trader:position_sync:68"
    assert redis.set.await_args.kwargs["ex"] == 120
    assert payload["status"] == "ok"
    assert payload["position_count"] == 2
    assert payload["tickets"] == [10, 12]
    assert payload["interval_sec"] == 1.0
    assert payload["last_success_at"]


@pytest.mark.asyncio
async def test_position_loop_publishes_only_after_database_snapshot() -> None:
    class Session:
        account_id = 68
        login = 8042325
        calls = 0

        async def get_positions(self):
            self.calls += 1
            if self.calls == 1:
                return [{"ticket": 10}]
            raise asyncio.CancelledError

    session = Session()
    with (
        patch("src.trader_main.repo.sync_positions", new=AsyncMock()) as sync,
        patch(
            "src.trader_main._publish_position_sync_status",
            new=AsyncMock(),
        ) as publish,
    ):
        await _sync_positions(session, interval_sec=0)

    sync.assert_awaited_once_with(68, [{"ticket": 10}])
    publish.assert_awaited_once()
    assert publish.await_args.kwargs["positions"] == [{"ticket": 10}]
    assert publish.await_args.kwargs["interval_sec"] == 0


@pytest.mark.asyncio
async def test_position_status_endpoint_returns_trader_heartbeat() -> None:
    payload = {
        "account_id": 68,
        "status": "ok",
        "position_count": 2,
        "last_success_at": "2026-08-28T12:05:01+00:00",
    }
    redis = SimpleNamespace(get=AsyncMock(return_value=orjson.dumps(payload)))

    with patch("src.api.routes.trading.get_redis_pool", return_value=redis):
        result = await get_position_sync_status(68)

    assert result == payload
    redis.get.assert_awaited_once_with("trader:position_sync:68")


@pytest.mark.asyncio
async def test_position_status_endpoint_fails_closed_without_heartbeat() -> None:
    redis = SimpleNamespace(get=AsyncMock(return_value=None))

    with (
        patch("src.api.routes.trading.get_redis_pool", return_value=redis),
        pytest.raises(HTTPException) as error,
    ):
        await get_position_sync_status(68)

    assert error.value.status_code == 503
