from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response

from src.api.routes.trade_commands import create_trade_command
from src.api.schemas import TradeCommandCreate


@pytest.mark.asyncio
async def test_lost_reply_retries_return_existing_command_after_its_expiry():
    expiry = datetime.now(UTC) - timedelta(minutes=1)
    body = TradeCommandCreate(
        command_id=uuid4(), account_id=68, action="close_position",
        position_ticket=1, expected_position_identifier=1, expected_symbol="TEST",
        expected_type=0, expected_magic=7, max_volume=1,
        reason="TEST_ONLY", expires_at=expiry,
    )
    existing = {**body.model_dump(), "expires_at": expiry, "status": "confirmed"}
    response = Response()
    with (
        patch("src.api.routes.trade_commands.get_settings", return_value=SimpleNamespace(
            trading_execution_enabled=True, trading_account_allowlist={68}
        )),
        patch("src.api.routes.trade_commands.repo.get_trade_command", new=AsyncMock(return_value=existing)),
        patch("src.api.routes.trade_commands.repo.create_trade_command", new=AsyncMock()) as insert,
    ):
        assert await create_trade_command(body, response, "isolated_test") is existing
        assert response.status_code == 200
        insert.assert_not_awaited()
        modified = body.model_copy(update={"expires_at": datetime.now(UTC)+timedelta(minutes=1)})
        with pytest.raises(HTTPException) as raised:
            await create_trade_command(modified, Response(), "isolated_test")
        assert raised.value.status_code == 409
