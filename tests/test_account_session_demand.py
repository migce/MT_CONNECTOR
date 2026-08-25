from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from src.api.auth import require_internal_api
from src.api.routes.account_sessions import router
from src.api.schemas import AccountSessionDemandRequest


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_internal_api] = lambda: "test-monitor"
    return app


def test_complete_desired_set_is_applied_and_trader_is_notified() -> None:
    applied_at = datetime(2026, 8, 13, 22, 30, tzinfo=timezone.utc)
    result = {
        "applied": True,
        "changed": True,
        "stale": False,
        "account_ids": [67, 68, 84],
        "effective_account_ids": [67, 68, 84],
        "source_updated_at": applied_at,
        "snapshot_id": "monitor-1",
    }
    with (
        patch(
            "src.api.routes.account_sessions.repo.reconcile_account_session_demand",
            AsyncMock(return_value=result),
        ) as reconcile_mock,
        patch(
            "src.api.routes.account_sessions._notify_trader_reload",
            AsyncMock(),
        ) as notify_mock,
        TestClient(_app()) as client,
    ):
        response = client.put(
            "/api/v1/internal/account-sessions/desired",
            json={
                "account_ids": [84, 68, 67, 68],
                "source_updated_at": applied_at.isoformat(),
                "snapshot_id": "monitor-1",
            },
        )

    assert response.status_code == 200
    assert response.json()["effective_account_ids"] == [67, 68, 84]
    reconcile_mock.assert_awaited_once_with(
        account_ids=[84, 68, 67, 68],
        source_updated_at=applied_at,
        snapshot_id="monitor-1",
    )
    notify_mock.assert_awaited_once()


def test_unchanged_desired_set_refreshes_snapshot_without_trader_reload() -> None:
    applied_at = datetime(2026, 8, 13, 22, 31, tzinfo=timezone.utc)
    result = {
        "applied": True,
        "changed": False,
        "stale": False,
        "account_ids": [67, 68, 84],
        "effective_account_ids": [67, 68, 84],
        "source_updated_at": applied_at,
        "snapshot_id": "monitor-2",
    }
    with (
        patch(
            "src.api.routes.account_sessions.repo.reconcile_account_session_demand",
            AsyncMock(return_value=result),
        ),
        patch(
            "src.api.routes.account_sessions._notify_trader_reload",
            AsyncMock(),
        ) as notify_mock,
        TestClient(_app()) as client,
    ):
        response = client.put(
            "/api/v1/internal/account-sessions/desired",
            json={
                "account_ids": [67, 68, 84],
                "source_updated_at": applied_at.isoformat(),
                "snapshot_id": "monitor-2",
            },
        )

    assert response.status_code == 200
    assert response.json()["changed"] is False
    notify_mock.assert_not_awaited()


def test_unknown_account_rejects_whole_snapshot_without_reload() -> None:
    with (
        patch(
            "src.api.routes.account_sessions.repo.reconcile_account_session_demand",
            AsyncMock(side_effect=ValueError("Unknown trading account IDs: [999]")),
        ),
        patch(
            "src.api.routes.account_sessions._notify_trader_reload",
            AsyncMock(),
        ) as notify_mock,
        TestClient(_app()) as client,
    ):
        response = client.put(
            "/api/v1/internal/account-sessions/desired",
            json={
                "account_ids": [67, 999],
                "source_updated_at": "2026-08-13T22:30:00Z",
                "snapshot_id": "monitor-bad",
            },
        )

    assert response.status_code == 409
    notify_mock.assert_not_awaited()


def test_desired_set_timestamp_must_be_timezone_aware() -> None:
    try:
        AccountSessionDemandRequest(
            account_ids=[67],
            source_updated_at=datetime(2026, 8, 13, 22, 30),
            snapshot_id="naive",
        )
    except ValidationError as exc:
        assert "must include a timezone" in str(exc)
    else:
        raise AssertionError("Naive source timestamp was accepted")
