from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes import trade_commands


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(trade_commands.router)
    return app


def test_readiness_requires_the_internal_bearer_token() -> None:
    settings = SimpleNamespace(
        internal_api_token="secret",
        trading_execution_enabled=True,
        trading_account_allowlist={68},
    )
    with patch("src.api.auth.get_settings", return_value=settings), patch(
        "src.api.routes.trade_commands.get_settings", return_value=settings
    ):
        client = TestClient(_app())
        assert client.get("/api/v1/trade-commands/readiness").status_code == 401
        response = client.get(
            "/api/v1/trade-commands/readiness",
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "execution_enabled": True,
        "account_allowlist": [68],
        "mode": "close_only",
    }


def test_broker_event_route_is_present_in_openapi() -> None:
    paths = _app().openapi()["paths"]
    assert "/api/v1/broker-events" in paths
    assert "/api/v1/trade-commands" in paths
    assert "/api/v1/trade-commands/readiness" in paths
