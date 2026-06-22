"""
Monitor API — FastAPI application factory.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.db.engine import dispose_engine, get_engine


@asynccontextmanager
async def _lifespan(app: FastAPI):
    settings = get_settings()
    # warm up DB pool
    await get_engine(settings.database_url)
    yield
    await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="MT5 Monitor",
        description="Administration & monitoring dashboard for MT5 Connector",
        version="0.1.0",
        lifespan=_lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routes ───────────────────────────────────────────────────────
    from app.auth.routes import router as auth_router
    from app.users.routes import router as users_router
    from app.proxy.market import router as market_router
    from app.proxy.trading import router as trading_router
    from app.proxy.system import router as system_router
    from app.proxy.websocket import router as ws_router

    app.include_router(auth_router, prefix="/api/auth", tags=["auth"])
    app.include_router(users_router, prefix="/api/admin/users", tags=["admin"])
    app.include_router(market_router, prefix="/api/market", tags=["market"])
    app.include_router(trading_router, prefix="/api/trading", tags=["trading"])
    app.include_router(system_router, prefix="/api/system", tags=["system"])
    app.include_router(ws_router)

    return app
