"""
FastAPI application factory.

Creates and configures the ASGI application with:
- REST routers (candles, ticks, symbols, health)
- WebSocket routers (real-time ticks, candles)
- Startup / shutdown lifecycle hooks (DB pool, etc.)
- CORS middleware
- OpenAPI metadata
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import candles, coverage, custom_candles, health, symbols, ticks
from src.api.websocket import streams
from src.config import get_settings
from src.db.engine import dispose_engine, get_engine
from src.db.init_timescale import init_timescaledb
from src.logging_config import setup_logging
from src.redis_bus.backfill_manager import BackfillRequester
from src.redis_bus.pool import close_redis_pool

logger = structlog.get_logger(__name__)

# Weak reference to the app — set during lifespan, used by get_backfill_requester()
_app_ref: FastAPI | None = None


def get_backfill_requester() -> BackfillRequester | None:
    """Return the BackfillRequester stored on app.state (available after startup)."""
    if _app_ref is not None:
        return getattr(_app_ref.state, "backfill_requester", None)
    return None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Application lifecycle: startup → yield → shutdown."""
    global _app_ref
    _app_ref = app
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)
    logger.info("api_starting", version="1.0.0")

    # Warm up the DB connection pool
    get_engine(settings)

    # Ensure TimescaleDB schema exists
    try:
        await init_timescaledb()
    except Exception:
        logger.warning("timescaledb_init_skipped", exc_info=True)

    # Connect backfill requester (for on-demand MT5 downloads)
    try:
        requester = BackfillRequester(settings)
        await requester.connect()
        app.state.backfill_requester = requester
    except Exception:
        logger.warning("backfill_requester_connect_failed", exc_info=True)
        app.state.backfill_requester = None

    yield

    # Shutdown
    requester = getattr(app.state, "backfill_requester", None)
    if requester is not None:
        await requester.close()
        app.state.backfill_requester = None
    await close_redis_pool()
    await dispose_engine()
    _app_ref = None
    logger.info("api_stopped")


def create_app() -> FastAPI:
    """Build and return the FastAPI ASGI application."""

    app = FastAPI(
        title="MT5 Connector API",
        description=(
            "Production-grade REST + WebSocket API for MetaTrader 5 "
            "historical and real-time market data."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=_lifespan,
    )

    # ---- CORS ----
    # When CORS_ORIGINS env var is set, restrict to those origins;
    # otherwise allow all (development mode, no credentials).
    settings = get_settings()
    cors_origins_raw = settings.cors_origins
    if cors_origins_raw == "*":
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # ---- REST routes ----
    app.include_router(custom_candles.router)  # must precede candles (path overlap)
    app.include_router(candles.router)
    app.include_router(ticks.router)
    app.include_router(symbols.router)
    app.include_router(health.router)
    app.include_router(coverage.router)

    # ---- WebSocket routes ----
    app.include_router(streams.router)

    return app
