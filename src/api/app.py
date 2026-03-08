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

logger = structlog.get_logger(__name__)

# Module-level singleton so route handlers can import it
backfill_requester: BackfillRequester | None = None


def get_backfill_requester() -> BackfillRequester | None:
    """Return the global BackfillRequester (available after startup)."""
    return backfill_requester


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Application lifecycle: startup → yield → shutdown."""
    global backfill_requester
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
        backfill_requester = BackfillRequester(settings)
        await backfill_requester.connect()
    except Exception:
        logger.warning("backfill_requester_connect_failed", exc_info=True)
        backfill_requester = None

    yield

    # Shutdown
    if backfill_requester is not None:
        await backfill_requester.close()
        backfill_requester = None
    await dispose_engine()
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
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
