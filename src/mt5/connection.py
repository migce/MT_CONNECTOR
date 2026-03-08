"""
MT5 Connection Manager.

Handles MetaTrader 5 terminal initialization, login, heartbeat monitoring,
and automatic reconnection with exponential backoff.

**All** MT5 API calls MUST go through the single-threaded executor
exposed by this module, because the MetaTrader5 package is NOT thread-safe.
"""

from __future__ import annotations

import asyncio
import functools
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

import structlog

from src.config import Settings, get_settings
from src.metrics import PollerMetrics

logger = structlog.get_logger(__name__)

# Single-thread executor — all MT5 calls are serialised here.
_mt5_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mt5")

T = TypeVar("T")


async def run_in_mt5(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Schedule *func* in the dedicated MT5 thread and await the result."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _mt5_executor,
        functools.partial(func, *args, **kwargs),
    )


class MT5Connection:
    """
    Manages the lifecycle of a MetaTrader 5 terminal connection.

    Usage::

        conn = MT5Connection(settings)
        await conn.connect()          # blocks until connected
        await conn.ensure_connected() # heartbeat check + auto-reconnect
        await conn.shutdown()          # clean close
    """

    # Backoff parameters
    BACKOFF_BASE = 1.0
    BACKOFF_MAX = 60.0
    BACKOFF_FACTOR = 2.0

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._connected = False
        self._backoff = self.BACKOFF_BASE
        self._metrics = PollerMetrics()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """
        Initialize the MT5 terminal and log in.

        Retries with exponential backoff until successful.
        """
        while True:
            ok = await run_in_mt5(self._try_connect)
            if ok:
                self._connected = True
                self._metrics.set_mt5_connected(True)
                self._backoff = self.BACKOFF_BASE
                logger.info(
                    "mt5_connected",
                    login=self._settings.mt5_login,
                    server=self._settings.mt5_server,
                )
                return True

            error = await run_in_mt5(self._last_error)
            logger.warning(
                "mt5_connect_failed",
                backoff=self._backoff,
                error=error,
            )
            await asyncio.sleep(self._backoff)
            self._backoff = min(self._backoff * self.BACKOFF_FACTOR, self.BACKOFF_MAX)

    async def ensure_connected(self) -> bool:
        """
        Check terminal liveness.  If the connection is lost, reconnect and
        return ``True`` (indicating a gap that should be backfilled).
        """
        info = await run_in_mt5(self._terminal_info)
        if info is not None:
            return False  # still connected, no gap

        logger.warning("mt5_connection_lost")
        self._connected = False
        self._metrics.set_mt5_connected(False)
        await self.connect()
        self._metrics.record_reconnect()
        return True  # reconnected — caller should backfill

    async def select_symbols(self, symbols: list[str]) -> None:
        """Enable symbols in MarketWatch so they can be queried."""
        for sym in symbols:
            ok = await run_in_mt5(self._symbol_select, sym)
            if not ok:
                logger.warning("mt5_symbol_select_failed", symbol=sym)
            else:
                logger.debug("mt5_symbol_selected", symbol=sym)

    async def shutdown(self) -> None:
        """Cleanly close the MT5 connection."""
        await run_in_mt5(self._shutdown)
        self._connected = False
        self._metrics.set_mt5_connected(False)
        logger.info("mt5_shutdown")

    @property
    def connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Private — these run inside _mt5_executor (single thread)
    # ------------------------------------------------------------------

    def _try_connect(self) -> bool:
        import MetaTrader5 as mt5

        s = self._settings
        if not mt5.initialize(
            path=s.mt5_path,
            login=s.mt5_login,
            password=s.mt5_password,
            server=s.mt5_server,
            timeout=s.mt5_timeout,
        ):
            return False

        if not mt5.login(
            login=s.mt5_login,
            password=s.mt5_password,
            server=s.mt5_server,
            timeout=s.mt5_timeout,
        ):
            return False

        return True

    @staticmethod
    def _terminal_info():
        import MetaTrader5 as mt5
        return mt5.terminal_info()

    @staticmethod
    def _last_error():
        import MetaTrader5 as mt5
        return mt5.last_error()

    @staticmethod
    def _symbol_select(symbol: str) -> bool:
        import MetaTrader5 as mt5
        return mt5.symbol_select(symbol, True)

    @staticmethod
    def _shutdown():
        import MetaTrader5 as mt5
        mt5.shutdown()
