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

    async def get_all_symbols(self) -> list[dict[str, str]]:
        """Fetch all symbols available on the broker's server.

        Returns a list of ``{"name": ..., "description": ...}`` dicts.
        """
        raw = await run_in_mt5(self._symbols_get)
        if raw is None:
            logger.warning("mt5_symbols_get_failed")
            return []
        results = []
        for info in raw:
            results.append({
                "name": info.name,
                "description": getattr(info, "description", ""),
            })
        logger.info("mt5_symbols_fetched", count=len(results))
        return results

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
        from src.mt5.portable import minimize_terminal_window, prepare_terminal

        s = self._settings
        # Write chart-less terminal.ini before mt5.initialize launches it
        prepare_terminal(s.mt5_path)
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

        # Terminal is connected — minimize its window
        minimize_terminal_window(s.mt5_path)
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
    def _symbol_info(symbol: str):
        import MetaTrader5 as mt5
        return mt5.symbol_info(symbol)

    @staticmethod
    def _symbols_get():
        import MetaTrader5 as mt5
        return mt5.symbols_get()

    @staticmethod
    def _shutdown():
        import MetaTrader5 as mt5
        mt5.shutdown()


# ------------------------------------------------------------------
# Symbol digits cache
# ------------------------------------------------------------------

_symbol_digits: dict[str, int] = {}


async def fetch_symbol_digits(symbols: list[str]) -> dict[str, int]:
    """Fetch and cache the number of price decimal digits for each symbol.

    Also persists the mapping to Redis so the API process can read it.
    """
    for symbol in symbols:
        info = await run_in_mt5(MT5Connection._symbol_info, symbol)
        if info is not None:
            _symbol_digits[symbol] = info.digits
            logger.debug("symbol_digits_cached", symbol=symbol, digits=info.digits)
        else:
            logger.warning("symbol_digits_unavailable", symbol=symbol)

    # Persist to Redis for the API process
    if _symbol_digits:
        try:
            from src.redis_bus.pool import get_redis_pool
            import orjson
            redis = get_redis_pool()
            await redis.set(
                "meta:symbol_digits",
                orjson.dumps(_symbol_digits),
            )
            logger.info("symbol_digits_published_to_redis", count=len(_symbol_digits))
        except Exception:
            logger.warning("symbol_digits_redis_publish_failed", exc_info=True)

    return _symbol_digits


def get_digits(symbol: str) -> int:
    """Return cached price digits for *symbol* (default 5)."""
    return _symbol_digits.get(symbol, 5)


# ------------------------------------------------------------------
# Publish full MT5 symbol catalogue to Redis
# ------------------------------------------------------------------

async def publish_mt5_symbols(connection: MT5Connection) -> int:
    """Fetch all symbols from MT5 and store in Redis for the API process.

    Returns the number of symbols published.
    """
    all_symbols = await connection.get_all_symbols()
    if not all_symbols:
        logger.warning("mt5_no_symbols_to_publish")
        return 0

    try:
        import orjson
        from src.redis_bus.pool import get_redis_pool
        redis = get_redis_pool()
        await redis.set("meta:mt5_symbols", orjson.dumps(all_symbols))
        logger.info("mt5_symbols_published_to_redis", count=len(all_symbols))
    except Exception:
        logger.warning("mt5_symbols_redis_publish_failed", exc_info=True)
    return len(all_symbols)
