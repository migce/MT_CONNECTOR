"""
Poller Metrics — thread-safe singleton for live dashboard.

Collects counters, rates, timestamps, and error counts from all poller
subsystems.  The ``Dashboard`` reads this object every refresh cycle.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class SymbolTickInfo:
    """Latest tick data for a single symbol."""
    bid: float = 0.0
    ask: float = 0.0
    last_tick_ts: float = 0.0  # monotonic
    count: int = 0


@dataclass
class OnDemandEntry:
    """One on-demand backfill request log entry."""
    ts: datetime
    symbol: str
    data_type: str
    timeframe: str | None
    rows: int
    status: str  # "ok" | "error"
    elapsed_sec: float


class PollerMetrics:
    """
    Central metrics store — singleton accessed by all subsystems.

    All public methods are thread-safe (collector runs in async loops,
    but MT5 calls happen in a thread-pool executor).
    """

    _instance: "PollerMetrics | None" = None
    _lock_cls = threading.Lock()

    def __new__(cls) -> "PollerMetrics":
        with cls._lock_cls:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init()
            return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Destroy the singleton so the next call to ``PollerMetrics()`` re-initialises.

        Primarily intended for tests.
        """
        with cls._lock_cls:
            cls._instance = None

    # -- initialisation (called once) ------------------------------------

    def _init(self) -> None:
        self._lock = threading.Lock()

        # Global counters
        self.ticks_total: int = 0
        self.candles_total: int = 0
        self.ticks_flushed_total: int = 0
        self.redis_pub_count: int = 0

        # Per-symbol tick info
        self.symbol_ticks: dict[str, SymbolTickInfo] = defaultdict(SymbolTickInfo)

        # Tick rate — sliding window (timestamps)
        self._tick_ts_window: deque[float] = deque(maxlen=5000)
        self.peak_ticks_sec: float = 0.0

        # Tick buffer depth (set by collector each flush)
        self.tick_buffer_depth: int = 0

        # Flush stats
        self.flush_count: int = 0
        self.flush_total_ms: float = 0.0
        self.last_flush_ms: float = 0.0

        # Error counters
        self.errors: dict[str, int] = defaultdict(int)
        # categories: tick_loop, candle_loop, flush, publish, heartbeat, gap_scan, backfill

        # Connection
        self.reconnect_count: int = 0
        self.mt5_connected: bool = False

        # Tasks alive
        self.task_alive: dict[str, bool] = {}

        # Gap scan
        self.last_gap_scan_time: datetime | None = None
        self.gaps_found: int = 0

        # On-demand backfill log (last 20)
        self.on_demand_log: deque[OnDemandEntry] = deque(maxlen=20)

        # Backfill progress
        self.backfill_phase: str = ""  # "" | "initial" | "on_demand"
        self.backfill_current: str = ""  # e.g. "EURUSD M1"

        # Startup
        self.start_time: float = time.monotonic()
        self.poller_started_at: datetime = datetime.now(timezone.utc)

        # ── Minute-bucket counters for time-window stats ────────────
        self._tick_minute_buckets: Counter[int] = Counter()
        self._candle_minute_buckets: Counter[int] = Counter()

        # ── API health (populated by health-checker task) ───────────
        self.api_healthy: bool = False
        self.api_latency_ms: float = 0.0
        self.api_requests_1h: int = 0
        self.api_requests_12h: int = 0
        self.api_requests_24h: int = 0
        self.api_errors_1h: int = 0
        self.api_avg_latency_ms: float = 0.0

        # ── Infrastructure health ───────────────────────────────────
        self.db_healthy: bool = False
        self.redis_healthy: bool = False
        self.db_latency_ms: float = 0.0
        self.redis_latency_ms: float = 0.0

    # -- tick metrics ----------------------------------------------------

    @staticmethod
    def _current_minute() -> int:
        return int(time.time()) // 60

    def record_tick(
        self, symbol: str, bid: float, ask: float,
    ) -> None:
        now = time.monotonic()
        minute = self._current_minute()
        with self._lock:
            self.ticks_total += 1
            info = self.symbol_ticks[symbol]
            info.bid = bid
            info.ask = ask
            info.last_tick_ts = now
            info.count += 1
            self._tick_ts_window.append(now)
            self._tick_minute_buckets[minute] += 1

    def record_ticks_flushed(self, count: int, elapsed_ms: float) -> None:
        with self._lock:
            self.ticks_flushed_total += max(count, 0)
            self.flush_count += 1
            self.flush_total_ms += elapsed_ms
            self.last_flush_ms = elapsed_ms

    def set_tick_buffer_depth(self, depth: int) -> None:
        with self._lock:
            self.tick_buffer_depth = depth

    # -- candle metrics --------------------------------------------------

    def record_candle_upsert(self, count: int = 1) -> None:
        minute = self._current_minute()
        with self._lock:
            self.candles_total += count
            self._candle_minute_buckets[minute] += count

    # -- redis metrics ---------------------------------------------------

    def record_redis_publish(self) -> None:
        with self._lock:
            self.redis_pub_count += 1

    # -- error metrics ---------------------------------------------------

    def record_error(self, category: str) -> None:
        with self._lock:
            self.errors[category] += 1

    # -- connection metrics ----------------------------------------------

    def record_reconnect(self) -> None:
        with self._lock:
            self.reconnect_count += 1

    def set_mt5_connected(self, val: bool) -> None:
        with self._lock:
            self.mt5_connected = val

    # -- task alive ------------------------------------------------------

    def set_task_alive(self, name: str, alive: bool) -> None:
        with self._lock:
            self.task_alive[name] = alive

    # -- gap scan --------------------------------------------------------

    def record_gap_scan(self, gaps: int) -> None:
        with self._lock:
            self.last_gap_scan_time = datetime.now(timezone.utc)
            self.gaps_found = gaps

    # -- on-demand backfill log ------------------------------------------

    def record_on_demand(
        self,
        symbol: str,
        data_type: str,
        timeframe: str | None,
        rows: int,
        status: str,
        elapsed_sec: float,
    ) -> None:
        entry = OnDemandEntry(
            ts=datetime.now(timezone.utc),
            symbol=symbol,
            data_type=data_type,
            timeframe=timeframe,
            rows=rows,
            status=status,
            elapsed_sec=elapsed_sec,
        )
        with self._lock:
            self.on_demand_log.append(entry)

    # -- backfill progress -----------------------------------------------

    def set_backfill_phase(self, phase: str, current: str = "") -> None:
        with self._lock:
            self.backfill_phase = phase
            self.backfill_current = current

    # -- computed / snapshot helpers --------------------------------------

    def ticks_per_sec(self) -> float:
        """Average ticks/sec over the last 10 seconds."""
        now = time.monotonic()
        cutoff = now - 10.0
        with self._lock:
            # Count timestamps in window > cutoff
            count = sum(1 for ts in self._tick_ts_window if ts > cutoff)
        return count / 10.0

    def update_peak_rate(self) -> float:
        rate = self.ticks_per_sec()
        if rate > self.peak_ticks_sec:
            self.peak_ticks_sec = rate
        return rate

    def avg_flush_ms(self) -> float:
        if self.flush_count == 0:
            return 0.0
        return self.flush_total_ms / self.flush_count

    def uptime_str(self) -> str:
        elapsed = time.monotonic() - self.start_time
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def stale_symbols(self, threshold_sec: float = 30.0) -> list[str]:
        """Return symbols that haven't received a tick in *threshold_sec*."""
        now = time.monotonic()
        result = []
        with self._lock:
            for sym, info in self.symbol_ticks.items():
                if info.last_tick_ts > 0 and (now - info.last_tick_ts) > threshold_sec:
                    result.append(sym)
        return result

    def total_errors(self) -> int:
        with self._lock:
            return sum(self.errors.values())

    # -- time-window helpers ---------------------------------------------

    def ticks_in_window(self, seconds: float) -> int:
        """Count ticks received within the last *seconds* seconds."""
        minutes = max(int(seconds) // 60, 1)
        cutoff = self._current_minute() - minutes
        with self._lock:
            return sum(c for m, c in self._tick_minute_buckets.items() if m >= cutoff)

    def candles_in_window(self, seconds: float) -> int:
        """Count candles upserted within the last *seconds* seconds."""
        minutes = max(int(seconds) // 60, 1)
        cutoff = self._current_minute() - minutes
        with self._lock:
            return sum(c for m, c in self._candle_minute_buckets.items() if m >= cutoff)

    def prune_minute_buckets(self) -> None:
        """Remove minute buckets older than 7 days + 1 h buffer."""
        cutoff = self._current_minute() - (7 * 24 * 60 + 60)
        with self._lock:
            for bucket in [self._tick_minute_buckets, self._candle_minute_buckets]:
                stale = [m for m in bucket if m < cutoff]
                for m in stale:
                    del bucket[m]

    # -- minute-bucket persistence (save/restore via Redis) --------------

    def export_minute_buckets(self) -> dict[str, Any]:
        """Return serialisable snapshot of minute-bucket counters + peak rate."""
        with self._lock:
            return {
                "ticks": {str(k): v for k, v in self._tick_minute_buckets.items()},
                "candles": {str(k): v for k, v in self._candle_minute_buckets.items()},
                "peak_ticks_sec": self.peak_ticks_sec,
            }

    def import_minute_buckets(self, data: dict[str, Any]) -> int:
        """Restore minute-bucket counters from a previously exported snapshot.

        Returns total number of bucket entries restored.
        """
        cutoff = self._current_minute() - (7 * 24 * 60 + 60)
        count = 0
        with self._lock:
            for key_str, val in (data.get("ticks") or {}).items():
                m = int(key_str)
                if m >= cutoff:
                    self._tick_minute_buckets[m] += val
                    count += 1
            for key_str, val in (data.get("candles") or {}).items():
                m = int(key_str)
                if m >= cutoff:
                    self._candle_minute_buckets[m] += val
                    count += 1
            # Restore peak tick rate
            saved_peak = data.get("peak_ticks_sec", 0.0)
            if saved_peak > self.peak_ticks_sec:
                self.peak_ticks_sec = saved_peak
        return count

    # -- health-checker updates ------------------------------------------

    def update_api_health(
        self,
        healthy: bool,
        latency_ms: float,
        requests_1h: int,
        requests_12h: int,
        requests_24h: int,
        errors_1h: int,
        avg_latency_ms: float,
    ) -> None:
        with self._lock:
            self.api_healthy = healthy
            self.api_latency_ms = latency_ms
            self.api_requests_1h = requests_1h
            self.api_requests_12h = requests_12h
            self.api_requests_24h = requests_24h
            self.api_errors_1h = errors_1h
            self.api_avg_latency_ms = avg_latency_ms

    def update_infra_health(
        self,
        db_ok: bool,
        redis_ok: bool,
        db_latency_ms: float = 0.0,
        redis_latency_ms: float = 0.0,
    ) -> None:
        with self._lock:
            self.db_healthy = db_ok
            self.redis_healthy = redis_ok
            self.db_latency_ms = db_latency_ms
            self.redis_latency_ms = redis_latency_ms
