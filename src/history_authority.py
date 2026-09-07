"""Fail-closed admission for the history process, independent of broker IPC."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import orjson
import structlog
from redis.exceptions import RedisError

logger = structlog.get_logger(__name__)
READ_PROOF = "return {redis.call('GET', KEYS[1]) or '', redis.call('PTTL', KEYS[1])}"


class HistoryAuthorityConflict(RuntimeError):
    """Positive evidence of an incompatible live owner; requires operator review."""


class HistoryAuthority:
    def __init__(self, redis: Callable, *, clock=time.monotonic, sleep=asyncio.sleep):
        self.redis = redis
        self.clock = clock
        self.sleep = sleep
        self.expires = 0.0
        self.owner = None
        self.reason = "starting"
        self.failures = 0
        self._lock = asyncio.Lock()
        self.conflict = None

    def _conflict(self, reason):
        self.expires = 0.0
        self.conflict = reason
        raise HistoryAuthorityConflict(reason)

    @property
    def ready(self):
        return self.clock() < self.expires

    def _unavailable(self, reason):
        self.expires = 0.0
        self.failures += 1
        if self.reason != reason:
            logger.warning("history_authority_waiting", reason=reason)
        self.reason = reason
        return False

    async def check(self):
        async with self._lock:
            if self.conflict:
                raise HistoryAuthorityConflict(self.conflict)
            # Native callers always await this read. Health may retain the
            # earlier proof only until its original expiry, never extend it.
            started = self.clock()
            try:
                async with asyncio.timeout(2):
                    raw, ttl_ms = await self.redis().eval(READ_PROOF, 1, "poller:status")
                if not raw:
                    return self._unavailable("poller_status_missing")
                data = orjson.loads(raw)
                if not isinstance(data, dict):
                    return self._unavailable("poller_status_invalid")
                if data.get("history_isolated") is False:
                    self._conflict("live_history_ownership_conflict")
                if data.get("history_isolated") is not True:
                    return self._unavailable("poller_isolation_missing")
                owner = data.get("started_at")
                if not isinstance(owner, str) or not owner:
                    return self._unavailable("poller_generation_missing")
                if not isinstance(ttl_ms, int) or not 0 < ttl_ms <= 10_000:
                    return self._unavailable("poller_status_expired")
                expires = started + min(ttl_ms / 1000, 2.5)
                if self.clock() >= expires:
                    return self._unavailable("poller_status_expired")
                if self.owner is not None and owner != self.owner:
                    self._conflict("live_history_generation_changed")
                self.owner = owner
                self.expires = expires
                if self.failures:
                    logger.info("history_authority_restored", failed_probes=self.failures)
                self.reason = "ok"
                self.failures = 0
                return True
            except (TimeoutError, RedisError, ValueError, TypeError):
                return self._unavailable("poller_status_read_failed")

    async def wait(self):
        """No native work is submitted until a fresh, expiring proof is read."""
        while not await self.check():
            await self.sleep(min(2 ** min(self.failures - 1, 3), 5))

    async def monitor(self):
        while True:
            await self.check()
            await self.sleep(1)
