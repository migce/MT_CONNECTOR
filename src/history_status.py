"""Shared history availability contract; no broker or API dependencies."""
from __future__ import annotations

import time


class HistoryWorkerUnavailable(RuntimeError):
    def __init__(self, phase: str = "unavailable"):
        self.phase = phase if phase in {"starting", "reconnecting", "failed", "unavailable"} else "unavailable"
        super().__init__("Historical data loading is temporarily unavailable. Please retry shortly.")


def fresh_history_status(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    try:
        return -2 <= time.time() - float(data.get("observed_at", 0)) < 30
    except (TypeError, ValueError, OverflowError):
        return False
