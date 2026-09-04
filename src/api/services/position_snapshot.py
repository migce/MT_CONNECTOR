"""Fail-closed validation of an atomic Trader-owned position snapshot."""
from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any


def validate_position_snapshot(payload: Any, account_id: int, *, max_age: float = 30) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Invalid position snapshot")
    if (payload.get("account_id") != account_id or payload.get("complete") is not True
            or payload.get("status") != "ok" or not isinstance(payload.get("generation"), str)
            or not payload["generation"]):
        raise ValueError("Incomplete position snapshot")
    try:
        started = datetime.fromisoformat(payload["started_at"].replace("Z", "+00:00"))
        completed = datetime.fromisoformat(payload["last_success_at"].replace("Z", "+00:00"))
        now = datetime.now(UTC)
        if (started.tzinfo is None or completed.tzinfo is None or completed < started
                or not 0 <= (now - started).total_seconds() <= max_age
                or completed > now):
            raise ValueError("Stale position snapshot")
        rows = payload["positions"]
        if (not isinstance(rows, list) or type(payload["position_count"]) is not int
                or len(rows) != payload["position_count"]):
            raise ValueError("Incomplete position list")
        tickets = []
        for row in rows:
            if (not isinstance(row, dict) or row.get("account_id") != account_id
                    or type(row.get("ticket")) is not int or row["ticket"] <= 0
                    or type(row.get("identifier")) is not int or row["identifier"] <= 0
                    or type(row.get("type")) is not int or row["type"] not in (0, 1)
                    or not isinstance(row.get("symbol"), str) or not row["symbol"]
                    or type(row.get("magic")) is not int
                    or not math.isfinite(float(row["volume"])) or float(row["volume"]) <= 0):
                raise ValueError("Invalid position row")
            tickets.append(row["ticket"])
        if len(set(tickets)) != len(tickets) or sorted(tickets) != payload["tickets"]:
            raise ValueError("Position snapshot ticket mismatch")
    except (KeyError, TypeError, AttributeError, OverflowError) as exc:
        raise ValueError("Invalid position snapshot") from exc
    return payload
