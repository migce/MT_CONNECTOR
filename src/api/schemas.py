"""
Pydantic schemas for the API layer — request parameters and response models.
"""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


# ---------------------------------------------------------------
# Paginated response wrapper
# ---------------------------------------------------------------

class PaginatedResponse(BaseModel, Generic[T]):
    """
    Generic wrapper for paginated list endpoints.

    - **data** — list of items for the current page
    - **count** — number of items in ``data`` (convenience)
    - **has_more** — ``true`` if additional rows exist beyond the requested limit
    - **next_from** — ISO-8601 timestamp to pass as ``from`` for the next page
      (``null`` when ``has_more`` is ``false``)
    """

    data: list[T]
    count: int = Field(description="Number of items in `data`.")
    has_more: bool = Field(
        description="True if more rows exist beyond the requested limit.",
    )
    next_from: Optional[str] = Field(
        default=None,
        description=(
            "ISO-8601 timestamp to use as the `from` parameter "
            "for fetching the next page. Null when has_more is false."
        ),
    )


# ---------------------------------------------------------------
# Candle
# ---------------------------------------------------------------

class CandleResponse(BaseModel):
    time: datetime
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    tick_volume: int
    real_volume: int = 0
    spread: int = 0

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------
# Tick
# ---------------------------------------------------------------

class TickResponse(BaseModel):
    time_msc: datetime
    symbol: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    volume: int = 0
    flags: int = 0

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------
# Symbol info
# ---------------------------------------------------------------

class SymbolInfo(BaseModel):
    symbol: str
    description: str = ""

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------
# Health
# ---------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str = "ok"
    mt5_connected: bool = False
    db_connected: bool = False
    redis_connected: bool = False
    uptime_sec: float = 0.0
    symbols_active: int = 0
    version: str = "1.0.0"


# ---------------------------------------------------------------
# Service uptime
# ---------------------------------------------------------------

class ServiceUptimeEntry(BaseModel):
    """Uptime stats for a single service over a time window."""
    service: str = Field(description="Service name: mt5, db, redis, api")
    up_sec: float = Field(description="Total seconds the service was UP")
    down_sec: float = Field(description="Total seconds the service was DOWN")
    uptime_pct: float = Field(description="Uptime percentage (0-100)")


class UptimeResponse(BaseModel):
    """Uptime summaries for all services."""
    period_24h: list[ServiceUptimeEntry] = Field(
        default_factory=list,
        description="Last 24 hours uptime per service",
    )
    period_30d: list[ServiceUptimeEntry] = Field(
        default_factory=list,
        description="Last 30 days uptime per service",
    )


# ---------------------------------------------------------------
# Spread history
# ---------------------------------------------------------------

class SpreadPoint(BaseModel):
    """Single spread data point (from candles or raw ticks)."""
    time: datetime
    spread: float = Field(description="Spread value (points for candles, price units for ticks)")

    model_config = {"from_attributes": True}


class SpreadAggPoint(BaseModel):
    """Aggregated spread over a time bucket."""
    time: datetime = Field(description="Bucket start time")
    spread_avg: float = Field(description="Average spread in the bucket")
    spread_min: float = Field(description="Minimum spread in the bucket")
    spread_max: float = Field(description="Maximum spread in the bucket")

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------
# WebSocket messages (used for documentation / client SDK)
# ---------------------------------------------------------------

class WsTickMessage(BaseModel):
    """JSON message pushed over the ``/ws/ticks/{symbol}`` WebSocket."""
    event: str = "tick"
    symbol: str
    bid: float
    ask: float
    last: Optional[float] = None
    volume: int = 0
    time_msc: datetime


class WsCandleMessage(BaseModel):
    """JSON message pushed over ``/ws/candles/{symbol}/{timeframe}``."""
    event: str = "candle"
    symbol: str
    timeframe: str
    time: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: int
    real_volume: int = 0
    spread: int = 0
