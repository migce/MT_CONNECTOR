"""
Pydantic schemas for the API layer — request parameters and response models.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


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
