"""
Pydantic schemas for the API layer — request parameters and response models.
"""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, Field, model_validator

from src.api.digits import get_digits, normalize_money, normalize_price

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

    @model_validator(mode="after")
    def _round_prices(self):
        d = get_digits(self.symbol)
        self.open = normalize_price(self.open, d)
        self.high = normalize_price(self.high, d)
        self.low = normalize_price(self.low, d)
        self.close = normalize_price(self.close, d)
        return self


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

    @model_validator(mode="after")
    def _round_prices(self):
        d = get_digits(self.symbol)
        if self.bid is not None:
            self.bid = normalize_price(self.bid, d)
        if self.ask is not None:
            self.ask = normalize_price(self.ask, d)
        if self.last is not None:
            self.last = normalize_price(self.last, d)
        return self


# ---------------------------------------------------------------
# Symbol info
# ---------------------------------------------------------------

class SymbolInfo(BaseModel):
    symbol: str
    description: str = ""
    tracked: bool = False

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------
# Health
# ---------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str = "ok"
    mt5_connected: bool = False
    trader_connected: bool = False
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
    _spread_digits: int = 6

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _round_spread(self):
        self.spread = normalize_price(self.spread, self._spread_digits)
        return self


class SpreadAggPoint(BaseModel):
    """Aggregated spread over a time bucket."""
    time: datetime = Field(description="Bucket start time")
    spread_avg: float = Field(description="Average spread in the bucket")
    spread_min: float = Field(description="Minimum spread in the bucket")
    spread_max: float = Field(description="Maximum spread in the bucket")
    _spread_digits: int = 6

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _round_spread(self):
        self.spread_avg = normalize_price(self.spread_avg, self._spread_digits)
        self.spread_min = normalize_price(self.spread_min, self._spread_digits)
        self.spread_max = normalize_price(self.spread_max, self._spread_digits)
        return self


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


# ---------------------------------------------------------------
# Trading accounts
# ---------------------------------------------------------------

class VerifyRequest(BaseModel):
    """Payload for verifying MT5 credentials without creating an account."""
    mt5_login: int = Field(description="MT5 account number")
    mt5_password: str = Field(description="MT5 account password")
    mt5_server: str = Field(description="Broker server name")
    mt5_path: Optional[str] = Field(
        default=None,
        description="Path to terminal64.exe. Uses default from .env if null.",
    )
    account_id: Optional[int] = Field(
        default=None,
        description="Existing account ID (context only, not used for creation).",
    )


class VerifyResponse(BaseModel):
    """Result of MT5 credential verification."""
    ok: bool
    account_name: str = ""
    server: str = ""
    balance: float = 0
    leverage: int = 0
    currency: str = ""
    message: str = ""


class AccountCreate(BaseModel):
    """Payload for creating a new trading account."""
    label: str = Field(description="Human-readable label, e.g. 'Demo-1'")
    description: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Optional free-text description (up to 255 characters).",
    )
    mt5_login: int = Field(description="MT5 account number")
    mt5_password: str = Field(description="MT5 account password")
    mt5_server: str = Field(description="Broker server name")
    enabled: bool = Field(default=True)
    verify_credentials: bool = Field(
        default=False,
        description="If true, verify MT5 login before saving. Requires trader process.",
    )


class AccountUpdate(BaseModel):
    """Payload for updating a trading account (all fields optional)."""
    label: Optional[str] = None
    description: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Optional free-text description (up to 255 characters).",
    )
    mt5_login: Optional[int] = None
    mt5_password: Optional[str] = None
    mt5_server: Optional[str] = None
    mt5_path: Optional[str] = None
    enabled: Optional[bool] = None
    verify_credentials: bool = Field(
        default=False,
        description="If true, verify MT5 login before saving. Requires trader process.",
    )


class AccountResponse(BaseModel):
    """Public representation of a trading account (password excluded)."""
    id: int
    label: str
    description: Optional[str] = None
    mt5_login: int
    mt5_server: str
    mt5_path: str
    enabled: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------
# Deals & Positions
# ---------------------------------------------------------------

class DealResponse(BaseModel):
    ticket: int
    account_id: int
    order: int
    time: datetime
    time_msc: int
    type: int
    entry: int
    magic: int = 0
    position_id: int = 0
    reason: int = 0
    symbol: str
    volume: float
    price: float
    commission: float = 0.0
    swap: float = 0.0
    profit: float = 0.0
    fee: float = 0.0
    comment: str = ""
    external_id: str = ""

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _round_prices(self):
        d = get_digits(self.symbol)
        self.price = normalize_price(self.price, d)
        self.volume = normalize_price(self.volume, 2)
        self.commission = normalize_money(self.commission)
        self.swap = normalize_money(self.swap)
        self.profit = normalize_money(self.profit)
        self.fee = normalize_money(self.fee)
        return self


class PositionResponse(BaseModel):
    ticket: int
    account_id: int
    time: datetime
    time_update: Optional[datetime] = None
    type: int
    magic: int = 0
    identifier: int = 0
    reason: int = 0
    symbol: str
    volume: float
    price_open: float
    price_current: float
    sl: float = 0.0
    tp: float = 0.0
    swap: float = 0.0
    profit: float = 0.0
    comment: str = ""
    external_id: str = ""

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _round_prices(self):
        d = get_digits(self.symbol)
        self.price_open = normalize_price(self.price_open, d)
        self.price_current = normalize_price(self.price_current, d)
        self.sl = normalize_price(self.sl, d)
        self.tp = normalize_price(self.tp, d)
        self.volume = normalize_price(self.volume, 2)
        self.swap = normalize_money(self.swap)
        self.profit = normalize_money(self.profit)
        return self


# ---------------------------------------------------------------
# Account info (balance / equity / margin)
# ---------------------------------------------------------------

class AccountInfoResponse(BaseModel):
    account_id: int
    balance: float
    equity: float
    margin: float
    margin_free: float
    margin_level: float
    leverage: int
    currency: str
    profit: float
    name: str = ""
    server: str = ""
    trade_mode: int = 0
    open_positions_count: int = 0
    open_volume_lots: float = 0.0
    has_open_positions: bool = False
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _round_money(self):
        self.balance = normalize_money(self.balance)
        self.equity = normalize_money(self.equity)
        self.margin = normalize_money(self.margin)
        self.margin_free = normalize_money(self.margin_free)
        self.margin_level = normalize_money(self.margin_level)
        self.profit = normalize_money(self.profit)
        self.open_volume_lots = normalize_money(self.open_volume_lots)
        self.has_open_positions = self.open_positions_count > 0
        return self
