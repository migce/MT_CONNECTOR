"""
MT5 Connector — Application Configuration.

All settings are loaded from environment variables / .env file.
Validated via Pydantic Settings.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ---------------------------------------------------------------------------
# Timeframe mapping helpers
# ---------------------------------------------------------------------------

class Timeframe(str, enum.Enum):
    """Supported OHLCV timeframes."""

    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"

    @property
    def seconds(self) -> int:
        """Duration of this timeframe in seconds."""
        return _TF_SECONDS[self]

    @property
    def mt5_constant(self) -> int:
        """Return the MetaTrader5 TIMEFRAME_* integer constant."""
        return _TF_MT5[self]

    @classmethod
    def from_string(cls, value: str) -> "Timeframe":
        return cls(value.upper())


# MT5 TIMEFRAME constants (values from MetaTrader5 package)
# We hard-code them to avoid importing mt5 at config-parse time.
_TF_MT5 = {
    Timeframe.M1: 1,
    Timeframe.M5: 5,
    Timeframe.M15: 15,
    Timeframe.H1: 16385,    # 0x4001
    Timeframe.H4: 16388,    # 0x4004
    Timeframe.D1: 16408,    # 0x4018
}

_TF_SECONDS = {
    Timeframe.M1: 60,
    Timeframe.M5: 300,
    Timeframe.M15: 900,
    Timeframe.H1: 3600,
    Timeframe.H4: 14400,
    Timeframe.D1: 86400,
}

# Parent timeframes: which base TF is needed to aggregate into target
TF_AGGREGATION_MAP: dict[Timeframe, Timeframe] = {
    Timeframe.M5: Timeframe.M1,
    Timeframe.M15: Timeframe.M1,
    Timeframe.H1: Timeframe.M1,
    Timeframe.H4: Timeframe.H1,
    Timeframe.D1: Timeframe.H1,
}


# ---------------------------------------------------------------------------
# Custom (non-standard) timeframe support
# ---------------------------------------------------------------------------

_CUSTOM_TF_RE = re.compile(
    r"^(?P<unit>[MHDWST])(?P<value>\d+)$", re.IGNORECASE,
)

_UNIT_MULTIPLIER = {
    "S": 1,       # seconds
    "M": 60,      # minutes
    "H": 3600,    # hours
    "D": 86400,   # days
    "W": 604800,  # weeks
}

# Standard timeframes that we already store pre-built
_STANDARD_SECONDS = {tf.seconds for tf in Timeframe}


@dataclass(frozen=True)
class CustomTimeframe:
    """Parsed custom timeframe descriptor."""

    raw: str           # original string, e.g. "M2", "H6", "T500"
    is_tick_bar: bool  # True for tick-count bars (T100, T500 …)
    seconds: int       # interval in seconds (0 for tick bars)
    tick_count: int    # ticks per bar (0 for time-based bars)

    @property
    def bucket_interval(self) -> str:
        """PostgreSQL interval literal for ``time_bucket``."""
        return f"{self.seconds} seconds"


def parse_custom_timeframe(raw: str) -> CustomTimeframe:
    """
    Parse a custom timeframe string.

    Supported formats
    -----------------
    **Time-based** (aggregated from stored M1 candles via ``time_bucket``):
        ``M2``, ``M3``, ``M7``, ``M10``, ``M20``, ``M30``,
        ``H2``, ``H3``, ``H6``, ``H8``, ``H12``,
        ``D2``, ``W1`` — anything that matches ``[MHDWS]\\d+``.

    **Tick-based** (built on-the-fly from raw ticks):
        ``T100``, ``T250``, ``T500``, ``T1000`` — ``T\\d+``.

    Raises *ValueError* for unparsable strings.
    """
    raw = raw.strip().upper()
    m = _CUSTOM_TF_RE.match(raw)
    if not m:
        raise ValueError(
            f"Invalid custom timeframe '{raw}'. "
            "Expected format: M<n>, H<n>, D<n>, W<n>, S<n> or T<n>."
        )
    unit = m.group("unit").upper()
    value = int(m.group("value"))
    if value <= 0:
        raise ValueError(f"Timeframe value must be > 0, got {value}.")

    if unit == "T":
        return CustomTimeframe(raw=raw, is_tick_bar=True, seconds=0, tick_count=value)

    seconds = _UNIT_MULTIPLIER[unit] * value
    return CustomTimeframe(raw=raw, is_tick_bar=False, seconds=seconds, tick_count=0)


def is_standard_timeframe(tf_str: str) -> bool:
    """Return True if *tf_str* matches a standard stored timeframe enum."""
    try:
        Timeframe(tf_str.upper())
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    """Application-wide settings loaded from .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    # --- MetaTrader 5 Terminal ---
    mt5_path: str = Field(
        default=r"C:\Program Files\MetaTrader 5\terminal64.exe",
        description="Path to MetaTrader 5 terminal executable.",
    )
    mt5_login: int = Field(..., description="MT5 account login number.")
    mt5_password: str = Field(..., description="MT5 account password.")
    mt5_server: str = Field(..., description="MT5 broker server name.")
    mt5_timeout: int = Field(default=30000, description="MT5 connection timeout (ms).")

    # --- Symbols (stored as raw CSV string to avoid pydantic-settings JSON parse) ---
    symbols_csv: str = Field(
        default="EURUSD",
        alias="SYMBOLS",
        description="Comma-separated list of symbols to track.",
    )

    # --- Timeframes (stored as raw CSV string) ---
    timeframes_csv: str = Field(
        default="M1,M5,M15,H1,H4,D1",
        alias="TIMEFRAMES",
        description="Comma-separated list of timeframes to store.",
    )

    # --- Database ---
    db_host: str = Field(default="localhost")
    db_port: int = Field(default=5432)
    db_name: str = Field(default="mt5_data")
    db_user: str = Field(default="mt5user")
    db_password: str = Field(
        ...,
        description="Database password. Must be set via DB_PASSWORD env var or .env file.",
    )
    db_pool_min: int = Field(default=2)
    db_pool_max: int = Field(default=10)

    # --- Redis ---
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_password: Optional[str] = Field(default=None)
    redis_db: int = Field(default=0)

    # --- Poller ---
    tick_poll_interval_ms: int = Field(default=50)
    candle_poll_interval_sec: int = Field(default=5)
    backfill_days: int = Field(default=30)
    gap_scan_interval_min: int = Field(default=15)
    mt5_heartbeat_interval_sec: int = Field(default=10)

    # --- API ---
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=2)
    ws_heartbeat_sec: int = Field(default=30)
    cors_origins: str = Field(
        default="*",
        description=(
            "Comma-separated allowed CORS origins. "
            "Use '*' to allow all (no credentials). "
            "Set explicit origins to enable credentials."
        ),
    )

    # --- Data Retention ---
    tick_retention_days: int = Field(default=90)
    tick_compression_days: int = Field(default=7)
    candle_compression_days: int = Field(default=30)

    # --- Logging ---
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")

    # ----- Computed helpers -----

    @property
    def symbols(self) -> list[str]:
        """Parsed list of symbol names."""
        return [s.strip().upper() for s in self.symbols_csv.split(",") if s.strip()]

    @property
    def timeframes(self) -> list[Timeframe]:
        """Parsed list of Timeframe enums."""
        return [Timeframe(s.strip().upper()) for s in self.timeframes_csv.split(",") if s.strip()]

    @property
    def dsn(self) -> str:
        """Async PostgreSQL DSN for asyncpg."""
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def dsn_sync(self) -> str:
        """Sync PostgreSQL DSN (for Alembic / admin scripts)."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def redis_url(self) -> str:
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # ----- Validators -----

    @field_validator("redis_password", mode="before")
    @classmethod
    def _empty_str_to_none(cls, v):
        if v == "":
            return None
        return v


# ---------------------------------------------------------------------------
# Singleton access
# ---------------------------------------------------------------------------

_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Return cached application settings (lazy singleton)."""
    global _settings
    if _settings is None:
        _settings = Settings()  # type: ignore[call-arg]
    return _settings


def reload_settings() -> Settings:
    """Force reload settings from env (useful in tests)."""
    global _settings
    _settings = Settings()  # type: ignore[call-arg]
    return _settings
