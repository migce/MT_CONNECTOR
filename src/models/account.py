"""SQLAlchemy model for the ``trading_accounts`` table."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, Text

from .tick import Base


class TradingAccount(Base):
    """MT5 trading account registered for history / position monitoring.

    The poller's own account (used for market data) is the *system* account
    and is NOT stored here.  This table holds **user-defined** accounts
    that the trader process connects to.
    """

    __tablename__ = "trading_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(Text, nullable=False, unique=True)
    description = Column(Text, nullable=True)
    mt5_login = Column(Integer, nullable=False, unique=True)
    mt5_password = Column(Text, nullable=False)
    mt5_server = Column(Text, nullable=False)
    mt5_path = Column(
        Text,
        nullable=False,
        server_default=r"C:\Program Files\MetaTrader 5\terminal64.exe",
    )
    enabled = Column(Boolean, nullable=False, server_default="true")
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return (
            f"<TradingAccount id={self.id} label={self.label!r} "
            f"login={self.mt5_login} enabled={self.enabled}>"
        )
