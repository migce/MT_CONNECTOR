"""SQLAlchemy model for the ``sync_state`` table."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, DateTime, Text

from .tick import Base


class SyncState(Base):
    """Tracks the last-synced timestamp per (symbol, data_type) pair.

    ``data_type`` is either ``'tick'`` or a timeframe string like ``'M1'``,
    ``'H1'``, etc.
    """

    __tablename__ = "sync_state"

    symbol = Column(Text, primary_key=True, nullable=False)
    data_type = Column(Text, primary_key=True, nullable=False)
    last_synced_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime(1970, 1, 1, tzinfo=timezone.utc),
    )
    last_tick_msc = Column(BigInteger, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return (
            f"<SyncState {self.symbol}/{self.data_type} "
            f"synced_at={self.last_synced_at}>"
        )
