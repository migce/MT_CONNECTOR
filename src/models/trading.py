"""SQLAlchemy models for the ``deals`` and ``positions`` tables."""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, Text

from .tick import Base


class Deal(Base):
    """Closed deal from MT5 history (``mt5.history_deals_get``)."""

    __tablename__ = "deals"

    ticket = Column(BigInteger, primary_key=True)
    account_id = Column(Integer, nullable=False, index=True)
    order = Column(BigInteger, nullable=False, index=True)
    time = Column(DateTime(timezone=True), nullable=False)
    time_msc = Column(BigInteger, nullable=False)
    type = Column(Integer, nullable=False)
    entry = Column(Integer, nullable=False)
    magic = Column(BigInteger, default=0)
    position_id = Column(BigInteger, default=0, index=True)
    reason = Column(Integer, default=0)
    symbol = Column(Text, nullable=False)
    volume = Column(Float, nullable=False, default=0.0)
    price = Column(Float, nullable=False, default=0.0)
    commission = Column(Float, default=0.0)
    swap = Column(Float, default=0.0)
    profit = Column(Float, nullable=False, default=0.0)
    fee = Column(Float, default=0.0)
    comment = Column(Text, default="")
    external_id = Column(Text, default="")

    __table_args__ = (
        Index("idx_deals_account_time", "account_id", time.desc()),
        Index("idx_deals_account_symbol", "account_id", "symbol"),
    )

    def __repr__(self) -> str:
        return (
            f"<Deal #{self.ticket} {self.symbol} "
            f"vol={self.volume} profit={self.profit}>"
        )


class Position(Base):
    """Open position snapshot (``mt5.positions_get``).

    Rows are **replaced** on each sync cycle — this table always reflects
    the latest state of open positions for every tracked account.
    """

    __tablename__ = "positions"

    ticket = Column(BigInteger, primary_key=True)
    account_id = Column(Integer, nullable=False, index=True)
    time = Column(DateTime(timezone=True), nullable=False)
    time_update = Column(DateTime(timezone=True))
    type = Column(Integer, nullable=False)
    magic = Column(BigInteger, default=0)
    identifier = Column(BigInteger, default=0)
    reason = Column(Integer, default=0)
    symbol = Column(Text, nullable=False)
    volume = Column(Float, nullable=False, default=0.0)
    price_open = Column(Float, nullable=False, default=0.0)
    price_current = Column(Float, nullable=False, default=0.0)
    sl = Column(Float, default=0.0)
    tp = Column(Float, default=0.0)
    swap = Column(Float, default=0.0)
    profit = Column(Float, nullable=False, default=0.0)
    comment = Column(Text, default="")
    external_id = Column(Text, default="")

    __table_args__ = (
        Index("idx_positions_account", "account_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<Position #{self.ticket} {self.symbol} "
            f"vol={self.volume} profit={self.profit}>"
        )
