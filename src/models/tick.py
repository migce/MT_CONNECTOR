"""SQLAlchemy model for the ``ticks`` hypertable."""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base for all models."""
    pass


class Tick(Base):
    """Raw tick data stored in TimescaleDB hypertable."""

    __tablename__ = "ticks"

    # TimescaleDB hypertable — no single-column PK.
    # The unique constraint is (symbol, time_msc) via a DB-level index.
    time_msc = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    symbol = Column(Text, primary_key=True, nullable=False)
    bid = Column(Float)
    ask = Column(Float)
    last = Column(Float)
    volume = Column(BigInteger, default=0)
    flags = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_ticks_symbol_time", "symbol", time_msc.desc()),
    )

    def __repr__(self) -> str:
        return (
            f"<Tick {self.symbol} {self.time_msc} "
            f"bid={self.bid} ask={self.ask}>"
        )
