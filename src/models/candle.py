"""SQLAlchemy model for the ``candles`` hypertable."""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, Text

from .tick import Base


class Candle(Base):
    """OHLCV candle bar stored in TimescaleDB hypertable."""

    __tablename__ = "candles"

    time = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    symbol = Column(Text, primary_key=True, nullable=False)
    timeframe = Column(Text, primary_key=True, nullable=False)
    open = Column("open", Float, nullable=False)
    high = Column("high", Float, nullable=False)
    low = Column("low", Float, nullable=False)
    close = Column("close", Float, nullable=False)
    tick_volume = Column(BigInteger, nullable=False, default=0)
    real_volume = Column(BigInteger, default=0)
    spread = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_candles_symbol_tf_time", "symbol", "timeframe", time.desc()),
    )

    def __repr__(self) -> str:
        return (
            f"<Candle {self.symbol} {self.timeframe} {self.time} "
            f"O={self.open} H={self.high} L={self.low} C={self.close}>"
        )
