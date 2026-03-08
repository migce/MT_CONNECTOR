"""
Run the TimescaleDB DDL script (``scripts/init_db.sql``) against
the configured database.

Called once on first startup or via ``python -m src.db.init_timescale``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
from sqlalchemy import text

from src.db.engine import get_engine

logger = structlog.get_logger(__name__)

SQL_FILE = Path(__file__).resolve().parents[2] / "scripts" / "init_db.sql"


async def init_timescaledb() -> None:
    """Execute the DDL init script inside a transaction."""
    engine = get_engine()
    sql = SQL_FILE.read_text(encoding="utf-8")

    # Split on semicolons — execute each statement separately
    # (TimescaleDB functions cannot run inside a multi-statement string
    # through SQLAlchemy's execute easily.)
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]

    async with engine.begin() as conn:
        for stmt in statements:
            # Skip pure comments
            lines = [ln for ln in stmt.splitlines() if not ln.strip().startswith("--")]
            clean = "\n".join(lines).strip()
            if not clean:
                continue
            try:
                await conn.execute(text(clean))
            except Exception as exc:
                # Some statements are idempotent (IF NOT EXISTS) — log & continue
                logger.warning("init_ddl_stmt_error", error=str(exc), stmt=clean[:120])

    logger.info("timescaledb_schema_initialized", sql_file=str(SQL_FILE))


if __name__ == "__main__":
    from src.config import get_settings
    from src.logging_config import setup_logging

    s = get_settings()
    setup_logging(s.log_level, s.log_format)
    asyncio.run(init_timescaledb())
