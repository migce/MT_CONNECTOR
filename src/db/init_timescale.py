"""
Run the TimescaleDB DDL script (``scripts/init_db.sql``) against
the configured database.

Called once on first startup or via ``python -m src.db.init_timescale``.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import structlog
from sqlalchemy import text

from src.db.engine import get_engine

logger = structlog.get_logger(__name__)

SQL_FILE = Path(__file__).resolve().parents[2] / "scripts" / "init_db.sql"

# Regex to split SQL on semicolons that are NOT inside $$ blocks.
_STMT_SPLIT_RE = re.compile(
    r"""
    (\$\$.*?\$\$)  # match $$ ... $$ blocks (including contents)
    |              # OR
    (;)            # a statement-ending semicolon
    """,
    re.DOTALL | re.VERBOSE,
)


def _split_sql(sql: str) -> list[str]:
    """Split SQL text on top-level semicolons, respecting $$ blocks."""
    parts: list[str] = []
    current: list[str] = []
    last_end = 0
    for m in _STMT_SPLIT_RE.finditer(sql):
        current.append(sql[last_end:m.start()])
        if m.group(1):  # $$ block — keep as part of current statement
            current.append(m.group(1))
        else:  # semicolon — end of statement
            stmt = "".join(current).strip()
            if stmt and not stmt.startswith("--"):
                parts.append(stmt)
            current = []
        last_end = m.end()
    # trailing text after last semicolon
    tail = sql[last_end:].strip()
    if tail and not tail.startswith("--"):
        joined = "".join(current).strip() + (" " + tail if "".join(current).strip() else tail)
        if joined.strip():
            parts.append(joined.strip())
    elif current:
        joined = "".join(current).strip()
        if joined and not joined.startswith("--"):
            parts.append(joined)
    return parts


async def init_timescaledb() -> None:
    """Execute the DDL init script inside a transaction."""
    engine = get_engine()
    sql = SQL_FILE.read_text(encoding="utf-8")

    # Split on semicolons, respecting $$ blocks
    statements = _split_sql(sql)

    # Known-safe idempotent patterns that can legitimately fail
    _IDEMPOTENT_MARKERS = ("already exists", "IF NOT EXISTS", "duplicate key")

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
                err_msg = str(exc).lower()
                if any(marker.lower() in err_msg for marker in _IDEMPOTENT_MARKERS):
                    logger.debug("init_ddl_idempotent_skip", stmt=clean[:120])
                else:
                    logger.error("init_ddl_stmt_fatal", error=str(exc), stmt=clean[:120])
                    raise

    logger.info("timescaledb_schema_initialized", sql_file=str(SQL_FILE))


if __name__ == "__main__":
    from src.config import get_settings
    from src.logging_config import setup_logging

    s = get_settings()
    setup_logging(s.log_level, s.log_format)
    asyncio.run(init_timescaledb())
