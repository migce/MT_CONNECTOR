"""
Database repository — trading accounts, deals, positions.

Separated from the main repository to keep market-data and trading
concerns isolated.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

import structlog
from sqlalchemy import text
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.db.engine import get_session_factory

logger = structlog.get_logger(__name__)

_db_retry = retry(
    retry=retry_if_exception_type((OSError, ConnectionError, TimeoutError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    reraise=True,
)


# ---------------------------------------------------------------
# Trading accounts CRUD
# ---------------------------------------------------------------

_INSERT_ACCOUNT_SQL = text("""
    INSERT INTO trading_accounts (label, description, mt5_login, mt5_password, mt5_server, mt5_path, enabled)
    VALUES (:label, :description, :mt5_login, :mt5_password, :mt5_server, :mt5_path, :enabled)
    RETURNING id, label, description, mt5_login, mt5_server, mt5_path, enabled, created_at, updated_at
""")

_UPDATE_ACCOUNT_SQL = text("""
    UPDATE trading_accounts
    SET label        = COALESCE(:label, label),
        description  = COALESCE(:description, description),
        mt5_login    = COALESCE(:mt5_login, mt5_login),
        mt5_password = COALESCE(:mt5_password, mt5_password),
        mt5_server   = COALESCE(:mt5_server, mt5_server),
        mt5_path     = COALESCE(:mt5_path, mt5_path),
        enabled      = COALESCE(:enabled, enabled),
        updated_at   = NOW()
    WHERE id = :id
    RETURNING id, label, description, mt5_login, mt5_server, mt5_path, enabled, created_at, updated_at
""")

_DELETE_ACCOUNT_SQL = text("""
    DELETE FROM trading_accounts WHERE id = :id RETURNING id
""")

_SELECT_ACCOUNTS_SQL = text("""
    SELECT id, label, description, mt5_login, mt5_server, mt5_path, enabled, created_at, updated_at
    FROM trading_accounts
    ORDER BY id
""")

_SELECT_ACCOUNT_BY_ID_SQL = text("""
    SELECT id, label, description, mt5_login, mt5_password, mt5_server, mt5_path, enabled,
           created_at, updated_at
    FROM trading_accounts
    WHERE id = :id
""")

_SELECT_ENABLED_ACCOUNTS_SQL = text("""
    SELECT id, label, mt5_login, mt5_password, mt5_server, mt5_path
    FROM trading_accounts
    WHERE enabled = TRUE
    ORDER BY id
""")


@_db_retry
async def create_account(
    label: str,
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
    mt5_path: str = r"C:\Program Files\MetaTrader 5\terminal64.exe",
    enabled: bool = True,
    description: str | None = None,
) -> dict[str, Any]:
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            row = (
                await session.execute(
                    _INSERT_ACCOUNT_SQL,
                    {
                        "label": label,
                        "description": description,
                        "mt5_login": mt5_login,
                        "mt5_password": mt5_password,
                        "mt5_server": mt5_server,
                        "mt5_path": mt5_path,
                        "enabled": enabled,
                    },
                )
            ).mappings().one()
    return dict(row)


@_db_retry
async def update_account(account_id: int, **fields) -> dict[str, Any] | None:
    params = {
        "id": account_id,
        "label": fields.get("label"),
        "description": fields.get("description"),
        "mt5_login": fields.get("mt5_login"),
        "mt5_password": fields.get("mt5_password"),
        "mt5_server": fields.get("mt5_server"),
        "mt5_path": fields.get("mt5_path"),
        "enabled": fields.get("enabled"),
    }
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(_UPDATE_ACCOUNT_SQL, params)
            row = result.mappings().first()
    return dict(row) if row else None


@_db_retry
async def delete_account(account_id: int) -> bool:
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(
                _DELETE_ACCOUNT_SQL, {"id": account_id},
            )
            return result.rowcount > 0


@_db_retry
async def list_accounts() -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(_SELECT_ACCOUNTS_SQL)
            return [dict(r) for r in result.mappings().all()]


@_db_retry
async def get_account(account_id: int) -> dict[str, Any] | None:
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(
                _SELECT_ACCOUNT_BY_ID_SQL, {"id": account_id},
            )
            row = result.mappings().first()
    return dict(row) if row else None


@_db_retry
async def get_enabled_accounts() -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(_SELECT_ENABLED_ACCOUNTS_SQL)
        return [dict(r) for r in result.mappings().all()]


# ---------------------------------------------------------------
# Deals
# ---------------------------------------------------------------

_UPSERT_DEALS_SQL = text("""
    INSERT INTO deals (
        ticket, account_id, "order", time, time_msc, type, entry,
        magic, position_id, reason, symbol, volume, price,
        commission, swap, profit, fee, comment, external_id
    ) VALUES (
        :ticket, :account_id, :order, :time, :time_msc, :type, :entry,
        :magic, :position_id, :reason, :symbol, :volume, :price,
        :commission, :swap, :profit, :fee, :comment, :external_id
    )
    ON CONFLICT (ticket) DO UPDATE SET
        profit     = EXCLUDED.profit,
        commission = EXCLUDED.commission,
        swap       = EXCLUDED.swap,
        fee        = EXCLUDED.fee,
        comment    = EXCLUDED.comment
""")

_SELECT_EXISTING_DEAL_TICKETS_SQL = text("""
    SELECT ticket
    FROM deals
    WHERE ticket = ANY(:tickets)
""")


@_db_retry
async def upsert_deals(rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        return {"total": 0, "inserted": 0, "updated": 0}

    # Estimate inserted/updated by checking which tickets already exist.
    # Using RETURNING with executemany is not reliable with our async driver.
    unique_tickets = {int(r["ticket"]) for r in rows if r.get("ticket") is not None}

    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            existing_result = await session.execute(
                _SELECT_EXISTING_DEAL_TICKETS_SQL,
                {"tickets": list(unique_tickets) or [0]},
            )
            existing_tickets = {int(x) for x in existing_result.scalars().all()}
            await session.execute(_UPSERT_DEALS_SQL, rows)

    total = len(unique_tickets)
    inserted = len(unique_tickets - existing_tickets)
    updated = len(unique_tickets & existing_tickets)

    logger.debug(
        "deals_upserted",
        total=total,
        inserted=inserted,
        updated=updated,
    )
    return {"total": total, "inserted": inserted, "updated": updated}


@_db_retry
async def get_last_deal_time(account_id: int) -> datetime | None:
    """Return the timestamp of the most recent deal for *account_id*, or None."""
    sql = text(
        "SELECT max(time) AS last_time FROM deals WHERE account_id = :account_id"
    )
    factory = get_session_factory()
    async with factory() as session:
        row = (await session.execute(sql, {"account_id": account_id})).first()
        return row[0] if row and row[0] else None


@_db_retry
async def query_deals(
    account_id: int,
    date_from: datetime,
    date_to: datetime,
    symbol: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    clauses = [
        "account_id = :account_id",
        "time >= :date_from",
        "time < :date_to",
    ]
    params: dict[str, Any] = {
        "account_id": account_id,
        "date_from": date_from,
        "date_to": date_to,
        "limit": limit,
    }
    if symbol:
        clauses.append("symbol = :symbol")
        params["symbol"] = symbol

    where = " AND ".join(clauses)
    sql = text(
        f"SELECT ticket, account_id, \"order\", time, time_msc, type, entry,"
        f"       magic, position_id, reason, symbol, volume, price,"
        f"       commission, swap, profit, fee, comment, external_id"
        f" FROM deals WHERE {where} ORDER BY time DESC LIMIT :limit"
    )
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r) for r in result.mappings().all()]


# ---------------------------------------------------------------
# Positions (open)
# ---------------------------------------------------------------

_REPLACE_POSITIONS_SQL = text("""
    INSERT INTO positions (
        ticket, account_id, time, time_update, type, magic, identifier,
        reason, symbol, volume, price_open, price_current,
        sl, tp, swap, profit, comment, external_id
    ) VALUES (
        :ticket, :account_id, :time, :time_update, :type, :magic, :identifier,
        :reason, :symbol, :volume, :price_open, :price_current,
        :sl, :tp, :swap, :profit, :comment, :external_id
    )
    ON CONFLICT (ticket) DO UPDATE SET
        price_current = EXCLUDED.price_current,
        time_update   = EXCLUDED.time_update,
        volume        = EXCLUDED.volume,
        sl            = EXCLUDED.sl,
        tp            = EXCLUDED.tp,
        swap          = EXCLUDED.swap,
        profit        = EXCLUDED.profit,
        comment       = EXCLUDED.comment
""")

_DELETE_STALE_POSITIONS_SQL = text("""
    DELETE FROM positions
    WHERE account_id = :account_id
      AND ticket != ALL(:active_tickets)
""")


@_db_retry
async def sync_positions(account_id: int, rows: list[dict[str, Any]]) -> int:
    """Replace all positions for *account_id* with the new snapshot."""
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            affected = 0
            if rows:
                result = await session.execute(_REPLACE_POSITIONS_SQL, rows)
                affected = result.rowcount
            # Remove positions that are no longer open
            active = [r["ticket"] for r in rows] if rows else [0]
            await session.execute(
                _DELETE_STALE_POSITIONS_SQL,
                {"account_id": account_id, "active_tickets": active},
            )
    logger.debug("positions_synced", account_id=account_id, count=affected)
    return affected


@_db_retry
async def query_positions(
    account_id: int,
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    clauses = ["account_id = :account_id"]
    params: dict[str, Any] = {"account_id": account_id}
    if symbol:
        clauses.append("symbol = :symbol")
        params["symbol"] = symbol

    where = " AND ".join(clauses)
    sql = text(
        f"SELECT ticket, account_id, time, time_update, type, magic, identifier,"
        f"       reason, symbol, volume, price_open, price_current,"
        f"       sl, tp, swap, profit, comment, external_id"
        f" FROM positions WHERE {where} ORDER BY time DESC"
    )
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r) for r in result.mappings().all()]


# ---------------------------------------------------------------
# Account info (balance / equity / margin snapshots)
# ---------------------------------------------------------------

_UPSERT_ACCOUNT_INFO_SQL = text("""
    INSERT INTO account_info (
        account_id, balance, equity, margin, margin_free, margin_level,
        leverage, currency, profit, name, server, trade_mode, updated_at
    ) VALUES (
        :account_id, :balance, :equity, :margin, :margin_free, :margin_level,
        :leverage, :currency, :profit, :name, :server, :trade_mode, NOW()
    )
    ON CONFLICT (account_id) DO UPDATE SET
        balance      = EXCLUDED.balance,
        equity       = EXCLUDED.equity,
        margin       = EXCLUDED.margin,
        margin_free  = EXCLUDED.margin_free,
        margin_level = EXCLUDED.margin_level,
        leverage     = EXCLUDED.leverage,
        currency     = EXCLUDED.currency,
        profit       = EXCLUDED.profit,
        name         = EXCLUDED.name,
        server       = EXCLUDED.server,
        trade_mode   = EXCLUDED.trade_mode,
        updated_at   = NOW()
""")


@_db_retry
async def upsert_account_info(row: dict[str, Any]) -> None:
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            await session.execute(_UPSERT_ACCOUNT_INFO_SQL, row)
    logger.debug("account_info_upserted", account_id=row.get("account_id"))


_QUERY_ACCOUNT_INFO_SQL = text("""
    SELECT a.account_id, a.balance, a.equity, a.margin, a.margin_free,
           a.margin_level, a.leverage, a.currency, a.profit, a.name,
           a.server, a.trade_mode, a.updated_at,
           COALESCE(p.open_positions_count, 0) AS open_positions_count,
           COALESCE(p.open_volume_lots, 0.0)   AS open_volume_lots
    FROM account_info a
    LEFT JOIN (
        SELECT account_id,
               COUNT(*)    AS open_positions_count,
               SUM(volume) AS open_volume_lots
        FROM positions
        GROUP BY account_id
    ) p ON p.account_id = a.account_id
    WHERE a.account_id = :account_id
""")

_QUERY_ALL_ACCOUNT_INFO_SQL = text("""
    SELECT a.account_id, a.balance, a.equity, a.margin, a.margin_free,
           a.margin_level, a.leverage, a.currency, a.profit, a.name,
           a.server, a.trade_mode, a.updated_at,
           COALESCE(p.open_positions_count, 0) AS open_positions_count,
           COALESCE(p.open_volume_lots, 0.0)   AS open_volume_lots
    FROM account_info a
    LEFT JOIN (
        SELECT account_id,
               COUNT(*)    AS open_positions_count,
               SUM(volume) AS open_volume_lots
        FROM positions
        GROUP BY account_id
    ) p ON p.account_id = a.account_id
    ORDER BY a.account_id
""")


@_db_retry
async def get_account_info(account_id: int) -> dict[str, Any] | None:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            _QUERY_ACCOUNT_INFO_SQL, {"account_id": account_id},
        )
        row = result.mappings().first()
    return dict(row) if row else None


@_db_retry
async def get_all_account_info() -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(_QUERY_ALL_ACCOUNT_INFO_SQL)
        return [dict(r) for r in result.mappings().all()]
