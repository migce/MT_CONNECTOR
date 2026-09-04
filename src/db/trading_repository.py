"""
Database repository — trading accounts, deals, positions.

Separated from the main repository to keep market-data and trading
concerns isolated.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence
from uuid import UUID

import orjson
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
    RETURNING id, label, description, mt5_login, mt5_server, mt5_path, enabled,
              session_required,
              enabled AND COALESCE(
                  session_required,
                  NOT EXISTS (SELECT 1 FROM trading_account_session_demand WHERE singleton_id = 1)
              ) AS session_active,
              created_at, updated_at
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
    RETURNING id, label, description, mt5_login, mt5_server, mt5_path, enabled,
              session_required,
              enabled AND COALESCE(
                  session_required,
                  NOT EXISTS (SELECT 1 FROM trading_account_session_demand WHERE singleton_id = 1)
              ) AS session_active,
              created_at, updated_at
""")

_DELETE_ACCOUNT_SQL = text("""
    DELETE FROM trading_accounts WHERE id = :id RETURNING id
""")

_SELECT_ACCOUNTS_SQL = text("""
    SELECT id, label, description, mt5_login, mt5_server, mt5_path, enabled,
           session_required,
           enabled AND COALESCE(
               session_required,
               NOT EXISTS (SELECT 1 FROM trading_account_session_demand WHERE singleton_id = 1)
           ) AS session_active,
           created_at, updated_at
    FROM trading_accounts
    ORDER BY id
""")

_SELECT_ACCOUNT_BY_ID_SQL = text("""
    SELECT id, label, description, mt5_login, mt5_password, mt5_server, mt5_path, enabled,
           session_required,
           enabled AND COALESCE(
               session_required,
               NOT EXISTS (SELECT 1 FROM trading_account_session_demand WHERE singleton_id = 1)
           ) AS session_active,
           created_at, updated_at
    FROM trading_accounts
    WHERE id = :id
""")

_SELECT_ENABLED_ACCOUNTS_SQL = text("""
    SELECT id, label, mt5_login, mt5_password, mt5_server, mt5_path
    FROM trading_accounts
    WHERE enabled = TRUE
      AND COALESCE(
          session_required,
          NOT EXISTS (SELECT 1 FROM trading_account_session_demand WHERE singleton_id = 1)
      )
    ORDER BY id
""")

_SELECT_SESSION_DEMAND_SQL = text("""
    SELECT source_updated_at, desired_account_ids, snapshot_id
    FROM trading_account_session_demand
    WHERE singleton_id = 1
    FOR UPDATE
""")

_SELECT_KNOWN_ACCOUNT_IDS_SQL = text("""
    SELECT id FROM trading_accounts ORDER BY id
""")

_APPLY_SESSION_DEMAND_SQL = text("""
    UPDATE trading_accounts
    SET session_required = id = ANY(CAST(:account_ids AS INTEGER[]))
""")

_UPSERT_SESSION_DEMAND_SQL = text("""
    INSERT INTO trading_account_session_demand (
        singleton_id, source_updated_at, desired_account_ids, snapshot_id, applied_at
    ) VALUES (1, :source_updated_at, CAST(:account_ids AS INTEGER[]), :snapshot_id, NOW())
    ON CONFLICT (singleton_id) DO UPDATE SET
        source_updated_at = EXCLUDED.source_updated_at,
        desired_account_ids = EXCLUDED.desired_account_ids,
        snapshot_id = EXCLUDED.snapshot_id,
        applied_at = NOW()
""")

_SELECT_EFFECTIVE_ACCOUNT_IDS_SQL = text("""
    SELECT id
    FROM trading_accounts
    WHERE enabled = TRUE AND session_required = TRUE
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


async def reconcile_account_session_demand(
    *,
    account_ids: Sequence[int],
    source_updated_at: datetime,
    snapshot_id: str,
) -> dict[str, Any]:
    """Atomically apply a complete Monitor-owned desired account set.

    Before the first accepted snapshot, ``NULL`` demand retains the legacy
    enabled-only behaviour. Afterwards new or omitted accounts remain stopped
    until a newer complete snapshot explicitly requires them.
    """
    normalized_ids = sorted({int(account_id) for account_id in account_ids})
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            current_result = await session.execute(_SELECT_SESSION_DEMAND_SQL)
            current = current_result.mappings().first()
            if current is not None and source_updated_at < current["source_updated_at"]:
                effective_result = await session.execute(_SELECT_EFFECTIVE_ACCOUNT_IDS_SQL)
                return {
                    "applied": False,
                    "changed": False,
                    "stale": True,
                    "account_ids": list(current["desired_account_ids"]),
                    "effective_account_ids": [int(value) for value in effective_result.scalars().all()],
                    "source_updated_at": current["source_updated_at"],
                    "snapshot_id": current["snapshot_id"],
                }

            known_result = await session.execute(_SELECT_KNOWN_ACCOUNT_IDS_SQL)
            known_ids = {int(value) for value in known_result.scalars().all()}
            unknown_ids = sorted(set(normalized_ids) - known_ids)
            if unknown_ids:
                raise ValueError(f"Unknown trading account IDs: {unknown_ids}")

            changed = (
                current is None
                or sorted(int(value) for value in current["desired_account_ids"])
                != normalized_ids
            )
            if changed:
                await session.execute(
                    _APPLY_SESSION_DEMAND_SQL,
                    {"account_ids": normalized_ids},
                )
            await session.execute(
                _UPSERT_SESSION_DEMAND_SQL,
                {
                    "account_ids": normalized_ids,
                    "source_updated_at": source_updated_at,
                    "snapshot_id": snapshot_id,
                },
            )
            effective_result = await session.execute(_SELECT_EFFECTIVE_ACCOUNT_IDS_SQL)
            effective_ids = [int(value) for value in effective_result.scalars().all()]

    return {
        "applied": True,
        "changed": changed,
        "stale": False,
        "account_ids": normalized_ids,
        "effective_account_ids": effective_ids,
        "source_updated_at": source_updated_at,
        "snapshot_id": snapshot_id,
    }


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


# ---------------------------------------------------------------
# Durable close-only trade commands
# ---------------------------------------------------------------

_TRADE_COMMAND_COLUMNS = """
    id, account_id, action, status, position_ticket,
    expected_position_identifier, expected_symbol, expected_type,
    expected_magic, max_volume, reason, correlation_id, requested_by,
    requested_at, expires_at, next_attempt_at, attempt_count, claimed_at,
    submitted_at, completed_at, last_error, result, created_at, updated_at
"""

_QUALIFIED_TRADE_COMMAND_COLUMNS = """
    command.id, command.account_id, command.action, command.status,
    command.position_ticket, command.expected_position_identifier,
    command.expected_symbol, command.expected_type, command.expected_magic,
    command.max_volume, command.reason, command.correlation_id,
    command.requested_by, command.requested_at, command.expires_at,
    command.next_attempt_at, command.attempt_count, command.claimed_at,
    command.submitted_at, command.completed_at, command.last_error,
    command.result, command.created_at, command.updated_at
"""


def _json(value: Any) -> str:
    return orjson.dumps(value if value is not None else {}).decode()


def _uuid(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


@_db_retry
async def create_trade_command(
    *,
    command_id: UUID,
    account_id: int,
    position_ticket: int,
    expected_position_identifier: int | None,
    expected_symbol: str,
    expected_type: int,
    expected_magic: int | None,
    max_volume: float,
    reason: str,
    correlation_id: str | None,
    requested_by: str,
    requested_at: datetime,
    expires_at: datetime | None,
) -> tuple[dict[str, Any], bool]:
    """Create an idempotent close command and return ``(row, created)``."""
    insert_sql = text(f"""
        INSERT INTO trade_commands (
            id, account_id, action, status, position_ticket,
            expected_position_identifier, expected_symbol, expected_type,
            expected_magic, max_volume, reason, correlation_id, requested_by,
            requested_at, expires_at, next_attempt_at
        ) VALUES (
            :id, :account_id, 'close_position', 'accepted', :position_ticket,
            :expected_position_identifier, :expected_symbol, :expected_type,
            :expected_magic, :max_volume, :reason, :correlation_id, :requested_by,
            :requested_at, :expires_at, NOW()
        )
        ON CONFLICT (id) DO NOTHING
        RETURNING {_TRADE_COMMAND_COLUMNS}
    """)
    select_sql = text(f"SELECT {_TRADE_COMMAND_COLUMNS} FROM trade_commands WHERE id = :id")
    params = {
        "id": command_id,
        "account_id": account_id,
        "position_ticket": position_ticket,
        "expected_position_identifier": expected_position_identifier,
        "expected_symbol": expected_symbol,
        "expected_type": expected_type,
        "expected_magic": expected_magic,
        "max_volume": max_volume,
        "reason": reason,
        "correlation_id": correlation_id,
        "requested_by": requested_by,
        "requested_at": requested_at,
        "expires_at": expires_at,
    }
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            inserted = (await session.execute(insert_sql, params)).mappings().first()
            if inserted is not None:
                return dict(inserted), True
            existing = (await session.execute(select_sql, {"id": command_id})).mappings().one()
            return dict(existing), False


@_db_retry
async def get_trade_command(command_id: UUID | str) -> dict[str, Any] | None:
    sql = text(f"SELECT {_TRADE_COMMAND_COLUMNS} FROM trade_commands WHERE id = :id")
    factory = get_session_factory()
    async with factory() as session:
        row = (await session.execute(sql, {"id": _uuid(command_id)})).mappings().first()
    return dict(row) if row else None


@_db_retry
async def list_trade_attempts(command_id: UUID | str) -> list[dict[str, Any]]:
    sql = text("""
        SELECT id, command_id, attempt_no, phase, retcode, message,
               request_payload, result_payload, started_at, finished_at
        FROM trade_attempts
        WHERE command_id = :command_id
        ORDER BY attempt_no, id
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, {"command_id": _uuid(command_id)})
        return [dict(row) for row in result.mappings().all()]


@_db_retry
async def claim_next_trade_command(account_ids: Sequence[int]) -> dict[str, Any] | None:
    if not account_ids:
        return None
    sql = text(f"""
        WITH candidate AS (
            SELECT id
            FROM trade_commands
            WHERE status IN ('accepted', 'retry_pending')
              AND next_attempt_at <= NOW()
              AND (expires_at IS NULL OR expires_at > NOW())
              AND account_id = ANY(:account_ids)
            ORDER BY next_attempt_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE trade_commands AS command
        SET status = 'claimed',
            claimed_at = NOW(),
            attempt_count = command.attempt_count + 1,
            updated_at = NOW()
        FROM candidate
        WHERE command.id = candidate.id
        RETURNING {_QUALIFIED_TRADE_COMMAND_COLUMNS}
    """)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            row = (
                await session.execute(sql, {"account_ids": list(account_ids)})
            ).mappings().first()
    return dict(row) if row else None


@_db_retry
async def requeue_stale_claimed_commands(timeout_sec: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_sec)
    sql = text("""
        UPDATE trade_commands
        SET status = 'retry_pending', claimed_at = NULL, next_attempt_at = NOW(),
            last_error = 'dispatcher_recovered_stale_claim', updated_at = NOW()
        WHERE status = 'claimed' AND claimed_at < :cutoff
    """)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(sql, {"cutoff": cutoff})
            return result.rowcount


@_db_retry
async def expire_trade_commands() -> int:
    sql = text("""
        UPDATE trade_commands
        SET status = 'expired', completed_at = NOW(), updated_at = NOW(),
            last_error = COALESCE(last_error, 'command_expired')
        WHERE status IN ('accepted', 'retry_pending')
          AND expires_at IS NOT NULL AND expires_at <= NOW()
    """)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(sql)
            return result.rowcount


@_db_retry
async def append_trade_attempt(
    *,
    command_id: UUID | str,
    attempt_no: int,
    phase: str,
    retcode: int | None,
    message: str | None,
    request_payload: dict[str, Any],
    result_payload: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    sql = text("""
        INSERT INTO trade_attempts (
            command_id, attempt_no, phase, retcode, message,
            request_payload, result_payload, started_at, finished_at
        ) VALUES (
            :command_id, :attempt_no, :phase, :retcode, :message,
            CAST(:request_payload AS JSONB), CAST(:result_payload AS JSONB),
            :started_at, :finished_at
        )
    """)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            await session.execute(sql, {
                "command_id": _uuid(command_id),
                "attempt_no": attempt_no,
                "phase": phase,
                "retcode": retcode,
                "message": message,
                "request_payload": _json(request_payload),
                "result_payload": _json(result_payload),
                "started_at": started_at,
                "finished_at": finished_at,
            })


@_db_retry
async def finish_trade_command(
    command_id: UUID | str,
    *,
    status: str,
    result: dict[str, Any],
    error: str | None = None,
) -> None:
    sql = text("""
        UPDATE trade_commands
        SET status = :status, result = CAST(:result AS JSONB), last_error = :error,
            submitted_at = COALESCE(submitted_at, NOW()), completed_at = NOW(),
            updated_at = NOW()
        WHERE id = :id
    """)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            await session.execute(sql, {
                "id": _uuid(command_id),
                "status": status,
                "result": _json(result),
                "error": error,
            })


@_db_retry
async def retry_trade_command(
    command_id: UUID | str,
    *,
    result: dict[str, Any],
    error: str,
    delay_sec: float,
) -> None:
    next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay_sec)
    sql = text("""
        UPDATE trade_commands
        SET status = 'retry_pending', result = CAST(:result AS JSONB),
            last_error = :error, claimed_at = NULL, next_attempt_at = :next_attempt_at,
            updated_at = NOW()
        WHERE id = :id
    """)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            await session.execute(sql, {
                "id": _uuid(command_id),
                "result": _json(result),
                "error": error,
                "next_attempt_at": next_attempt_at,
            })


# ---------------------------------------------------------------
# Durable broker position events
# ---------------------------------------------------------------

@_db_retry
async def insert_broker_position_event(event: dict[str, Any]) -> int | None:
    sql = text("""
        INSERT INTO broker_position_events (
            dedupe_key, account_id, event_type, position_ticket,
            position_identifier, symbol, position_type, magic,
            volume_before, volume_after, event_time, event_time_msc,
            close_deal_ticket, payload
        ) VALUES (
            :dedupe_key, :account_id, :event_type, :position_ticket,
            :position_identifier, :symbol, :position_type, :magic,
            :volume_before, :volume_after, :event_time, :event_time_msc,
            :close_deal_ticket, CAST(:payload AS JSONB)
        )
        ON CONFLICT (dedupe_key) DO NOTHING
        RETURNING id
    """)
    params = dict(event)
    params["payload"] = _json(params.get("payload", {}))
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            row = (await session.execute(sql, params)).first()
    return int(row[0]) if row else None


@_db_retry
async def query_broker_position_events(
    *,
    after_id: int = 0,
    account_id: int | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    clauses = ["id > :after_id"]
    params: dict[str, Any] = {"after_id": after_id, "limit": limit}
    if account_id is not None:
        clauses.append("account_id = :account_id")
        params["account_id"] = account_id
    sql = text(f"""
        SELECT id, account_id, event_type, position_ticket, position_identifier,
               symbol, position_type, magic, volume_before, volume_after,
               event_time, event_time_msc, close_deal_ticket, payload, observed_at
        FROM broker_position_events
        WHERE {' AND '.join(clauses)}
        ORDER BY id
        LIMIT :limit
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(row) for row in result.mappings().all()]
