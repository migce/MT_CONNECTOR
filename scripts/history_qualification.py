"""Explicit one-shot Windows qualification of ONLY the dedicated terminal.

Output contains booleans, version and a bar count, never account secrets or
market/order data. It neither consumes jobs nor touches live/trading terminals.
"""
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


async def probe():
    from src.history_main import HistoryService
    from src.mt5.history_limits import apply_history_memory_limit
    memory_fence = apply_history_memory_limit()
    from src.config import get_settings
    from src.mt5.history_connection import HistoryConnection
    from src.mt5.connection import run_in_mt5
    import MetaTrader5 as mt5

    s = get_settings()
    from src.db.engine import get_engine, dispose_engine
    from src.redis_bus.pool import get_redis_pool, close_redis_pool
    from sqlalchemy import text
    engine = get_engine(s.model_copy(update={"db_pool_min": 2, "db_pool_max": 3}))
    async with engine.connect() as db:
        await db.execute(text("SET TRANSACTION READ ONLY"))
        assert (await db.execute(text("SELECT 1"))).scalar() == 1
        await db.rollback()
    await get_redis_pool().ping()
    connection = HistoryConnection(s)
    try:
        ok = await run_in_mt5(connection._try_connect)
    except RuntimeError:
        return {"error_type": "identity_or_permission_proof", "proof": getattr(connection, "last_proof", {})}
    if not ok:
        raise RuntimeError("History initialization unavailable")
    info = await run_in_mt5(mt5.terminal_info)
    account = await run_in_mt5(mt5.account_info)
    await connection.select_symbols(["EURUSD"])
    bars = await run_in_mt5(mt5.copy_rates_from_pos, "EURUSD", mt5.TIMEFRAME_M1, 1, 10)
    result = dict(connected=bool(info.connected), data_path=info.data_path,
                  build=info.build, trade_allowed=bool(info.trade_allowed),
                  tradeapi_disabled=bool(info.tradeapi_disabled),
                  login_matches=account.login == s.mt5_login,
                  process_memory_limit_mib=1024, database_read=True, redis_ping=True,
                  bars_read=0 if bars is None else len(bars))
    await connection.shutdown()
    await close_redis_pool()
    await dispose_engine()
    return result


if __name__ == "__main__":
    import os
    try:
        outcome = asyncio.run(asyncio.wait_for(probe(), timeout=90))
        (ROOT / "history-proof.json").write_text(json.dumps(outcome), encoding="utf-8")
        os._exit(0)
    except BaseException as exc:
        (ROOT / "history-proof.json").write_text(json.dumps({"error_type": type(exc).__name__}), encoding="utf-8")
        os._exit(1)
