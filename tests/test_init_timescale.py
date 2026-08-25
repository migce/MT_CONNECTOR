import pytest

from src.db.init_timescale import _trading_schema_statements


def test_trading_schema_statements_keep_comment_headed_ddl() -> None:
    sql = """
    CREATE TABLE old_table (id BIGINT PRIMARY KEY);
    -- 11. TRADE COMMANDS — durable execution
    CREATE TABLE IF NOT EXISTS broker_position_events (id BIGINT PRIMARY KEY);
    CREATE INDEX IF NOT EXISTS broker_position_events_id_idx
        ON broker_position_events (id);
    """

    assert _trading_schema_statements(sql) == [
        "CREATE TABLE IF NOT EXISTS broker_position_events (id BIGINT PRIMARY KEY)",
        "CREATE INDEX IF NOT EXISTS broker_position_events_id_idx\n"
        "        ON broker_position_events (id)",
    ]


def test_trading_schema_statements_require_marker() -> None:
    with pytest.raises(RuntimeError, match="marker is missing"):
        _trading_schema_statements("CREATE TABLE unrelated (id BIGINT);")
