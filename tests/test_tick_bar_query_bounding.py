from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from src.db import repository


class _Rows:
    def all(self) -> list[object]:
        return []


class _Session:
    def __init__(self) -> None:
        self.statement = None
        self.params = None
        self.calls: list[tuple[object, dict[str, object]]] = []

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, statement: object, params: dict[str, object]) -> _Rows:
        self.statement = statement
        self.params = params
        self.calls.append((statement, params))
        return _Rows()


class _Factory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dt_from", "dt_to", "source_order", "number_order"),
    [
        (None, None, "ORDER BY t.time_msc DESC", "ORDER BY t.time_msc DESC"),
        (
            None,
            datetime(2026, 8, 2, tzinfo=UTC),
            "ORDER BY t.time_msc DESC",
            "ORDER BY t.time_msc DESC",
        ),
        (
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            "ORDER BY t.time_msc ASC",
            "ORDER BY t.time_msc ASC",
        ),
    ],
)
async def test_tick_bar_query_bounds_source_before_windowing(
    dt_from: datetime | None,
    dt_to: datetime | None,
    source_order: str,
    number_order: str,
) -> None:
    session = _Session()
    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_tick_bars(
            symbol="EURUSD",
            tick_count=4_000,
            tf_label="T4000",
            dt_from=dt_from,
            dt_to=dt_to,
            limit=51,
            price_field="mid",
            include_incomplete=False,
        )

    assert rows == []
    assert session.params is not None
    assert session.params["source_limit"] == 204_000
    sql = str(session.statement)
    source_start = sql.index("WITH source_ticks AS")
    source_limit = sql.index("LIMIT :source_limit")
    numbered_start = sql.index("numbered AS")
    assert source_start < source_limit < numbered_start
    assert source_order in sql[source_start:numbered_start]
    assert number_order in sql[numbered_start:]
    assert "(t.bid + t.ask) / 2.0" in sql[source_start:numbered_start]


@pytest.mark.asyncio
async def test_tick_bar_to_only_cursor_keeps_latest_before_semantics() -> None:
    session = _Session()
    cursor = datetime(2026, 8, 2, tzinfo=UTC)
    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        await repository.query_tick_bars(
            symbol="EURUSD",
            tick_count=500,
            tf_label="T500",
            dt_to=cursor,
            limit=600,
            max_source_rows=300_000,
        )

    assert session.params is not None
    assert session.params["dt_to"] == cursor
    assert session.params["source_limit"] == 300_000
    sql = str(session.statement)
    assert "t.time_msc <= :dt_to" in sql
    assert "ORDER BY t.time_msc DESC" in sql
    assert "SELECT * FROM bars ORDER BY time ASC" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("price_field", ["bid", "ask", "last", "mid"])
@pytest.mark.parametrize("include_incomplete", [False, True])
async def test_tick_bar_query_keeps_price_and_incomplete_contract(
    price_field: str,
    include_incomplete: bool,
) -> None:
    session = _Session()
    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        await repository.query_tick_bars(
            symbol="XAUUSD",
            tick_count=500,
            tf_label="T500",
            limit=7,
            price_field=price_field,  # type: ignore[arg-type]
            include_incomplete=include_incomplete,
        )

    sql = str(session.statement)
    expected_price = "(t.bid + t.ask) / 2.0" if price_field == "mid" else f"t.{price_field}"
    assert expected_price in sql
    assert ("HAVING COUNT(*) = :tick_count" in sql) is (not include_incomplete)
    assert session.params is not None
    assert session.params["source_limit"] == 3_500


@pytest.mark.asyncio
async def test_tick_bar_query_rejects_unknown_price_field() -> None:
    with pytest.raises(ValueError, match="Unknown price_field"):
        await repository.query_tick_bars(
            symbol="EURUSD",
            tick_count=100,
            tf_label="T100",
            price_field="secret",  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_tick_bar_query_applies_source_cap_and_local_work_mem() -> None:
    session = _Session()
    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        await repository.query_tick_bars(
            symbol="EURUSD",
            tick_count=1_000,
            tf_label="T1000",
            limit=1_500,
            max_source_rows=300_000,
            work_mem_mb=32,
        )

    assert len(session.calls) == 2
    assert "SET LOCAL work_mem = '32MB'" in str(session.calls[0][0])
    assert session.calls[1][1]["source_limit"] == 300_000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dt_from", "expected_source_order"),
    [
        (None, "ORDER BY t.time_msc DESC"),
        (datetime(2026, 8, 1, tzinfo=UTC), "ORDER BY t.time_msc ASC"),
    ],
)
async def test_information_bar_tick_source_is_bounded_and_returned_ascending(
    dt_from: datetime | None,
    expected_source_order: str,
) -> None:
    session = _Session()
    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_information_bar_ticks(
            symbol="EURUSD",
            dt_from=dt_from,
            dt_to=datetime(2026, 8, 2, tzinfo=UTC),
            source_limit=123_456,
        )

    assert rows == []
    assert session.params is not None
    assert session.params["source_limit"] == 123_456
    sql = str(session.statement)
    assert expected_source_order in sql
    assert "LIMIT :source_limit" in sql
    assert "ORDER BY time_msc ASC" in sql or "ORDER BY t.time_msc ASC" in sql


@pytest.mark.asyncio
async def test_information_tick_query_applies_local_work_mem() -> None:
    session = _Session()
    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        await repository.query_information_bar_ticks(
            symbol="EURUSD",
            source_limit=300_000,
            work_mem_mb=32,
        )

    assert len(session.calls) == 2
    assert "SET LOCAL work_mem = '32MB'" in str(session.calls[0][0])
    assert session.calls[1][1]["source_limit"] == 300_000
