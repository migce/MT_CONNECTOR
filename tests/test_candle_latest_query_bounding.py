from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.db import repository


class _Rows:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = [SimpleNamespace(_mapping=row) for row in rows]

    def all(self) -> list[SimpleNamespace]:
        return self._rows


class _Session:
    def __init__(self, responses: list[list[dict[str, object]]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, statement: object, params: dict[str, object]) -> _Rows:
        self.calls.append((str(statement), dict(params)))
        return _Rows(self.responses.pop(0))


class _Factory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


def _row(hour: int) -> dict[str, object]:
    return {"time": datetime(2026, 8, 22, hour, tzinfo=UTC)}


@pytest.mark.asyncio
async def test_latest_candles_use_recent_window_when_it_is_complete() -> None:
    expected = [_row(1), _row(2), _row(3)]
    session = _Session([expected])

    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_candles("EURUSD", "H1", limit=3)

    assert rows == expected
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "time >= :recent_from" in sql
    assert params["recent_from"] <= datetime.now(UTC)


@pytest.mark.asyncio
async def test_latest_candles_fall_back_when_recent_window_is_sparse() -> None:
    recent = [_row(3)]
    expected = [_row(1), _row(2), _row(3)]
    session = _Session([recent, expected])

    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_candles("EURUSD", "H1", limit=3)

    assert rows == expected
    assert len(session.calls) == 2
    assert "time >= :recent_from" in session.calls[0][0]
    assert "time >= :recent_from" not in session.calls[1][0]
    assert "recent_from" not in session.calls[1][1]


@pytest.mark.asyncio
async def test_explicit_range_keeps_single_forward_query() -> None:
    expected = [_row(1), _row(2)]
    session = _Session([expected])

    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_candles(
            "EURUSD",
            "H1",
            dt_from=datetime(2026, 8, 22, tzinfo=UTC),
            limit=3,
        )

    assert rows == expected
    assert len(session.calls) == 1
    assert "ORDER BY time ASC LIMIT :limit" in session.calls[0][0]
    assert "recent_from" not in session.calls[0][1]


@pytest.mark.asyncio
async def test_latest_custom_candles_use_recent_source_window() -> None:
    expected = [_row(1), _row(2), _row(3)]
    session = _Session([expected])

    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_custom_tf_candles(
            "EURUSD",
            bucket_seconds=21_600,
            tf_label="H6",
            limit=3,
            source_tf="H1",
        )

    assert rows == expected
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "c.time >= time_bucket" in sql
    assert params["recent_from"] <= datetime.now(UTC)


@pytest.mark.asyncio
async def test_latest_custom_candles_fall_back_when_recent_window_is_sparse() -> None:
    recent = [_row(3)]
    expected = [_row(1), _row(2), _row(3)]
    session = _Session([recent, expected])

    with patch.object(repository, "get_session_factory", return_value=_Factory(session)):
        rows = await repository.query_custom_tf_candles(
            "EURUSD",
            bucket_seconds=21_600,
            tf_label="H6",
            limit=3,
            source_tf="H1",
        )

    assert rows == expected
    assert len(session.calls) == 2
    assert "c.time >= time_bucket" in session.calls[0][0]
    assert "c.time >= time_bucket" not in session.calls[1][0]
    assert "recent_from" not in session.calls[1][1]
