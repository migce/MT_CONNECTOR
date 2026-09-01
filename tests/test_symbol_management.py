"""Focused contracts for Connector-owned Symbol Management."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.api.routes.symbol_management import (
    BackfillJobCreate,
    CustomTimeframeCreate,
    CustomTimeframeBinding,
    ManagedSymbolUpdate,
    bind_timeframe,
    create_job,
    create_timeframe,
    update_symbol,
)
from src.config import custom_timeframe_source
from src.mt5.backfill import MT5HistoryCallTimeoutError, _await_history_call
from src.redis_bus.backfill_manager import BackfillListener


@pytest.mark.asyncio
async def test_history_timeout_returns_without_waiting_for_cancelled_worker() -> None:
    blocked = asyncio.Future()

    with pytest.raises(MT5HistoryCallTimeoutError):
        await _await_history_call(blocked, timeout=0.01)

    assert blocked.cancelled()


@pytest.mark.asyncio
async def test_listener_recycles_poller_after_publishing_history_timeout() -> None:
    backfiller = MagicMock()
    backfiller.on_demand_ticks = AsyncMock(
        side_effect=MT5HistoryCallTimeoutError("history IPC timeout")
    )
    recycle = MagicMock()
    listener = BackfillListener(
        backfiller,
        settings=MagicMock(),
        fatal_history_timeout=recycle,
    )
    listener._redis = MagicMock()
    listener._redis.delete = AsyncMock()
    listener._redis.publish = AsyncMock()
    now = datetime.now(UTC)

    await listener._handle_request({
        "request_id": "request-timeout",
        "symbol": "USTEC",
        "data_type": "ticks",
        "from": (now - timedelta(hours=1)).isoformat(),
        "to": now.isoformat(),
    })

    listener._redis.publish.assert_awaited_once()
    recycle.assert_called_once_with()


@pytest.mark.asyncio
async def test_tick_history_is_persisted_in_bounded_batches() -> None:
    from src.mt5.backfill import Backfiller

    settings = MagicMock()
    settings.backfill_tick_batch_rows = 2
    backfiller = Backfiller(MagicMock(), settings=settings)
    rows = [
        {
            "time_msc": datetime.fromtimestamp(index / 1000, tz=UTC),
            "symbol": "USTEC",
        }
        for index in range(5)
    ]
    progress: list[tuple[int, int]] = []

    async def report(batch, processed):
        progress.append((len(batch), processed))

    with patch(
        "src.mt5.backfill.repo.insert_ticks",
        new=AsyncMock(side_effect=lambda batch: len(batch)),
    ) as insert:
        affected = await backfiller._persist_tick_batches(
            rows,
            progress_callback=report,
        )

    assert affected == 5
    assert [len(call.args[0]) for call in insert.await_args_list] == [2, 2, 1]
    assert progress == [(2, 2), (2, 4), (1, 5)]


@pytest.mark.asyncio
async def test_cannot_enable_symbol_outside_broker_catalog() -> None:
    with (
        patch(
            "src.api.routes.symbol_management.validate_symbol",
            return_value="NOTREAL",
        ),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        pytest.raises(HTTPException) as exc_info,
    ):
        await update_symbol("NOTREAL", ManagedSymbolUpdate(active=True))

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_tick_timeframes_cannot_be_materialized() -> None:
    with (
        patch(
            "src.api.routes.symbol_management.validate_symbol",
            return_value="EURUSD",
        ),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        pytest.raises(HTTPException) as exc_info,
    ):
        await bind_timeframe(
            "EURUSD",
            "T500",
            CustomTimeframeBinding(mode="materialized", enabled=True),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Tick bars are virtual-only"


@pytest.mark.parametrize(
    ("timeframe", "source"),
    [
        ("M7", "M1"),
        ("M10", "M5"),
        ("M30", "M15"),
        ("H6", "H1"),
        ("H8", "H4"),
        ("D2", "D1"),
        ("W1", "D1"),
        ("T500", None),
    ],
)
def test_custom_timeframe_uses_coarsest_exact_source(
    timeframe: str,
    source: str | None,
) -> None:
    assert custom_timeframe_source(timeframe) == source


@pytest.mark.asyncio
async def test_custom_timeframe_type_must_match_code() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await create_timeframe(CustomTimeframeCreate(code="T500", kind="time"))

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Timeframe type does not match its code"


@pytest.mark.asyncio
async def test_custom_timeframe_response_exposes_source_contract() -> None:
    with patch(
        "src.api.routes.symbol_management.sm.upsert_custom_timeframe",
        new=AsyncMock(return_value={"code": "M10", "unit": "M", "value": 10}),
    ):
        result = await create_timeframe(CustomTimeframeCreate(code="M10", kind="time"))

    assert result["kind"] == "time"
    assert result["source_type"] == "candles"
    assert result["source_timeframe"] == "M5"


@pytest.mark.asyncio
async def test_custom_job_uses_same_exact_source_contract() -> None:
    now = datetime.now(UTC)
    requester = MagicMock()
    requester.enqueue_job = AsyncMock(return_value="request-custom")

    async def capture(values):
        return ({"id": "job-custom", **values}, True)

    with (
        patch("src.api.routes.symbol_management.validate_symbol", return_value="EURUSD"),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        patch(
            "src.api.routes.symbol_management.sm.create_job",
            new=AsyncMock(side_effect=capture),
        ),
        patch("src.api.app.get_backfill_requester", return_value=requester),
    ):
        result = await create_job(BackfillJobCreate(
            symbol="EURUSD",
            target_type="custom",
            timeframe="M10",
            mode="fill_missing",
            **{"from": now - timedelta(days=1), "to": now - timedelta(hours=1)},
        ))

    assert result["source_type"] == "candles"
    assert result["source_timeframe"] == "M5"


@pytest.mark.asyncio
async def test_custom_candle_query_uses_same_exact_source_contract() -> None:
    from src.api.routes.custom_candles import get_custom_candles

    with (
        patch("src.api.routes.custom_candles.validate_symbol", return_value="EURUSD"),
        patch(
            "src.api.routes.custom_candles.maybe_backfill_candles",
            new=AsyncMock(return_value=[]),
        ) as backfill,
        patch(
            "src.api.routes.custom_candles.repo.query_custom_tf_candles",
            new=AsyncMock(return_value=[]),
        ) as query,
    ):
        result = await get_custom_candles(
            symbol="EURUSD",
            timeframe="M10",
            from_dt=None,
            to_dt=None,
            limit=100,
            bars=None,
            price="bid",
            include_incomplete=False,
        )

    assert result.count == 0
    assert backfill.await_args.kwargs["timeframe"] == "M5"
    assert query.await_args.kwargs["source_tf"] == "M5"


@pytest.mark.asyncio
async def test_job_create_enqueues_persistent_job() -> None:
    now = datetime.now(UTC)
    stored = {
        "id": "job-1",
        "symbol": "EURUSD",
        "target_type": "candles",
        "timeframe": "M1",
        "source_type": "candles",
        "source_timeframe": "M1",
        "mode": "fill_missing",
        "range_from": now - timedelta(days=1),
        "range_to": now - timedelta(hours=1),
    }
    requester = MagicMock()
    requester.enqueue_job = AsyncMock(return_value="request-1")

    with (
        patch(
            "src.api.routes.symbol_management.validate_symbol",
            return_value="EURUSD",
        ),
        patch(
            "src.api.routes.symbol_management.get_all_mt5_symbols",
            return_value={"EURUSD": "Euro vs US Dollar"},
        ),
        patch(
            "src.api.routes.symbol_management.sm.create_job",
            new=AsyncMock(return_value=(stored, True)),
        ),
        patch(
            "src.api.app.get_backfill_requester",
            return_value=requester,
        ),
    ):
        result = await create_job(BackfillJobCreate(
            symbol="EURUSD",
            target_type="candles",
            timeframe="M1",
            mode="fill_missing",
            **{"from": stored["range_from"], "to": stored["range_to"]},
        ))

    assert result["deduplicated"] is False
    requester.enqueue_job.assert_awaited_once_with(stored)


@pytest.mark.asyncio
async def test_listener_marks_short_broker_result_partial() -> None:
    now = datetime.now(UTC)
    backfiller = MagicMock()

    async def short_result(*_args, progress_callback=None, **_kwargs):
        assert progress_callback is not None
        await progress_callback(now - timedelta(hours=12), 120)
        return 120

    backfiller.on_demand_candles = AsyncMock(side_effect=short_result)
    listener = BackfillListener(backfiller, settings=MagicMock())
    listener._redis = MagicMock()
    listener._redis.delete = AsyncMock()
    listener._redis.publish = AsyncMock()
    updates: list[dict] = []

    async def update_job(_job_id, **changes):
        updates.append(changes)
        return changes

    with (
        patch(
            "src.db.symbol_management.get_job",
            new=AsyncMock(return_value={"id": "job-1", "status": "queued"}),
        ),
        patch(
            "src.db.symbol_management.update_job",
            new=update_job,
        ),
    ):
        await listener._handle_request({
            "request_id": "request-1",
            "job_id": "job-1",
            "symbol": "EURUSD",
            "data_type": "candles",
            "timeframe": "H1",
            "target_type": "candles",
            "target_timeframe": "H1",
            "mode": "fill_missing",
            "from": (now - timedelta(days=1)).isoformat(),
            "to": now.isoformat(),
        })

    terminal = updates[-1]
    assert terminal["status"] == "partial"
    assert terminal["covered_to"] == now - timedelta(hours=12)
    assert terminal["error"] == "Broker returned only part of the requested range"


@pytest.mark.asyncio
async def test_tick_job_reports_scanned_progress_and_downloaded_rows() -> None:
    now = datetime.now(UTC)
    start = now - timedelta(days=1)
    midpoint = start + timedelta(hours=12)
    backfiller = MagicMock()

    async def ticking_result(
        *_args,
        progress_callback=None,
        scan_progress_callback=None,
        **_kwargs,
    ):
        assert progress_callback is not None
        assert scan_progress_callback is not None
        await progress_callback(midpoint, 250)
        await scan_progress_callback(midpoint, 250)
        await progress_callback(now, 500)
        await scan_progress_callback(now, 500)
        return 480

    backfiller.on_demand_ticks = AsyncMock(side_effect=ticking_result)
    listener = BackfillListener(backfiller, settings=MagicMock())
    listener._redis = MagicMock()
    listener._redis.delete = AsyncMock()
    listener._redis.publish = AsyncMock()
    updates: list[dict] = []

    async def update_job(_job_id, **changes):
        updates.append(changes)
        return changes

    with (
        patch(
            "src.db.symbol_management.get_job",
            new=AsyncMock(return_value={"id": "job-ticks", "status": "queued"}),
        ),
        patch("src.db.symbol_management.update_job", new=update_job),
    ):
        await listener._handle_request({
            "request_id": "request-ticks",
            "job_id": "job-ticks",
            "symbol": "USTEC",
            "data_type": "ticks",
            "target_type": "ticks",
            "mode": "fill_missing",
            "from": start.isoformat(),
            "to": now.isoformat(),
        })

    progress_updates = [item for item in updates if "progress" in item and item.get("status") is None]
    assert any(float(item["progress"]) >= 0.49 for item in progress_updates)
    terminal = updates[-1]
    assert terminal["status"] == "succeeded"
    assert terminal["rows_read"] == 500
    assert terminal["rows_written"] == 480


@pytest.mark.asyncio
async def test_listener_ignores_duplicate_nonqueued_job() -> None:
    listener = BackfillListener(MagicMock(), settings=MagicMock())
    listener._redis = MagicMock()
    with patch(
        "src.db.symbol_management.get_job",
        new=AsyncMock(return_value={"id": "job-1", "status": "running"}),
    ):
        await listener._handle_request({
            "request_id": "request-duplicate",
            "job_id": "job-1",
            "symbol": "EURUSD",
            "data_type": "candles",
            "timeframe": "M1",
            "from": datetime.now(UTC).isoformat(),
            "to": datetime.now(UTC).isoformat(),
        })

    listener._backfiller.on_demand_candles.assert_not_called()


@pytest.mark.asyncio
async def test_recovered_fill_job_resumes_after_last_committed_tick() -> None:
    now = datetime.now(UTC)
    start = now - timedelta(days=2)
    covered = now - timedelta(days=1)
    backfiller = MagicMock()
    backfiller.on_demand_ticks = AsyncMock(return_value=0)
    listener = BackfillListener(backfiller, settings=MagicMock())
    listener._redis = MagicMock()
    listener._redis.delete = AsyncMock()
    listener._redis.publish = AsyncMock()

    with (
        patch(
            "src.db.symbol_management.get_job",
            new=AsyncMock(return_value={
                "id": "job-resume",
                "status": "queued",
                "covered_to": covered,
                "rows_read": 12_000,
                "progress": 0.5,
            }),
        ),
        patch("src.db.symbol_management.update_job", new=AsyncMock()),
    ):
        await listener._handle_request({
            "request_id": "request-resume",
            "job_id": "job-resume",
            "symbol": "USTEC",
            "data_type": "ticks",
            "target_type": "ticks",
            "mode": "fill_missing",
            "from": start.isoformat(),
            "to": now.isoformat(),
        })

    assert backfiller.on_demand_ticks.await_args.args[1] == (
        covered + timedelta(milliseconds=1)
    )


@pytest.mark.asyncio
async def test_recovery_preserves_committed_progress() -> None:
    from src.db import symbol_management as sm

    session = AsyncMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=1))
    transaction = AsyncMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=None)
    session.begin = MagicMock(return_value=transaction)
    session_context = AsyncMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=None)
    factory = MagicMock(return_value=session_context)

    with patch("src.db.symbol_management.get_session_factory", return_value=factory):
        assert await sm.recover_interrupted_jobs() == 1

    statement = str(session.execute.await_args.args[0])
    assert "progress=" not in statement.replace(" ", "")
