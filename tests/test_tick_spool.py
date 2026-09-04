from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.mt5 import collector as module
from src.mt5.collector import Collector
from src.mt5.tick_spool import TickSpool, TickSpoolFull


def tick(offset=0):
    return {"symbol": "TEST", "time_msc": datetime(2026, 9, 5, tzinfo=UTC) + timedelta(milliseconds=offset),
            "bid": 1.1, "ask": 1.2, "last": 0, "volume": 1, "flags": 0}


def test_unacknowledged_ticks_survive_reopen_and_new_arrivals(tmp_path):
    path = str(tmp_path / "ticks.sqlite3")
    spool = TickSpool(path)
    spool.append(tick())
    assert spool.append(tick()) == 1
    last, rows = spool.peek()
    assert rows == [tick()]
    spool.append(tick(1))
    spool.close()
    spool = TickSpool(path)
    assert spool.stats()["pending"] == 2
    assert spool.acknowledge(last) == 1
    assert spool.peek()[1] == [tick(1)]
    spool.close()


def test_capacity_failure_never_evicts_accepted_ticks(tmp_path):
    spool = TickSpool(str(tmp_path / "ticks.sqlite3"), max_bytes=150)
    spool.append(tick())
    with pytest.raises(TickSpoolFull):
        spool.append(tick(1))
    assert spool.stats()["pending"] == 1
    assert spool.peek()[1] == [tick()]
    spool.close()


def test_committed_spool_survives_abrupt_writer_exit(tmp_path):
    path = str(tmp_path / "crash.sqlite3")
    code = (
        "import os,sys; from src.mt5.tick_spool import TickSpool; "
        "from tests.test_tick_spool import tick; "
        "spool=TickSpool(sys.argv[1]); spool.append(tick()); os._exit(17)"
    )
    result = subprocess.run([sys.executable, "-c", code, path], env=os.environ.copy(),
                            capture_output=True, timeout=5)
    assert result.returncode == 17
    spool = TickSpool(path)
    assert spool.stats()["pending"] == 1
    assert spool.peek()[1] == [tick()]
    spool.close()


@pytest.mark.parametrize("phase,error", [
    ("insert", ConnectionError("DB failed")),
    ("insert", asyncio.CancelledError()),
    ("watermark", ConnectionError("watermark failed")),
])
async def test_flush_only_acknowledges_complete_committed_batch(tmp_path, monkeypatch, phase, error):
    spool = TickSpool(str(tmp_path / "ticks.sqlite3"))
    spool.append(tick())
    collector = Collector.__new__(Collector)
    collector._spool = spool
    collector._metrics = SimpleNamespace(set_tick_buffer_depth=lambda _n: None,
                                        record_ticks_flushed=lambda *_args: None)
    insert = AsyncMock(return_value=1)
    watermark = AsyncMock()
    (insert if phase == "insert" else watermark).side_effect = error
    monkeypatch.setattr(module.repo, "insert_ticks", insert)
    monkeypatch.setattr(module.repo, "update_sync_state", watermark)
    with pytest.raises(type(error)):
        await collector._flush_tick_buffer()
    assert spool.stats()["pending"] == 1
    insert.side_effect = watermark.side_effect = None
    await collector._flush_tick_buffer()
    assert spool.stats()["pending"] == 0
    spool.close()
