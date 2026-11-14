"""Criterion 4 of stage 1a: kill -9 at exact points; everything accepted is processed exactly once."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import KASSA, KASSA2, AppFactory, write_config

ROOT = Path(__file__).resolve().parents[2]
COUNT = 60


def run_child(config: Path, sqlite: Path, dsn: str, point: str, after: int) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "AUTOCASH_CRASHPOINT": point, "AUTOCASH_CRASHPOINT_AFTER": str(after),
           "PYTHONIOENCODING": "utf-8"}
    return subprocess.run(
        [sys.executable, "-m", "tests.acceptance.crash_child", str(config), str(sqlite), dsn, str(COUNT)],
        cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=180,
    )


@pytest.mark.parametrize(("point", "after"), [
    ("uploader_before_pg_commit", 1),  # committed to SQLite, the PostgreSQL transaction is lost
    ("uploader_before_pg_commit", 3),
    ("uploader_after_pg_commit", 2),   # committed to PostgreSQL, the SQLite mark is lost
    ("module_before_commit", 5),       # sorted, the module result is not committed
    ("module_after_commit", 5),        # the module result is committed, the worker died right after
])
async def test_kill_9_everything_accepted_is_processed_exactly_once(
    app_factory: AppFactory, tmp_path: Path, pg_dsn: str, config: dict[str, Any], point: str, after: int
) -> None:
    config["uploader"]["batch_max"] = 7  # several batches, so the crash lands in the middle
    config_path = write_config(tmp_path / "config.yaml", config)
    sqlite_path = tmp_path / "buffer.sqlite"

    child = await asyncio.to_thread(run_child, config_path, sqlite_path, pg_dsn, point, after)
    assert f"ACCEPTED {COUNT}" in child.stdout, child.stderr[-2000:]
    assert child.returncode == 137 and "IDLE" not in child.stdout, (child.returncode, child.stderr[-2000:])

    app = app_factory(cfg=config)  # the same SQLite file and the same database
    await app.start()
    await app.ready.wait()
    await app.idle(timeout=60)

    pool = app.db.pool
    assert await pool.fetchval("SELECT count(*) FROM messages") == COUNT
    assert await pool.fetchval("SELECT count(DISTINCT (chat_id, msg_id)) FROM messages") == COUNT
    rows = await pool.fetch("SELECT status, msg_type, count(*) AS n FROM trace_inbound GROUP BY 1, 2")
    assert [(r["status"], r["msg_type"], r["n"]) for r in rows] == [("processed", "deposit.request", COUNT)]
    rows = await pool.fetch("SELECT status, count(*) AS n FROM trace_module GROUP BY 1")
    assert [(r["status"], r["n"]) for r in rows] == [("processed", COUNT)]
    # Exactly once: the module's counter was incremented once per message, not per attempt.
    counters = await pool.fetch(
        "SELECT entity_key, (attrs->>'count')::int AS n FROM entities WHERE entity_type = 'demo.counter'")
    assert {r["entity_key"]: r["n"] for r in counters} == {str(KASSA): COUNT // 2, str(KASSA2): COUNT // 2}
    # Order of arrival is kept inside each chat.
    for chat in (KASSA, KASSA2):
        ids = [r["msg_id"] for r in await pool.fetch(
            "SELECT msg_id FROM messages WHERE chat_id = $1 ORDER BY ingest_id", chat)]
        assert ids == sorted(ids)
    stats = await app.buffer.stats()
    assert stats["pending"] == 0 and stats["total"] == COUNT
