"""Stage 1, criterion 2: hard reset under load (kill -9 at random moments, a series of runs).

Every event committed to SQLite is processed, and every outbound task is delivered exactly
once or has a final status with a reason. Each run kills the process twice at random moments:
once while messages are flowing and answers are being sent, once during the recovery.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import AppFactory, write_config

ROOT = Path(__file__).resolve().parents[2]
COUNT = 120


def run_child(mode: str, args: list[str], kill_after: float | None, marker: str = "STARTED") -> tuple[int, str]:
    """Start a child; after it prints `marker`, kill it hard after `kill_after` seconds (None: let it finish)."""
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.Popen(
        [sys.executable, "-m", "tests.acceptance.crash_load_child", mode, *args],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8",
        errors="replace",
    )
    lines: list[str] = []
    started = threading.Event()

    def reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)
            if line.startswith(marker):
                started.set()

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    if not started.wait(180):
        proc.kill()
        raise AssertionError(f"child did not print {marker}:\n" + "".join(lines[-40:]))
    if kill_after is None:
        code = proc.wait(timeout=240)
    else:
        time.sleep(kill_after)
        proc.kill()  # SIGKILL on Linux, TerminateProcess on Windows: no cleanup at all
        code = proc.wait(timeout=30)
    thread.join(5)
    return code, "".join(lines)


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_hard_reset_under_load_loses_nothing_and_sends_everything_once(
    app_factory: AppFactory, tmp_path: Path, pg_dsn: str, config: dict[str, Any], seed: int
) -> None:
    rnd = random.Random(seed)
    config["uploader"]["batch_max"] = 9
    config_path = write_config(tmp_path / "config.yaml", config)
    sqlite_path, state = tmp_path / "buffer.sqlite", tmp_path / "telegram_state.jsonl"
    args = [str(config_path), str(sqlite_path), pg_dsn, str(state), str(COUNT)]

    # The load child is killed while messages are flowing and answers are being sent.
    await asyncio.to_thread(run_child, "load", args, rnd.uniform(0.3, 2.0), "FLOWING")
    await asyncio.to_thread(run_child, "recover", args, rnd.uniform(0.2, 2.5))
    code, out = await asyncio.to_thread(run_child, "recover", args, None)
    assert code == 0 and "IDLE" in out, out[-3000:]

    # Accepted = committed to SQLite. Every accepted event is in PostgreSQL and finished.
    conn = sqlite3.connect(sqlite_path)
    try:
        accepted, pending = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE uploaded_at IS NULL) FROM events").fetchone()
    finally:
        conn.close()
    assert pending == 0

    app = app_factory(cfg=config)  # inspect the same database
    await app.db.connect()
    pool = app.db.pool
    assert await pool.fetchval("SELECT count(*) FROM messages") == accepted
    unfinished = await pool.fetchval("SELECT count(*) FROM trace_inbound WHERE finished_at IS NULL")
    assert unfinished == 0
    statuses = {r["status"] for r in await pool.fetch("SELECT DISTINCT status FROM trace_inbound")}
    assert statuses <= {"processed", "skipped"}
    assert await pool.fetchval("SELECT count(*) FROM trace_module WHERE status <> 'processed'") == 0

    # Every accepted /ping got exactly one outbound task, and it is final.
    pings = await pool.fetchval("SELECT count(*) FROM messages WHERE text = '/ping' AND origin = 'external'")
    assert pings > 0
    tasks = await pool.fetch(
        "SELECT o.random_id, o.status, o.reason_code FROM outbound_tasks o JOIN trace_inbound t "
        "ON t.trace_id = o.parent_trace_id WHERE o.rule = 'ping'")
    assert len(tasks) == pings
    assert all(t["status"] == "delivered" or t["reason_code"] for t in tasks)
    assert await pool.fetchval("SELECT count(*) FROM outbound_tasks WHERE finished_at IS NULL") == 0

    # ...and Telegram received each delivered answer exactly once (no duplicates, no orphans).
    sends = [json.loads(line) for line in state.read_text(encoding="utf-8").splitlines()]
    pong_ids = [s["rid"] for s in sends if s["t"] == "send" and s["text"] == "pong"]
    assert len(pong_ids) == len(set(pong_ids))
    delivered = {t["random_id"] for t in tasks if t["status"] == "delivered"}
    assert set(pong_ids) == delivered
    await app.db.close()
