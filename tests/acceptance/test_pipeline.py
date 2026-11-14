"""Criteria 1, 2, 7 of stage 1a: end-to-end flow, latency, versions, unknown."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import asyncpg

from autocash.core.app import App
from tests.conftest import ACCOUNT, BOT, CASHER, DEPOSIT_TEXT, KASSA, KASSA2, PLAYER
from tests.fake_telegram.server import FakeTelegram

# The stage prompt does not name a rate for criterion 1. 50 msg/s is ~40 times the load of NFT-4
# after tenfold growth (100k messages a day ~ 1.2 msg/s); see docs/decisions.md, D-1a-5.
LIVE_RATE_PER_S = 50


async def fetch(app: App, sql: str, *args: Any) -> list[asyncpg.Record]:
    rows: list[asyncpg.Record] = await app.db.pool.fetch(sql, *args)
    return rows


async def test_message_goes_through_the_whole_chain(app: App, fake: FakeTelegram) -> None:
    fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=42))
    await app.idle()

    (msg,) = await fetch(app, "SELECT * FROM messages")
    assert msg["msg_type"] == "deposit.request"
    assert json.loads(msg["fields"]) == {"deposit_id": "42", "mel_id": "1481598133", "tgid": "7287295969"}
    assert msg["order_key"] == "deposit_id=42"
    assert "tel:" not in msg["text"]
    assert msg["version"] == 0 and msg["scope_id"] == 0 and msg["origin"] == "external"

    (trace,) = await fetch(app, "SELECT * FROM trace_inbound")
    assert trace["status"] == "processed" and trace["config_version"] == app.store.current.version
    for stage in ("accepted_at", "extracted_at", "buffered_at", "in_pg_at", "sorted_at", "finished_at"):
        assert trace[stage] is not None, stage
    (mod,) = await fetch(app, "SELECT * FROM trace_module")
    assert mod["module"] == "demo" and mod["status"] == "processed"
    assert mod["queued_at"] and mod["started_at"] and mod["finished_at"]
    (seen,) = await fetch(app, "SELECT * FROM message_seen")
    assert seen["account_id"] == ACCOUNT
    counter = await app.db.pool.fetchval(
        "SELECT attrs->>'count' FROM entities WHERE entity_type = 'demo.counter' AND entity_key = $1", str(KASSA)
    )
    assert counter == "1"


async def test_latency_p95_under_one_second_on_1000_messages(app: App, fake: FakeTelegram) -> None:
    """Criterion 1: accepted -> start of module processing, p95 <= 1 s (NFT-6)."""
    total = 1000
    started = time.perf_counter()
    posted = 0
    while posted < total:
        due = min(total, int((time.perf_counter() - started) * LIVE_RATE_PER_S) + 1)
        while posted < due:
            fake.post(KASSA if posted % 2 else KASSA2, BOT, DEPOSIT_TEXT.format(deposit_id=posted))
            posted += 1
        await asyncio.sleep(0.005)
    await app.idle(timeout=60)

    rows = await fetch(
        app,
        "SELECT extract(epoch FROM m.started_at - t.accepted_at) AS lag, t.status, t.msg_type "
        "FROM trace_inbound t JOIN trace_module m USING (trace_id)",
    )
    assert len(rows) == total
    assert all(r["status"] == "processed" and r["msg_type"] == "deposit.request" for r in rows)
    lags = sorted(float(r["lag"]) for r in rows)
    p95 = lags[int(total * 0.95) - 1]
    print(f"latency: p50={lags[total // 2]:.3f}s p95={p95:.3f}s max={lags[-1]:.3f}s")
    assert p95 <= 1.0
    total_count = await app.db.pool.fetchval(
        "SELECT sum((attrs->>'count')::int) FROM entities WHERE entity_type = 'demo.counter'"
    )
    assert total_count == total


async def test_edit_creates_version_and_delete_is_recorded(app: App, fake: FakeTelegram) -> None:
    """Criterion 2."""
    msg = fake.post(KASSA, BOT, "note one")
    await app.idle()
    fake.edit(msg, "note two")
    await app.idle()
    fake.edit(msg, "note two")  # the text did not change: no new version (FT-5.2)
    await app.idle()

    rows = await fetch(app, "SELECT version, event, text, msg_type FROM messages ORDER BY ingest_id")
    assert [(r["event"], r["text"]) for r in rows] == [("new", "note one"), ("edit", "note two")]
    assert rows[0]["version"] == 0 and rows[1]["version"] > 0
    assert all(r["msg_type"] == "test.note" for r in rows)

    fake.delete(msg)
    await app.idle()
    assert await app.db.pool.fetchval("SELECT count(*) FROM message_deletions") == 1
    assert await app.db.pool.fetchval("SELECT count(*) FROM messages WHERE deleted_at IS NOT NULL") == 2
    statuses = await fetch(app, "SELECT event, status FROM trace_inbound ORDER BY accepted_at")
    assert [(r["event"], r["status"]) for r in statuses] == [
        ("new", "processed"), ("edit", "processed"), ("delete", "processed")]


async def test_repeated_upload_of_the_same_batch_changes_nothing(app: App, fake: FakeTelegram) -> None:
    """Criterion 2: upsert by (account, chat_id, msg_id, version) is idempotent (FT-4.4)."""
    for i in range(20):
        fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=i))
    await app.idle()
    snapshot = "SELECT ingest_id, trace_id, msg_type, sorted_at FROM messages ORDER BY ingest_id"
    before = await fetch(app, snapshot)
    traces_before = await fetch(app, "SELECT * FROM trace_inbound ORDER BY trace_id")
    modules_before = await fetch(app, "SELECT * FROM trace_module ORDER BY trace_id")

    # Simulate "PostgreSQL committed, SQLite mark was lost": everything is uploaded once more.
    await asyncio.to_thread(_unmark, str(app.buffer.path))
    app.buffer.pending_event.set()
    await asyncio.sleep(0.3)
    await app.idle()

    assert await fetch(app, snapshot) == before
    assert await fetch(app, "SELECT * FROM trace_inbound ORDER BY trace_id") == traces_before
    assert await fetch(app, "SELECT * FROM trace_module ORDER BY trace_id") == modules_before
    assert await app.db.pool.fetchval("SELECT count(*) FROM message_seen") == 20
    assert await app.db.pool.fetchval(
        "SELECT attrs->>'count' FROM entities WHERE entity_key = $1", str(KASSA)) == "20"


def _unmark(path: str) -> None:
    import sqlite3

    conn = sqlite3.connect(path)
    conn.execute("UPDATE events SET uploaded_at = NULL")
    conn.commit()
    conn.close()


async def test_unknown_gets_reason_and_is_counted(app: App, fake: FakeTelegram) -> None:
    """Criterion 7."""
    fake.post(KASSA, PLAYER, "просто текст без правила")
    fake.post(KASSA, PLAYER, "ещё один")
    fake.post(KASSA2, PLAYER, "и в другой кассе")
    await app.idle()

    rows = await fetch(app, "SELECT msg_type, status, reason_code, finished_at FROM trace_inbound")
    assert len(rows) == 3
    assert all(r["msg_type"] == "unknown" and r["status"] == "skipped" and r["reason_code"] == "UNKNOWN_TYPE"
               and r["finished_at"] is not None for r in rows)
    assert app.sorter.unknown[KASSA] == 2 and app.sorter.unknown[KASSA2] == 1
    counters = await fetch(app, "SELECT chat_id, count FROM unknown_counters ORDER BY chat_id DESC")
    assert [(r["chat_id"], r["count"]) for r in counters] == [(KASSA, 2), (KASSA2, 1)]
    assert await app.db.pool.fetchval("SELECT count(*) FROM trace_module") == 0  # not delivered to modules
    summary = app.sorter.unknown_summary()
    assert summary is not None and str(KASSA) in summary
    app.alerts.debug(summary)
    await app.idle()
    from tests.conftest import DEBUG

    assert any(chat == DEBUG and "unknown" in text for _, chat, text, _ in fake.sent)


async def test_ping_command_is_answered_once_and_own_reply_is_system(app: App, fake: FakeTelegram) -> None:
    msg = fake.post(KASSA, CASHER, "/ping")
    await app.idle()

    assert [(chat, text) for _, chat, text, _ in fake.sent] == [(KASSA, "pong")]
    (task,) = await fetch(app, "SELECT * FROM outbound_tasks")
    assert task["status"] == "delivered" and task["stage"] == "delivered" and task["delivered_msg_id"]
    assert task["reply_to"] == msg.id_for(ACCOUNT) and task["random_id"] and task["kind"] == "command_reply"
    (attempt,) = await fetch(app, "SELECT * FROM outbound_attempts")
    assert attempt["result"] == "delivered" and attempt["account_id"] == ACCOUNT
    rows = await fetch(app, "SELECT text, origin, msg_type, is_command FROM messages ORDER BY ingest_id")
    assert [(r["text"], r["origin"], r["msg_type"]) for r in rows] == [
        ("/ping", "external", "command.ping"), ("pong", "system", "unknown")]
    assert rows[0]["is_command"]
