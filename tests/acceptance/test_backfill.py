"""Stage 4: loading the history of chats (criteria 1-5)."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest

from autocash.core.app import App
from autocash.modules.api import Context, Event, Module
from autocash.modules.demo.module import DemoModule
from tests.conftest import ACCOUNT, BOT, DEBUG, KASSA, KASSA2, LOGS, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001


def history(fake: FakeTelegram, chat: int, count: int, text: str = "note {i}", minutes_ago: float = 600) -> None:
    """Messages written before the pool was online: they exist only as history."""
    start = datetime.now(UTC) - timedelta(minutes=minutes_ago)
    for i in range(count):
        fake.post(chat, BOT, text.format(i=i), date=start + timedelta(seconds=i))


async def loaded(app: App) -> int:
    n: int = await app.db.pool.fetchval("SELECT count(*) FROM messages WHERE source = 'backfill'")
    return n


async def done(app: App) -> bool:
    return bool(await app.db.pool.fetchval(
        "SELECT bool_and(status = 'done') AND count(*) > 0 FROM backfill_jobs"))


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"]}
    config["backfill"] = {"batch": 50, "interval_s": 0.05}
    return config


# ---- criterion 2: history gets no outbound tasks --------------------------------------------------------
async def test_history_creates_no_outbound_tasks(app_factory: AppFactory, fake: FakeTelegram,
                                                 cfg: dict[str, Any]) -> None:
    for _ in range(5):
        fake.post(KASSA, PLAYER, "/ping", date=datetime.now(UTC) - timedelta(hours=1))
    history(fake, KASSA, 30)
    app = await started(app_factory(cfg=cfg, modules=[DemoModule()]))
    assert await app.backfill.begin([KASSA]) == 1
    await wait_for(lambda: done(app), timeout=60, what="history loaded")
    await app.idle()
    assert await loaded(app) == 35
    assert await app.db.pool.fetchval("SELECT count(*) FROM outbound_tasks") == 0
    assert sent_by(fake, KASSA) == []
    skipped = await app.db.pool.fetchval("SELECT count(*) FROM rule_log WHERE rule = 'backfill.external_skipped'")
    assert skipped == 5
    # The demo counter (an internal action) did count the history.
    assert await app.db.pool.fetchval(
        "SELECT (attrs->>'count')::int FROM entities WHERE entity_key = $1", str(KASSA)) == 35
    # A live /ping is still answered.
    fake.post(KASSA, PLAYER, "/ping")
    await wait_for(lambda: sent_by(fake, KASSA) == [(ACCOUNT, "pong")], what="live answer")


# ---- criterion 3: the live event of a key waits for that key's history ------------------------------------
class Recorder(Module):
    name: ClassVar[str] = "recorder"
    subscriptions: ClassVar[list[str]] = ["test.keyed"]
    concurrency: ClassVar[int] = 4
    seen: list[tuple[str, int, str]]

    async def handle(self, event: Event, ctx: Context) -> None:
        f = event.fields
        self.seen.append((str(f.get("key")), int(f.get("n") or 0), event.record.source))
        await asyncio.sleep(0.01)


async def test_live_event_of_a_key_is_processed_after_its_history(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    cfg["queue"] = {"backfill_rate_per_s": 20, "live_threshold": 100}  # the history crawls
    start = datetime.now(UTC) - timedelta(hours=2)
    for i in range(60):
        fake.post(KASSA, BOT, f"key: {'A' if i % 2 else 'B'}\nn: {i}", date=start + timedelta(seconds=i))
    rec = Recorder()
    rec.seen = []
    app = await started(app_factory(cfg=cfg, modules=[rec]))
    await app.backfill.begin([KASSA])
    await wait_for(lambda: len(rec.seen) >= 4, timeout=60, what="history started")
    fake.post(KASSA, BOT, "key: A\nn: 1000")  # live, while A still has history in the queue
    fake.post(KASSA, BOT, "key: C\nn: 2000")  # live, a key without history: not held back
    await wait_for(lambda: len(rec.seen) == 62, timeout=120, what="everything processed")
    a = [n for key, n, _ in rec.seen if key == "A"]
    assert a == sorted(a) and a[-1] == 1000  # the live event of A came after all of A's history
    order = [n for _, n, _ in rec.seen]
    assert order.index(2000) < order.index(1000)  # C did not wait for A


# ---- criterion 4: a restart in the middle of loading -------------------------------------------------------
async def test_restart_in_the_middle_continues_without_duplicates(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    cfg["backfill"] = {"batch": 20, "interval_s": 0.3}
    history(fake, KASSA, 150)
    history(fake, KASSA2, 40)
    app = await started(app_factory(cfg=cfg, modules=[DemoModule()]))
    await app.backfill.begin([KASSA, KASSA2])
    await wait_for(lambda: app.backfill.batches >= 3, timeout=60, what="some batches")
    await app.stop()
    first = await _loaded(app_factory, cfg)
    assert 0 < first < 190  # stopped in the middle

    app2 = await started(app_factory(cfg=cfg, modules=[DemoModule()]))
    await wait_for(lambda: done(app2), timeout=120, what="history finished after the restart")
    await app2.idle()
    pool = app2.db.pool
    assert await pool.fetchval("SELECT count(*) FROM messages WHERE source = 'backfill'") == 190
    assert await pool.fetchval("SELECT count(DISTINCT (chat_id, msg_id)) FROM messages") == 190
    assert await pool.fetchval("SELECT sum(loaded) FROM backfill_jobs") >= 190
    counters = await pool.fetch("SELECT entity_key, (attrs->>'count')::int AS n FROM entities")
    assert {r["entity_key"]: r["n"] for r in counters} == {str(KASSA): 150, str(KASSA2): 40}
    assert "done 2" in await app2.commands.backfill(["status"])


async def _loaded(app_factory: AppFactory, cfg: dict[str, Any]) -> int:
    """Progress is stored per chat once a batch is accepted (it may not be in PostgreSQL yet)."""
    probe = app_factory(cfg=cfg, modules=[])
    await probe.db.connect()
    try:
        n: int = await probe.db.pool.fetchval("SELECT coalesce(sum(loaded), 0) FROM backfill_jobs")
        return n
    finally:
        await probe.db.close()


# ---- criteria 1 and 5: the live flow stays fast; no FloodWait ------------------------------------------------
@pytest.mark.timeout(600)
async def test_live_flow_and_read_commands_stay_fast_during_backfill_without_flood_wait(
        app_factory: AppFactory, cfg: dict[str, Any]) -> None:
    server = FakeTelegram(max_calls_per_s=8)  # more than 8 calls a second -> FLOOD_WAIT
    for chat in (KASSA, KASSA2, LOGS, DEBUG):
        server.add_chat(chat, "supergroup", members=(ACCOUNT,))
    for chat in range(4):
        server.add_chat(-1003000000000 - chat, "supergroup", members=(ACCOUNT,))
        cfg["sources"].append({"chat_id": -1003000000000 - chat, "roles": ["kassa"], "template": "kassa"})
        history(server, -1003000000000 - chat, 300)
    cfg["pool"]["api_rate_per_s"] = {"user": 6, "bot": 6}  # our limiter keeps under the server's limit
    cfg["backfill"] = {"batch": 50, "interval_s": 0.0, "chats_at_once": 4}
    app = await started(app_factory(cfg=cfg, modules=[DemoModule()], server=server))
    await app.backfill.begin(None)

    # x10 of the observed live flow (5-10 thousand a day) is ~1-2 messages a second; we post 5.
    lags: list[float] = []
    replies: list[float] = []
    for i in range(30):
        fake_now = time.perf_counter()
        if i % 10 == 0:
            before = len(sent_by(server, KASSA))
            server.post(KASSA, ADMIN, "/status")
            await wait_for(lambda b=before: len(sent_by(server, KASSA)) > b, timeout=10, what="/status")
            replies.append(time.perf_counter() - fake_now)
        else:
            server.post(KASSA2, BOT, f"note live {i}")
        await asyncio.sleep(0.2)
    await wait_for(lambda: done(app), timeout=300, what="history loaded")
    await app.idle(timeout=120)
    rows = await app.db.pool.fetch(
        "SELECT extract(epoch FROM m.started_at - t.accepted_at) AS lag FROM trace_inbound t "
        "JOIN trace_module m USING (trace_id) JOIN messages g ON g.trace_id = t.trace_id "
        "WHERE g.source = 'live' AND g.text LIKE 'note live%'")
    lags = sorted(float(r["lag"]) for r in rows)
    assert len(lags) == 27
    p95 = lags[int(len(lags) * 0.95) - 1]
    print(f"live during backfill: p95={p95:.3f}s max={lags[-1]:.3f}s; /status replies {replies}")
    assert p95 <= 1.0 and max(replies) <= 1.0
    assert server.flood_waits == 0 and app.backfill.flood_waits == 0
    assert await loaded(app) == 1200
