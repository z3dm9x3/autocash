"""Criteria 1 and 2 of stage 1b: the pool changes at run time; catch-up without FloodWait.

Also: the safety poll (FT-2.5), "difference too long" (FT-2.7), the sessions directory (FT-1.2).
"""

from __future__ import annotations

import asyncio
import base64
import time
from pathlib import Path
from typing import Any, ClassVar

import pytest

from autocash.core.pool.pool import PoolFull
from autocash.core.pool.sessions import KEY_ENV, SUFFIX, SessionData, write_session
from autocash.core.pool.state import AccountState
from autocash.core.telegram.api import TelegramClient
from autocash.modules.api import Context, Event, Module
from tests.conftest import ACCOUNT, BOT, DEPOSIT_TEXT, KASSA, KASSA2, LOGS, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import ACC2, ACC3, join_all, sent_by, started, wait_for

EXTRA = tuple(range(201, 210))  # nine more accounts: 1 -> 10


def add_sources(config: dict[str, Any], fake: FakeTelegram, count: int, members: tuple[int, ...]) -> list[int]:
    chats = [-1002000000000 - i for i in range(count)]
    for chat_id in chats:
        fake.add_chat(chat_id, "supergroup", members=members)
        config["sources"].append({"chat_id": chat_id, "roles": ["kassa"], "template": "kassa",
                                  "senders": {BOT: "kassa_bot"}})
    return chats


async def test_pool_shrinks_3_to_1_and_grows_1_to_10_without_losing_messages(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    """Criterion 1 (FT-1.2, FT-1.7, NFT-11)."""
    join_all(fake, ACC2, ACC3)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2, ACC3)))
    limit = app.store.current.config.pool.rebalance_s
    assert app.pool.coverage(KASSA) == 3
    posted = 0
    stop = asyncio.Event()

    async def keep_posting() -> None:
        nonlocal posted
        while not stop.is_set():
            fake.post(KASSA if posted % 2 else KASSA2, BOT, DEPOSIT_TEXT.format(deposit_id=posted))
            posted += 1
            await asyncio.sleep(0.01)

    poster = asyncio.create_task(keep_posting())
    await asyncio.sleep(0.3)

    # ---- 3 -> 1 ----
    changed = time.monotonic()
    await app.pool.remove(ACC2)
    await app.pool.remove(ACC3)
    await wait_for(lambda: app.scheduler.rebalanced_at >= app.pool.changed_at, timeout=limit, what="rebalance")
    assert time.monotonic() - changed < limit
    for duty in ("send", "read", "poll"):
        assert set(app.scheduler.assignments[duty].values()) == {ACCOUNT}, duty
    assert app.pool.coverage(KASSA) == 1 and app.pool.coverage(KASSA2) == 1
    assert app.pool.state(ACC2) is AccountState.REMOVED and ACC2 not in app.pool.user_ids
    await asyncio.sleep(0.3)

    # ---- 1 -> 10 ----
    join_all(fake, *EXTRA)
    changed = time.monotonic()
    for account in EXTRA:
        await app.pool.add(fake.client(account), f"extra-{account}")
    await wait_for(lambda: all(a.dialogs_loaded for a in app.pool.live())
                   and app.scheduler.rebalanced_at >= app.pool.changed_at, timeout=limit, what="rebalance")
    assert time.monotonic() - changed < limit
    assert len(app.pool.active()) == 10 and app.pool.coverage(KASSA) == 10
    assert not app.pool.under_covered()
    load = app.scheduler.summary()
    duties = [d["send"] + d["read"] for d in load.values()]
    assert len(load) == 10 and max(duties) - min(duties) <= 1, load  # spread evenly, not piled on one account
    with pytest.raises(PoolFull):
        await app.pool.add(fake.client(999), "eleventh")

    await asyncio.sleep(0.3)
    # The flow keeps going until there is a meaningful volume, whatever the speed of the machine.
    await wait_for(lambda: posted > 50, timeout=30, what="posted volume")
    stop.set()
    await poster
    await app.idle(timeout=60)

    # Nothing was lost while the pool was changing.
    db = app.db.pool
    assert posted > 50
    assert await db.fetchval("SELECT count(*) FROM messages WHERE msg_type = 'deposit.request'") == posted
    assert await db.fetchval("SELECT count(*) FROM trace_inbound WHERE status = 'processed'") == posted
    assert await db.fetchval("SELECT sum((attrs->>'count')::int) FROM entities WHERE entity_type = 'demo.counter'") \
        == posted
    states = await db.fetch("SELECT state, count(*) AS n FROM accounts GROUP BY 1 ORDER BY 1")
    assert {r["state"]: r["n"] for r in states} == {"active": 10, "removed": 2}
    stored = await db.fetch("SELECT DISTINCT account_id FROM duty_assignments")
    assert {r["account_id"] for r in stored} <= {ACCOUNT, *EXTRA} and len(stored) > 1


async def test_safety_poll_scales_with_the_pool_10_chats_per_account(
    app_factory: AppFactory, fake: FakeTelegram, config: dict[str, Any]
) -> None:
    """FT-2.5: 10 polled chats with one account, 100 with ten."""
    add_sources(config, fake, 120, (ACCOUNT, *EXTRA))
    app = await started(app_factory(cfg=config))
    assert len(app.scheduler.assignments["poll"]) == 10
    for account in EXTRA:
        await app.pool.add(fake.client(account))
    await wait_for(lambda: len(app.scheduler.assignments["poll"]) == 100, what="poll duties")
    per_account = [len(app.scheduler.chats_for("poll", a.account_id)) for a in app.pool.active()]
    assert per_account == [10] * 10


async def test_safety_poll_brings_updates_that_were_not_pushed(app_factory: AppFactory, fake: FakeTelegram) -> None:
    """FT-2.5: Telegram may send fewer push updates for a supergroup; the poll closes the hole."""
    app = await started(app_factory())
    fake.clients[ACCOUNT].deaf = True
    for i in range(3):
        fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=i))

    async def stored() -> bool:
        return bool(await app.db.pool.fetchval("SELECT count(*) = 3 FROM trace_inbound WHERE status = 'processed'"))

    await wait_for(stored, timeout=10, what="polled messages")
    assert app.poller.polls > 0 and fake.calls["getChannelDifference"] > 0
    rows = await app.db.pool.fetch("SELECT msg_id FROM messages ORDER BY ingest_id")
    assert [r["msg_id"] for r in rows] == [1, 2, 3]


async def test_catch_up_of_100_chats_without_a_single_flood_wait(
    app_factory: AppFactory, config: dict[str, Any]
) -> None:
    """Criterion 2 (FT-2.6, FT-10.1): the fake server answers FloodWait above 5 calls per second."""
    server = FakeTelegram(max_calls_per_s=5)
    for chat_id in (KASSA, KASSA2, LOGS):
        server.add_chat(chat_id, "supergroup", members=(ACCOUNT, ACC2))
    chats = add_sources(config, server, 98, (ACCOUNT, ACC2))
    chats += [KASSA, KASSA2]
    config["pool"]["api_rate_per_s"] = {"user": 4, "bot": 4}

    app = await started(app_factory(cfg=config, accounts=(ACCOUNT, ACC2), server=server))
    server.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=0))
    await app.idle()
    await app.stop()

    # The system is down; three messages arrive in each of the 100 chats.
    for n in range(3):
        for chat_id in chats:
            server.post(chat_id, BOT, DEPOSIT_TEXT.format(deposit_id=f"{-chat_id}{n}"))

    app2 = app_factory(cfg=config, accounts=(ACCOUNT, ACC2), server=server)
    began = time.monotonic()
    await started(app2)
    assert len(app2.pool.gaps) == 100  # only the chats with a gap are caught up
    await app2.idle(timeout=90)
    took = time.monotonic() - began

    assert server.flood_waits == 0, "an active call ran into FloodWait"
    assert not app2.pool.gaps and app2.poller.catchups == 100
    db = app2.db.pool
    assert await db.fetchval("SELECT count(*) FROM messages") == 301
    assert await db.fetchval("SELECT count(*) FROM trace_inbound WHERE status = 'processed'") == 301
    per_chat = await db.fetch("SELECT chat_id, count(*) AS n FROM messages GROUP BY 1 HAVING count(*) < 3")
    assert per_chat == []
    # Both accounts took part: the budget of the system grows with the pool (FT-10.1).
    readers = {a.account_id: a.limiter.calls for a in app2.pool.active()}
    assert all(calls > 20 for calls in readers.values()), readers
    assert all(a.state is AccountState.ACTIVE for a in app2.pool.live())
    print(f"\ncatch-up of 100 chats: {took:.1f} s, API calls: {dict(server.calls)}")


class Reconciling(Module):
    name: ClassVar[str] = "reconciling"
    subscriptions: ClassVar[list[str]] = ["deposit.*"]

    def __init__(self) -> None:
        self.reconciled: list[int] = []

    async def handle(self, event: Event, ctx: Context) -> None:
        return None

    async def reconcile(self, chat_id: int) -> bool:
        self.reconciled.append(chat_id)
        return True


async def test_difference_too_long_marks_the_source_and_calls_the_reconcile_hook(
    app_factory: AppFactory, config: dict[str, Any]
) -> None:
    """FT-2.7."""
    server = FakeTelegram(difference_limit=5)
    for chat_id in (KASSA, KASSA2, LOGS):
        server.add_chat(chat_id, "supergroup", members=(ACCOUNT,))
    app = await started(app_factory(cfg=config, server=server))
    await app.stop()
    for i in range(10):
        server.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=i))

    module = Reconciling()
    app2 = await started(app_factory(cfg=config, server=server, modules=[module]))
    await wait_for(lambda: module.reconciled == [KASSA], what="reconcile hook")
    await app2.idle()
    gap = await app2.db.pool.fetchrow("SELECT chat_id, pts_from, pts_to, status FROM source_gaps")
    assert (gap["chat_id"], gap["pts_from"], gap["pts_to"], gap["status"]) == (KASSA, 0, 10, "reconciled")
    assert any("Разрыв" in text and str(KASSA) in text for _, text in sent_by(server, LOGS))
    assert KASSA not in app2.pool.gaps and app2.pool.pts[KASSA] == 10

    # The live flow of the source goes on after the gap.
    server.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=99))
    await app2.idle()
    assert await app2.db.pool.fetchval("SELECT count(*) FROM messages WHERE chat_id = $1", KASSA) == 1


async def test_session_file_appearing_adds_an_account_and_vanishing_removes_it(
    app_factory: AppFactory, fake: FakeTelegram, config: dict[str, Any], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """FT-1.2: no restart, no command - the sessions directory is the source of truth."""
    key = bytes(range(32))
    monkeypatch.setenv(KEY_ENV, base64.b64encode(key).decode())
    sessions = tmp_path / "sessions"
    config["pool"].update({"sessions_dir": str(sessions), "scan_interval_s": 0.1})
    join_all(fake, ACC2, ACC3)

    def factory(data: SessionData) -> TelegramClient:
        return fake.client(int(data.session))  # the fake "session string" is the account id

    def session(account: int) -> SessionData:
        return SessionData(name=f"acc{account}", kind="user", api_id=1, api_hash="h", session=str(account))

    first = write_session(sessions, session(ACC2), key)
    app = await started(app_factory(cfg=config, accounts=(), client_factory=factory))
    assert [a.account_id for a in app.pool.active()] == [ACC2]

    write_session(sessions, session(ACC3), key)
    (sessions / f"broken{SUFFIX}").write_bytes(b"not a session")
    await wait_for(lambda: len(app.pool.active()) == 2, what="second account")
    fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=1))
    await app.idle()
    assert await app.db.pool.fetchval("SELECT count(*) FROM message_seen") == 2

    first.unlink()
    await wait_for(lambda: app.pool.state(ACC2) is AccountState.REMOVED, what="removal")
    assert [a.account_id for a in app.pool.active()] == [ACC3]
