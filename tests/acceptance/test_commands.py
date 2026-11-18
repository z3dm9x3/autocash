"""Stage 1c: debug commands, rights, emergency channel, stuck tasks, metrics (criteria 4-8)."""

from __future__ import annotations

import asyncio
import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest

from autocash.core.app import App
from autocash.core.pool.state import AccountState
from autocash.core.telegram.api import FloodWait
from autocash.modules.api import Context, Event, Module
from autocash.modules.demo.module import DemoModule
from tests.conftest import ACCOUNT, BOT, CASHER, KASSA, KASSA2, LOGS, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import ACC2, ACC3, join_all, sent_by, started, wait_for
from tests.pgcluster import PgCluster

ADMIN = 8001


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"]}
    return config


def replies(fake: FakeTelegram, chat_id: int, start: int = 0) -> list[str]:
    return [text for _, text in sent_by(fake, chat_id)[start:]]


async def ask(app: App, fake: FakeTelegram, text: str, chat_id: int = KASSA, user: int = ADMIN,
              timeout: float = 10.0) -> str:
    """Post a command and return the reply sent to the chat (the command's own trace is idle)."""
    before = len(sent_by(fake, chat_id))
    fake.post(chat_id, user, text)
    await wait_for(lambda: len(sent_by(fake, chat_id)) > before, timeout=timeout, what=f"reply to {text}")
    return replies(fake, chat_id, before)[0]


# ---- criterion 8: rights ---------------------------------------------------------------------------
async def test_command_without_rights_is_refused_with_access_denied(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]
) -> None:
    app = await started(app_factory(cfg=cfg))
    fake.post(KASSA, PLAYER, "/status")
    await app.idle()
    row = await app.db.pool.fetchrow(
        "SELECT t.status, t.reason_code, m.sender_id, m.fields FROM trace_inbound t "
        "JOIN messages m USING (trace_id) WHERE m.text = '/status'")
    assert (row["status"], row["reason_code"], row["sender_id"]) == ("cancelled", "ACCESS_DENIED", PLAYER)
    mod = await app.db.pool.fetchrow("SELECT status, reason_code FROM trace_module WHERE module = 'system'")
    assert (mod["status"], mod["reason_code"]) == ("cancelled", "ACCESS_DENIED")
    # D-RM-8: the chat gets nothing; the refusal goes to the group logs.
    assert sent_by(fake, KASSA) == []
    await wait_for(lambda: any(f"Отказ в правах: пользователь {PLAYER}" in t for _, t in sent_by(fake, LOGS)),
                   what="the refusal in logs")

    # A casher bound to this kassa has no rights either (rights belong to the user's role only);
    # an admin has every right.
    fake.post(KASSA, CASHER, "/status")
    await app.idle()
    assert sent_by(fake, KASSA) == []
    assert "Autocash: работает" in await ask(app, fake, "/status")


# ---- criterion 4: /accounts and /coverage follow the pool ------------------------------------------
async def test_pool_3_to_1_and_1_to_10_shows_in_accounts_and_coverage(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]
) -> None:
    join_all(fake, ACC2, ACC3)
    app = await started(app_factory(cfg=cfg, accounts=(ACCOUNT, ACC2, ACC3)))
    n_seconds = cfg["pool"]["rebalance_s"]
    text = await ask(app, fake, "/accounts")
    assert "Аккаунты пула: 3" in text and text.count(": active") == 3
    assert "Все источники покрыты" in await ask(app, fake, "/coverage")

    # 3 -> 1: within N seconds /accounts shows the change and /coverage the under-covered kassas.
    survivor = ACCOUNT
    changed = time.monotonic()
    await app.pool.remove(ACC2)
    await app.pool.remove(ACC3)

    async def shrunk() -> bool:
        acc = await ask(app, fake, "/accounts")
        return acc.count(": removed") == 2 and acc.count(": active") == 1 and "send: " in acc

    await wait_for(shrunk, timeout=n_seconds + 3, what="/accounts after 3 -> 1")
    assert time.monotonic() - changed <= n_seconds + 3
    cov = await ask(app, fake, "/coverage")
    assert "Покрытие ниже требуемого: 2" in cov and "kassa 1" in cov and "1 из 2" in cov
    assert app.pool.state(survivor) is AccountState.ACTIVE

    # 1 -> 10: nine more accounts join at run time.
    new = list(range(120, 129))
    join_all(fake, *new)
    changed = time.monotonic()
    for account_id in new:
        await app.pool.add(fake.client(account_id))

    async def grown() -> bool:
        acc = await ask(app, fake, "/accounts")
        return acc.count(": active") == 10 and all(f"• {a} " in acc for a in new)

    await wait_for(grown, timeout=n_seconds + 5, what="/accounts after 1 -> 10")
    assert time.monotonic() - changed <= n_seconds + 5
    assert "Все источники покрыты" in await ask(app, fake, "/coverage")


# ---- criterion 5: a ban and a deleted chat are visible in /trace -----------------------------------
class Relay(Module):
    """Answers every note in KASSA with a notification to KASSA2."""

    name: ClassVar[str] = "relay"
    subscriptions: ClassVar[list[str]] = ["test.note"]

    async def handle(self, event: Event, ctx: Context) -> None:
        if event.chat_id == KASSA:
            ctx.send(KASSA2, f"relay: {event.record.text}", rule="relay")


async def test_ban_and_deleted_chat_are_visible_in_trace_with_reason(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]
) -> None:
    cfg["outbox"]["ttl_s"] = {"command_reply": 60, "client_reply": 60, "notify": 60, "bulk": 60}
    join_all(fake, ACC2)
    fake.add_chat(PLAYER, "private", "player", members=(ACCOUNT,))
    app = await started(app_factory(cfg=cfg, accounts=(ACCOUNT, ACC2), modules=[DemoModule(), Relay()]))

    # The answer in the player's dialog is held back by a FloodWait, then its account is banned:
    # the answer waits with ACCOUNT_LOST (only the dialog's own account may answer, FT-9.4).
    fake.fail_next(ACCOUNT, FloodWait(1.0), method="send")
    fake.post(PLAYER, PLAYER, "/ping")
    await wait_for(lambda: app.pool.state(ACCOUNT) is AccountState.PAUSED, what="pause")
    fake.ban(ACCOUNT)
    await wait_for(lambda: app.pool.state(ACCOUNT) is AccountState.LOST, what="lost")
    await app.idle()
    task = await app.db.pool.fetchrow("SELECT trace_id::text, parent_trace_id::text FROM outbound_tasks "
                                      "WHERE chat_id = $1", PLAYER)
    text = await ask(app, fake, f"/trace {task['trace_id']}")
    assert "ожидает (ACCOUNT_LOST)" in text and str(PLAYER) in text
    # The trace of the inbound message shows its outbound task with the same reason.
    text = await ask(app, fake, f"/trace {task['parent_trace_id']}")
    assert "Входящая" in text and "Исходящая" in text and "ACCOUNT_LOST" in text

    # The chat is deleted: the relay task waits with NO_ACCOUNT_IN_CHAT.
    fake.delete_chat(KASSA2)
    await wait_for(lambda: app.pool.coverage(KASSA2) == 0, what="membership after the deletion")
    fake.post(KASSA, BOT, "note x")
    await app.idle()
    relay = await app.db.pool.fetchval("SELECT trace_id::text FROM outbound_tasks WHERE text = 'relay: note x'")
    text = await ask(app, fake, f"/trace {relay}")
    assert "ожидает (NO_ACCOUNT_IN_CHAT)" in text

    # A message link resolves to the trace of that message (t.me/c/<id>/<msg>).
    msg_id = await app.db.pool.fetchval("SELECT msg_id FROM messages WHERE text = 'note x'")
    text = await ask(app, fake, f"/trace https://t.me/c/{str(KASSA)[4:]}/{msg_id}")
    assert "Входящая" in text and "NO_ACCOUNT_IN_CHAT" in text


async def test_trace_uses_resolvers_of_modules(app: App, fake: FakeTelegram) -> None:
    async def by_note(arg: str) -> list[str]:
        rows = await app.db.pool.fetch("SELECT trace_id::text FROM messages WHERE text = $1", f"note {arg}")
        return [r["trace_id"] for r in rows]

    app.commands.resolvers.append(by_note)
    fake.post(KASSA, BOT, "note 777")
    await app.idle()
    text = await app.commands.trace(["777"])
    assert "Входящая" in text and "test.note" in text
    assert "не найдены" in await app.commands.trace(["nothing"])


# ---- criterion 6: emergency channel without PostgreSQL --------------------------------------------
async def test_status_and_accounts_answer_while_postgres_is_stopped(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any], pg_cluster: PgCluster
) -> None:
    app = await started(app_factory(cfg=cfg))
    try:
        await asyncio.to_thread(pg_cluster.stop)
        await wait_for(lambda: not app.db.available, timeout=20, what="outage detected")
        before = len(sent_by(fake, KASSA))
        fake.post(KASSA, ADMIN, "/status")
        fake.post(KASSA, ADMIN, "/accounts")
        fake.post(KASSA, PLAYER, "/status")  # no rights: no emergency answer
        await wait_for(lambda: len(sent_by(fake, KASSA)) >= before + 2, timeout=10, what="emergency replies")
        got = replies(fake, KASSA, before)
        status = next(t for t in got if t.startswith("Autocash"))
        assert "PostgreSQL: НЕДОСТУПЕН" in status and "аварийный канал" in status
        assert any(t.startswith("Аккаунты пула: 1") for t in got)
        await asyncio.sleep(0.5)
        assert len(sent_by(fake, KASSA)) == before + 2
    finally:
        await asyncio.to_thread(pg_cluster.start)
    await app.idle(timeout=60)
    # Once PostgreSQL is back the commands are traced, but not answered a second time;
    # the refusal of the command without rights goes to the group logs, not to the chat (D-RM-8).
    got = replies(fake, KASSA, before)
    assert sum(t.startswith("Autocash") for t in got) == 1 and sum(t.startswith("Аккаунты") for t in got) == 1
    assert len(got) == 2
    await wait_for(lambda: any(f"Отказ в правах: пользователь {PLAYER}" in t for _, t in sent_by(fake, LOGS)),
                   what="the refusal in logs")
    rows = await app.db.pool.fetch("SELECT t.status FROM trace_inbound t JOIN messages m USING (trace_id) "
                                   "WHERE m.sender_id = $1", ADMIN)
    assert [r["status"] for r in rows] == ["processed", "processed"]


# ---- criterion 7: stuck tasks and the mass alarm ---------------------------------------------------
class Slow(Module):
    name: ClassVar[str] = "slow"
    subscriptions: ClassVar[list[str]] = ["test.note"]
    concurrency: ClassVar[int] = 1
    timeout_s: ClassVar[float] = 30.0
    release: asyncio.Event

    async def handle(self, event: Event, ctx: Context) -> None:
        await self.release.wait()


async def test_stuck_tasks_are_marked_listed_and_raise_a_mass_alarm(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]
) -> None:
    cfg["stage_norms_s"] = {"module_done": 0.3}
    cfg["trace"] = {"stuck_check_s": 0.2, "mass_stuck_threshold": 4}
    slow = Slow()
    slow.release = asyncio.Event()
    app = await started(app_factory(cfg=cfg, modules=[slow]))
    for i in range(6):
        fake.post(KASSA, BOT, f"note {i}")
    await wait_for(lambda: any("module_done" in t and "Массовое зависание" in t for _, t in sent_by(fake, LOGS)),
                   timeout=10, what="mass stuck alarm")
    alarm = next(t for _, t in sent_by(fake, LOGS) if "Массовое зависание" in t)
    count = re.search(r": (\d+) задач", alarm)
    assert "обработка модулем" in alarm and count is not None and 4 <= int(count.group(1)) <= 6
    text = await ask(app, fake, "/stuck")
    assert "module_done" in text and "в работе: 6" in text
    assert "module_done" in await ask(app, fake, "/stuck module_done")
    assert "Зависших задач нет" in await ask(app, fake, "/stuck sorted")
    marked = await app.db.pool.fetchval(
        "SELECT count(*) FROM trace_module WHERE module = 'slow' AND stuck_stage = 'module_done'")
    assert marked == 6
    slow.release.set()
    await app.idle()
    assert "Зависших задач нет" in await ask(app, fake, "/stuck")
    # One alarm per episode.
    assert sum("Массовое зависание" in t for _, t in sent_by(fake, LOGS)) == 1


# ---- /retry, /cancel ------------------------------------------------------------------------------------
class Flaky(Module):
    name: ClassVar[str] = "flaky"
    subscriptions: ClassVar[list[str]] = ["test.note"]
    broken = True

    async def handle(self, event: Event, ctx: Context) -> None:
        if self.broken:
            raise RuntimeError("boom")
        ctx.send(KASSA2, f"done: {event.record.text}", rule="flaky")


async def test_retry_restarts_a_failed_module_task_and_cancel_records_the_author(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]
) -> None:
    flaky = Flaky()
    app = await started(app_factory(cfg=cfg, modules=[flaky]))
    fake.post(KASSA, BOT, "note a")
    fake.post(KASSA, BOT, "note b")
    await app.idle()
    rows = await app.db.pool.fetch("SELECT trace_id::text, status, reason_code FROM trace_module ORDER BY queued_at")
    assert [(r["status"], r["reason_code"]) for r in rows] == [("waiting", "MODULE_ERROR")] * 2
    first, second = rows[0]["trace_id"], rows[1]["trace_id"]

    flaky.broken = False
    assert "обработок модулем 1" in await ask(app, fake, f"/retry {first}")
    await app.idle()
    assert replies(fake, KASSA2) == ["done: note a"]
    row = await app.db.pool.fetchrow("SELECT status FROM trace_inbound WHERE trace_id = $1::uuid", first)
    assert row["status"] == "processed"

    text = await ask(app, fake, "/cancel reason=MODULE_ERROR module=flaky")
    assert "обработок модулем 1" in text and str(ADMIN) in text
    row = await app.db.pool.fetchrow(
        "SELECT m.status, m.reason_code, m.cancelled_by, t.status AS inbound FROM trace_module m "
        "JOIN trace_inbound t USING (trace_id) WHERE trace_id = $1::uuid", second)
    assert (row["status"], row["reason_code"], row["cancelled_by"], row["inbound"]) == (
        "cancelled", "CANCELLED_BY_USER", str(ADMIN), "cancelled")
    assert "нет" in await ask(app, fake, f"/retry {second}")  # a cancelled task stays cancelled


async def test_cancel_and_retry_of_a_waiting_outbound_task(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]
) -> None:
    app = await started(app_factory(cfg=cfg, modules=[Relay()]))
    fake.kick(ACCOUNT, KASSA2)
    app.pool.membership.remove(KASSA2, ACCOUNT)  # as if Telegram reported the kick
    fake.post(KASSA, BOT, "note one")
    fake.post(KASSA, BOT, "note two")
    await app.idle()
    tasks = await app.db.pool.fetch("SELECT id, trace_id::text, status, reason_code FROM outbound_tasks ORDER BY id")
    assert [(t["status"], t["reason_code"]) for t in tasks] == [("waiting", "NO_ACCOUNT_IN_CHAT")] * 2
    assert "Отменено: исходящих 1" in await ask(app, fake, f"/cancel {tasks[0]['trace_id']}")
    row = await app.db.pool.fetchrow("SELECT status, reason_code, cancelled_by FROM outbound_tasks WHERE id = $1",
                                     tasks[0]["id"])
    assert tuple(row) == ("cancelled", "CANCELLED_BY_USER", str(ADMIN))

    # The account is back in the chat: /retry with a filter restarts the remaining waiting task.
    fake.chats[KASSA2].members.add(ACCOUNT)
    app.pool.membership.set(KASSA2, ACCOUNT, True)
    await ask(app, fake, "/retry kind=outbound reason=NO_ACCOUNT_IN_CHAT")
    await app.idle()
    assert replies(fake, KASSA2) == ["relay: note two"]


# ---- other commands ------------------------------------------------------------------------------------
async def test_queue_reload_and_heavy_metrics(
    app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any], tmp_path: Any
) -> None:
    app = await started(app_factory(cfg=cfg))
    for i in range(5):
        fake.post(KASSA, BOT, f"note {i}")
    await app.idle()
    assert "Очереди модулей" in await ask(app, fake, "/queue")

    # /metrics is a heavy report: an immediate confirmation, then the result.
    before = len(sent_by(fake, KASSA))
    fake.post(KASSA, ADMIN, "/metrics 1")
    await wait_for(lambda: len(sent_by(fake, KASSA)) >= before + 2, what="ack and result")
    got = replies(fake, KASSA, before)
    assert any("принят" in t.lower() for t in got)
    result = next(t for t in got if t.startswith("Метрики"))
    assert "inbound/sorted" in result and "медиана" in result and "Финальные статусы" in result
    rows = await app.db.pool.fetch(
        "SELECT o.rule FROM outbound_tasks o JOIN messages m ON m.trace_id = o.parent_trace_id "
        "WHERE m.text = '/metrics 1'")
    assert sorted(r["rule"] for r in rows) == ["command.metrics", "command.metrics.result"]
    by_source = await app.commands.metrics(["1", "source"])
    assert f"[{KASSA}]" in by_source

    # /reload: a broken file is not applied, a good one is.
    old = app.store.current.version
    cfg_path = tmp_path / "config.yaml"
    good = cfg_path.read_text(encoding="utf-8")
    cfg_path.write_text(good + "\nunknown_key: 1\n", encoding="utf-8")
    assert "не применена" in await ask(app, fake, "/reload")
    assert app.store.current.version == old
    cfg_path.write_text(good.replace("batch_interval_ms: 50", "batch_interval_ms: 60"), encoding="utf-8")
    assert "перезагружена" in await ask(app, fake, "/reload")
    assert app.store.current.version != old


async def test_detailed_traces_are_purged_and_aggregates_stay(app: App, fake: FakeTelegram) -> None:
    for i in range(3):
        fake.post(KASSA, BOT, f"note {i}")
    await app.idle()
    old = datetime.now(UTC) - timedelta(days=60)
    shift = ", ".join(f"{c} = {c} - interval '60 days'" for c in (
        "accepted_at", "extracted_at", "buffered_at", "in_pg_at", "sorted_at", "finished_at"))
    await app.db.pool.execute(f"UPDATE trace_inbound SET {shift}")
    await app.db.pool.execute("UPDATE trace_module SET queued_at = queued_at - interval '60 days', "
                              "finished_at = finished_at - interval '60 days'")
    removed = await app.monitor.purge()
    assert removed == 6
    assert await app.db.pool.fetchval("SELECT count(*) FROM trace_inbound") == 0
    hour = old.replace(minute=0, second=0, microsecond=0)
    count = await app.db.pool.fetchval(
        "SELECT sum(count) FROM trace_metrics_hourly WHERE task_kind = 'inbound' AND stage = 'sorted' "
        "AND dim = 'all' AND hour BETWEEN $1::timestamptz - interval '1 hour' "
        "AND $1::timestamptz + interval '1 hour'", hour)
    assert count == 3


async def test_status_reports_readiness_and_counters(app: App, fake: FakeTelegram) -> None:
    text = await app.commands.status()
    assert "полная готовность за" in text and "PostgreSQL: доступен" in text
    assert app.ready_at is not None and app.ready_at >= app.started_at
