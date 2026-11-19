"""Criteria 3-6 of stage 1b: FloodWait, PEER_FLOOD, ban, deleted chat / kicked account."""

from __future__ import annotations

import time
from typing import Any, ClassVar

from autocash.core.pool.state import AccountState
from autocash.core.telegram.api import FloodWait
from autocash.modules.api import Context, Event, Module
from autocash.modules.demo.module import DemoModule
from tests.conftest import ACCOUNT, BOT, CASHER, DEPOSIT_TEXT, KASSA, KASSA2, LOGS, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import ACC2, join_all, sent_by, started, wait_for


class Relay(Module):
    """Sends a notification to another chat for every note: a group task to route."""

    name: ClassVar[str] = "relay"
    subscriptions: ClassVar[list[str]] = ["test.note"]

    async def handle(self, event: Event, ctx: Context) -> None:
        if event.external:
            ctx.send(KASSA2, f"relay: {event.record.text}", rule="relay", kind="notify")


def other(account: int) -> int:
    return ACC2 if account == ACCOUNT else ACCOUNT


async def test_flood_wait_pauses_account_survives_restart_and_duties_move(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    """Criterion 3 (FT-10.3)."""
    join_all(fake, ACC2)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2)))
    victim = app.scheduler.account_for("send", KASSA)
    assert victim is not None
    spare = other(victim)
    fake.fail_next(victim, FloodWait(7.0), method="send")

    began = time.time()
    fake.post(KASSA, CASHER, "/ping")
    await app.idle()

    assert app.pool.state(victim) is AccountState.PAUSED
    assert sent_by(fake, KASSA) == [(spare, "pong")]  # the task moved to the other account
    await wait_for(lambda: app.scheduler.rebalanced_at >= app.pool.changed_at, what="rebalance")
    assert app.scheduler.rebalanced_at - app.pool.changed_at < app.store.current.config.pool.rebalance_s
    for duty in ("send", "read", "poll"):
        assert not app.scheduler.chats_for(duty, victim), duty  # all its duties went to the other one
    assert app.scheduler.account_for("send", KASSA) == spare
    await app.writer.flush()
    pause = await app.db.pool.fetchrow(
        "SELECT method, extract(epoch FROM paused_until)::float8 AS until FROM flood_pauses WHERE account_id = $1",
        victim)
    assert pause["method"] == "send" and 5.0 < pause["until"] - began < 9.0
    attempts = await app.db.pool.fetch(  # the chat of the task (logs got the notice of the fuse, D-OBX-7)
        "SELECT a.account_id, a.result, a.error_code FROM outbound_attempts a "
        "JOIN outbound_tasks t ON t.id = a.task_id WHERE t.chat_id = $1 ORDER BY a.id", KASSA)
    assert [(a["account_id"], a["result"]) for a in attempts] == [(victim, "flood_wait"), (spare, "delivered")]
    assert app.outbox.fuse.factor(victim) == 0.5 and app.outbox.fuse.factor(spare) == 1.0  # per account
    log = await app.db.pool.fetch(
        "SELECT old_state, new_state FROM account_state_log WHERE account_id = $1 ORDER BY id", victim)
    assert [(r["old_state"], r["new_state"]) for r in log] == [("disconnected", "active"), ("active", "paused")]
    await app.stop()

    # Restart: the pause is read from the database before any active call is allowed.
    app2 = await started(app_factory(accounts=(ACCOUNT, ACC2)))
    assert app2.pool.state(victim) is AccountState.PAUSED and app2.pool.state(spare) is AccountState.ACTIVE
    assert not app2.scheduler.chats_for("send", victim)
    assert time.time() - began < 7.0, "the restart took longer than the pause: the check above is void"

    # When the pause ends the account comes back and takes its share of duties again.
    await wait_for(lambda: app2.pool.state(victim) is AccountState.ACTIVE, timeout=12, what="end of the pause")
    await wait_for(lambda: bool(app2.scheduler.chats_for("send", victim)), what="duties of the returned account")
    assert time.time() - began >= 7.0
    await app2.writer.flush()
    assert await app2.db.pool.fetchval("SELECT state FROM accounts WHERE account_id = $1", victim) == "active"


async def test_peer_flood_restricts_account_alarm_goes_through_another_one(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    """Criterion 4 (FT-1.3)."""
    join_all(fake, ACC2)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2)))
    victim = app.scheduler.account_for("send", KASSA)
    assert victim is not None
    spare = other(victim)
    fake.restricted.add(victim)

    fake.post(KASSA, CASHER, "/ping")
    await app.idle()

    assert app.pool.state(victim) is AccountState.RESTRICTED
    assert sent_by(fake, KASSA) == [(spare, "pong")]
    alarms = [(account, text) for account, text in sent_by(fake, LOGS) if "ограничен" in text]
    assert len(alarms) == 1 and alarms[0][0] == spare and str(victim) in alarms[0][1]  # sent by ANOTHER account
    assert app.pool.coverage(KASSA) == 2  # a restricted account still covers the chat (FT-1.5)

    # Intake goes on at the restricted account.
    fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=5))
    await app.idle()
    seen = await app.db.pool.fetch(
        "SELECT s.account_id FROM message_seen s JOIN messages m USING (scope_id, chat_id, msg_id, version) "
        "WHERE m.msg_type = 'deposit.request' ORDER BY 1")
    assert [r["account_id"] for r in seen] == sorted([victim, spare])
    assert await app.db.pool.fetchval("SELECT state FROM accounts WHERE account_id = $1", victim) == "restricted"
    assert not app.scheduler.chats_for("send", victim)


async def test_ban_loses_account_group_tasks_move_private_ones_wait_and_expire(
    app_factory: AppFactory, fake: FakeTelegram, config: dict[str, Any]
) -> None:
    """Criterion 5 (FT-9.4, FT-9.5, FT-9.6)."""
    config["outbox"]["ttl_s"] = {"command_reply": 3, "client_reply": 3, "notify": 60, "bulk": 60}
    join_all(fake, ACC2)
    fake.add_chat(PLAYER, "private", "player", members=(ACCOUNT,))  # the player wrote to ACCOUNT only
    app = await started(app_factory(accounts=(ACCOUNT, ACC2), cfg=config))

    # The answer in the private dialog is held back by a short FloodWait, then the account is banned.
    fake.fail_next(ACCOUNT, FloodWait(1.0), method="send")
    fake.post(PLAYER, PLAYER, "/ping")
    await wait_for(lambda: app.pool.state(ACCOUNT) is AccountState.PAUSED, what="pause")

    async def parked() -> bool:
        # The worker hands the task back to the router, which parks it: wait for the record.
        return bool(await app.db.pool.fetchval("SELECT status = 'waiting' FROM outbound_tasks"))

    await wait_for(parked, timeout=5, what="task parked")
    task = await app.db.pool.fetchrow("SELECT status, reason_code, dialog_account_id FROM outbound_tasks")
    assert (task["status"], task["reason_code"], task["dialog_account_id"]) == ("waiting", "ACCOUNT_PAUSED", ACCOUNT)

    fake.ban(ACCOUNT)
    await wait_for(lambda: app.pool.state(ACCOUNT) is AccountState.LOST, what="lost")
    await wait_for(lambda: any("потерян" in text for _, text in sent_by(fake, LOGS)), what="alarm")
    assert [account for account, text in sent_by(fake, LOGS) if "потерян" in text] == [ACC2]

    # A group task goes to the remaining account at once.
    fake.post(KASSA, CASHER, "/ping")
    await wait_for(lambda: sent_by(fake, KASSA) == [(ACC2, "pong")], what="group answer")

    # The private one waits for its own account with ACCOUNT_LOST and expires by TTL.
    await app.writer.flush()
    task = await app.db.pool.fetchrow("SELECT status, reason_code FROM outbound_tasks WHERE chat_id = $1", PLAYER)
    assert (task["status"], task["reason_code"]) == ("waiting", "ACCOUNT_LOST")

    async def expired() -> bool:
        return bool(await app.db.pool.fetchval(
            "SELECT status = 'expired' FROM outbound_tasks WHERE chat_id = $1", PLAYER))

    await wait_for(expired, timeout=10, what="TTL")
    task = await app.db.pool.fetchrow(
        "SELECT status, reason_code, last_wait_reason, finished_at FROM outbound_tasks WHERE chat_id = $1", PLAYER)
    assert (task["status"], task["reason_code"], task["last_wait_reason"]) == ("expired", "TTL_EXPIRED", "ACCOUNT_LOST")
    assert task["finished_at"] is not None and sent_by(fake, PLAYER) == []  # nobody else answered the player
    assert app.pool.coverage(KASSA) == 1 and fake.clients[ACCOUNT].starts == 1  # a lost account is not reconnected
    assert await app.db.pool.fetchval("SELECT state FROM accounts WHERE account_id = $1", ACCOUNT) == "lost"


async def test_kicked_account_and_deleted_chat_update_membership_and_tasks(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    """Criterion 6 (FT-1.4, FT-10.5, FT-11.8)."""
    join_all(fake, ACC2)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2), modules=[DemoModule(), Relay()]))
    assert app.pool.coverage(KASSA) == 2

    # 1. The account is kicked and Telegram tells it so: the map follows the event.
    victim = app.scheduler.account_for("send", KASSA)
    assert victim is not None
    spare = other(victim)
    fake.kick(victim, KASSA)
    await wait_for(lambda: app.pool.coverage(KASSA) == 1, what="membership after the kick")
    await wait_for(lambda: app.scheduler.account_for("send", KASSA) == spare, what="send duty")
    fake.post(KASSA, CASHER, "/ping")
    await app.idle()
    assert sent_by(fake, KASSA) == [(spare, "pong")]
    rows = await app.db.pool.fetch("SELECT account_id FROM chat_membership WHERE chat_id = $1", KASSA)
    assert [r["account_id"] for r in rows] == [spare]

    # 2. The account lost a chat without any event: the send error updates the map and the task
    #    is handed to the other account.
    #    The victim is the account OBX_Router will choose: the least loaded, on a tie the send duty.
    duty = app.scheduler.account_for("send", KASSA2)
    victim2 = min((ACCOUNT, ACC2), key=lambda a: (app.outbox.antiflood.recent(a), a != duty, a))
    fake.chats[KASSA2].members.discard(victim2)
    fake.post(KASSA, BOT, "note one")
    await app.idle()
    assert sent_by(fake, KASSA2) == [(other(victim2), "relay: note one")]
    assert list(app.pool.membership.members(KASSA2)) == [other(victim2)]
    attempts = await app.db.pool.fetch(
        "SELECT a.account_id, a.result, a.error_code FROM outbound_attempts a "
        "JOIN outbound_tasks t ON t.id = a.task_id WHERE t.chat_id = $1 ORDER BY a.id", KASSA2)
    assert [(a["account_id"], a["result"], a["error_code"]) for a in attempts] == [
        (victim2, "error", "CHANNEL_PRIVATE"), (other(victim2), "delivered", None)]

    # 3. The chat is deleted: nobody is left in it, the task waits with NO_ACCOUNT_IN_CHAT and
    #    the zero coverage raises an alarm.
    fake.delete_chat(KASSA2)
    await wait_for(lambda: app.pool.coverage(KASSA2) == 0, what="membership after the deletion")
    fake.post(KASSA, BOT, "note two")
    await app.idle()
    task = await app.db.pool.fetchrow(
        "SELECT status, reason_code, wait_until = expires_at AS until_ttl FROM outbound_tasks WHERE text = $1",
        "relay: note two")
    assert (task["status"], task["reason_code"], task["until_ttl"]) == ("waiting", "NO_ACCOUNT_IN_CHAT", True)
    await wait_for(lambda: any("Покрытие 0" in text and str(KASSA2) in text for _, text in sent_by(fake, LOGS)),
                   what="coverage alarm")
    assert (KASSA2, 0, 2) in app.pool.under_covered() and (KASSA, 1, 2) in app.pool.under_covered()

    # 4. An account comes (back) into the chat: the waiting task is delivered.
    fake.add_chat(KASSA2, "supergroup", members=())
    fake.join(ACCOUNT, KASSA2)
    await wait_for(lambda: sent_by(fake, KASSA2)[-1:] == [(ACCOUNT, "relay: note two")], what="delivery after join")


async def test_write_forbidden_keeps_reader_and_removes_sender(app_factory: AppFactory, fake: FakeTelegram) -> None:
    """FT-10.5: CHAT_WRITE_FORBIDDEN - the account stays a reader and leaves the senders of the chat."""
    join_all(fake, ACC2)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2)))
    victim = app.scheduler.account_for("send", KASSA)
    assert victim is not None
    fake.chats[KASSA].readonly.add(victim)

    fake.post(KASSA, CASHER, "/ping")
    await app.idle()
    assert sent_by(fake, KASSA) == [(other(victim), "pong")]
    assert app.pool.membership.members(KASSA) == {victim: False, other(victim): True}
    assert app.pool.coverage(KASSA) == 2 and app.pool.state(victim) is AccountState.ACTIVE

    # With no account left that may write, the task ends as "not delivered" (FT-11.5).
    fake.chats[KASSA].readonly.add(other(victim))
    fake.post(KASSA, CASHER, "/ping")
    await app.idle()
    last = await app.db.pool.fetchrow("SELECT status, reason_code FROM outbound_tasks WHERE chat_id = $1 "
                                      "ORDER BY id DESC LIMIT 1", KASSA)  # logs get a notice of it
    assert (last["status"], last["reason_code"]) == ("not_delivered", "CHAT_WRITE_FORBIDDEN")
