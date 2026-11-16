"""Criteria 7, 8, 9 of stage 1b: multi-account intake, own messages, a pool of one account."""

from __future__ import annotations

from typing import Any

from autocash.core.pool.state import AccountState
from tests.conftest import ACCOUNT, BOT, CASHER, DEPOSIT_TEXT, KASSA, KASSA2, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import ACC2, ACC3, join_all, sent_by, started


async def test_one_message_seen_by_three_accounts_is_stored_once(app_factory: AppFactory, fake: FakeTelegram) -> None:
    """Criterion 7 (FT-2.1, FT-2.2)."""
    join_all(fake, ACC2, ACC3)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2, ACC3)))

    msg = fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=1))
    await app.idle()

    pool = app.db.pool
    assert await pool.fetchval("SELECT count(*) FROM messages") == 1
    seen = await pool.fetch("SELECT account_id FROM message_seen ORDER BY account_id")
    assert [r["account_id"] for r in seen] == [ACCOUNT, ACC2, ACC3]
    assert await pool.fetchval("SELECT count(*) FROM trace_inbound") == 1
    assert await pool.fetchval("SELECT count(*) FROM trace_module WHERE status = 'processed'") == 1
    assert await pool.fetchval("SELECT attrs->>'count' FROM entities WHERE entity_key = $1", str(KASSA)) == "1"

    fake.edit(msg, DEPOSIT_TEXT.format(deposit_id=1) + "\nСтатус: ✅")
    await app.idle()
    assert await pool.fetchval("SELECT count(*) FROM messages") == 2  # one more version, again stored once
    assert await pool.fetchval("SELECT count(*) FROM message_seen") == 6
    assert await pool.fetchval("SELECT attrs->>'count' FROM entities WHERE entity_key = $1", str(KASSA)) == "2"
    assert app.gateway.accepted == 6  # every account accepted both events into the buffer


async def test_own_message_on_another_account_gets_origin_and_fires_no_rules(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    """Criterion 8 (FT-2.10)."""
    join_all(fake, ACC2, ACC3)
    app = await started(app_factory(accounts=(ACCOUNT, ACC2, ACC3)))

    fake.post(KASSA, CASHER, "/ping")
    await app.idle()
    # The answer went out once, through one account; the other two received it as an incoming
    # message of a pool account and did not answer it.
    assert [text for _, text in sent_by(fake, KASSA)] == ["pong"]
    sender = sent_by(fake, KASSA)[0][0]
    rows = await app.db.pool.fetch("SELECT text, origin, sender_id, msg_type FROM messages ORDER BY ingest_id")
    assert [(r["text"], r["origin"], r["sender_id"]) for r in rows] == [
        ("/ping", "external", CASHER), ("pong", "system", sender)]
    assert await app.db.pool.fetchval("SELECT count(*) FROM message_seen WHERE msg_id = 2") == 3

    # A staff member types a command by hand from a pool account: origin=human, no reaction.
    other = ACC2 if sender != ACC2 else ACC3
    fake.post(KASSA, other, "/ping")
    await app.idle()
    row = await app.db.pool.fetchrow("SELECT origin, msg_type FROM messages ORDER BY ingest_id DESC LIMIT 1")
    assert (row["origin"], row["msg_type"]) == ("human", "command.ping")
    assert [text for _, text in sent_by(fake, KASSA)] == ["pong"]  # still the single answer
    assert await app.db.pool.fetchval("SELECT count(*) FROM outbound_tasks") == 1


async def test_pool_of_one_account_does_everything(
    app_factory: AppFactory, fake: FakeTelegram, config: dict[str, Any]
) -> None:
    """Criterion 9 (FT-1.1): group and private replies, duties, coverage report."""
    fake.add_chat(PLAYER, "private", "player", members=(ACCOUNT,))
    app = await started(app_factory())

    assert app.pool.state(ACCOUNT) is AccountState.ACTIVE
    assert app.scheduler.account_for("send", KASSA) == ACCOUNT
    assert app.scheduler.account_for("read", KASSA) == ACCOUNT
    assert set(app.scheduler.chats_for("poll", ACCOUNT)) == {KASSA, KASSA2}  # sources with policy full
    # Kassas require a coverage of 2: with one account they are reported, but it is not an alarm.
    assert sorted(app.pool.under_covered()) == sorted([(KASSA, 1, 2), (KASSA2, 1, 2)])
    assert not any("Покрытие 0" in text for _, _, text, _ in fake.sent)

    fake.post(KASSA, CASHER, "/ping")
    fake.post(PLAYER, PLAYER, "/ping")  # a player writes to the account in a private dialog
    await app.idle()
    assert sent_by(fake, KASSA) == [(ACCOUNT, "pong")] and sent_by(fake, PLAYER) == [(ACCOUNT, "pong")]
    tasks = await app.db.pool.fetch(
        "SELECT chat_id, status, account_id, dialog_account_id FROM outbound_tasks ORDER BY chat_id")
    assert [(t["chat_id"], t["status"], t["account_id"], t["dialog_account_id"]) for t in tasks] == [
        (KASSA, "delivered", ACCOUNT, None), (PLAYER, "delivered", ACCOUNT, ACCOUNT)]
    stored = await app.db.pool.fetch("SELECT account_id, name, kind, state FROM accounts")
    assert [(r["account_id"], r["kind"], r["state"]) for r in stored] == [(ACCOUNT, "user", "active")]
    duties = await app.db.pool.fetch("SELECT DISTINCT account_id FROM duty_assignments")
    assert [r["account_id"] for r in duties] == [ACCOUNT]
