"""AS: Autosup on the fake Telegram and a real PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from autocash.core.app import App
from autocash.modules.autosup.AS_module import AutosupModule
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, AppFactory
from tests.fake_telegram.server import FakeMessage, FakeTelegram
from tests.helpers import started, wait_for

ASK = "Здравствуйте!\nПришлите ваш ID"
NEW = 9201  # writes for the first time ever
OLD = 9202  # has an old message with the ID
QUIET = 9203  # has an old message without an ID


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["modules"] = {"role_manager": {}, "autosup": {}}
    return config


def old_message(fake: FakeTelegram, user: int, text: str) -> int:
    """A message of the dialog written before the monitor knew the user."""
    if user not in fake.chats:
        fake.add_chat(user, "private", f"user {user}", members=(ACCOUNT,))
    fake._acct_seq[ACCOUNT] += 1
    msg_id = fake._acct_seq[ACCOUNT]
    fake.chats[user].messages.append(FakeMessage(user, user, text, datetime(2026, 9, 1, tzinfo=UTC), {ACCOUNT: msg_id}))
    return msg_id


async def as_app(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> App:
    for user in (NEW, OLD, QUIET):
        if user not in fake.chats:
            fake.add_chat(user, "private", f"user {user}", members=(ACCOUNT,))
    return await started(app_factory(cfg=cfg, modules=[RoleManagerModule(), AutosupModule()]))


def sent_to(fake: FakeTelegram, user: int) -> list[str]:
    return [text for _, chat, text, _ in fake.sent if chat == user]


async def player_of(app: App, tg_id: int) -> tuple[Any, list[str]]:
    player = await app.db.pool.fetchval("SELECT player_id FROM rm_users WHERE tg_id = $1", tg_id)
    mels = [r["mel_id"] for r in await app.db.pool.fetch(
        "SELECT mel_id FROM rm_player_mel WHERE player_id = $1", player)] if player else []
    return player, mels


async def test_first_contact_asks_takes_the_next_id_and_pins_it(app_factory: AppFactory, fake: FakeTelegram,
                                                                 cfg: dict[str, Any]) -> None:
    app = await as_app(app_factory, fake, cfg)
    fake.post(NEW, NEW, "Здравствуйте, вопрос по депозиту")
    await wait_for(lambda: sent_to(fake, NEW) == [ASK], what="the request of the ID")
    fake.post(NEW, NEW, "Перевёл 5 478 рублей")  # an amount: not an ID, still waiting
    fake.post(NEW, NEW, "+79161234567")  # a phone: not an ID
    reply = fake.post(NEW, NEW, "Id 1234567890")
    await wait_for(lambda: fake.pins, what="the pin")
    assert fake.pins == [(ACCOUNT, NEW, reply.id_for(ACCOUNT))]  # the user's own message is pinned
    player, mels = await player_of(app, NEW)
    assert player is not None and mels == ["1234567890"]
    row = await app.db.pool.fetchrow("SELECT contact, state, found_in FROM as_dialogs WHERE tg_id = $1", NEW)
    assert dict(row) == {"contact": "first", "state": "done", "found_in": "reply"}
    fake.post(NEW, NEW, "1987654321")  # done: later numbers change nothing
    await app.idle()
    assert (await player_of(app, NEW))[1] == ["1234567890"] and sent_to(fake, NEW) == [ASK]


async def test_an_id_with_spaces_is_written_back_normalized_and_that_message_pinned(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await as_app(app_factory, fake, cfg)
    fake.post(NEW, NEW, "добрый день")
    await wait_for(lambda: sent_to(fake, NEW) == [ASK], what="the request of the ID")
    fake.post(NEW, NEW, "1 234 567 890")
    await wait_for(lambda: fake.pins, what="the pin")
    assert sent_to(fake, NEW) == [ASK, "1234567890"]
    own = next(msg_id for acc, chat, text, msg_id in fake.sent if chat == NEW and text == "1234567890")
    assert fake.pins == [(ACCOUNT, NEW, own)]  # Autosup pins its own message
    assert (await player_of(app, NEW))[1] == ["1234567890"]
    tasks = await app.db.pool.fetch("SELECT module, action, binding, account_id, confirmed_at IS NOT NULL AS ok "
                                    "FROM outbound_tasks WHERE chat_id = $1 ORDER BY id", NEW)
    assert [(t["module"], t["action"], t["binding"], t["account_id"], t["ok"]) for t in tasks] == [
        ("autosup", "send", "dialog", ACCOUNT, True), ("autosup", "send", "dialog", ACCOUNT, True),
        ("autosup", "pin", "dialog", ACCOUNT, True)]


async def test_second_contact_takes_the_id_from_the_history(app_factory: AppFactory, fake: FakeTelegram,
                                                            cfg: dict[str, Any]) -> None:
    old_id = old_message(fake, OLD, "мой айди 1987654321")
    app = await as_app(app_factory, fake, cfg)
    fake.post(OLD, OLD, "снова пишу")
    await wait_for(lambda: fake.pins, what="the pin")
    assert fake.pins == [(ACCOUNT, OLD, old_id)] and sent_to(fake, OLD) == []  # no request: the ID is known
    assert (await player_of(app, OLD))[1] == ["1987654321"]
    row = await app.db.pool.fetchrow("SELECT contact, found_in FROM as_dialogs WHERE tg_id = $1", OLD)
    assert dict(row) == {"contact": "second", "found_in": "history"}


async def test_second_contact_without_an_id_asks_as_the_first_but_stays_second(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    old_message(fake, QUIET, "спасибо")
    app = await as_app(app_factory, fake, cfg)
    fake.post(QUIET, QUIET, "здравствуйте")
    await wait_for(lambda: sent_to(fake, QUIET) == [ASK], what="the request of the ID")
    row = await app.db.pool.fetchrow("SELECT contact, state FROM as_dialogs WHERE tg_id = $1", QUIET)
    assert dict(row) == {"contact": "second", "state": "awaiting_id"}
    assert await app.db.pool.fetchval("SELECT contact_protocol FROM rm_users WHERE tg_id = $1", QUIET) == "second"
