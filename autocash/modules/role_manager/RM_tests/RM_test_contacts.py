"""RM: protocols of the first and the second contact (D-RM-11, D-RM-12)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest

from autocash.core.app import App
from autocash.modules.api import Context, Event, Module
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, AppFactory
from tests.fake_telegram.server import FakeMessage, FakeTelegram
from tests.helpers import started, wait_for

ADMIN = 8001
NEW = 9101  # writes for the first time ever
OLD = 9102  # wrote to the account before the monitor knew them
KNOWN = 9103  # is in rm_users already


class Recorder(Module):
    """A module taking part in the contact protocols: it sees the user already written by RM."""

    name: ClassVar[str] = "recorder"
    protocols: ClassVar[list[str]] = ["contact.first", "contact.second"]

    def __init__(self) -> None:
        self.calls: list[tuple[str, int, str | None]] = []

    async def handle(self, event: Event, ctx: Context) -> None:
        return None

    async def on_protocol(self, protocol: str, subject: int, data: dict[str, Any]) -> None:
        assert self.services is not None
        role = await self.services.db.pool.fetchval("SELECT role FROM rm_users WHERE tg_id = $1", subject)
        self.calls.append((protocol, subject, role))


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["modules"] = {"role_manager": {}, "recorder": {}}
    return config


async def contact_app(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any],
                      recorder: Recorder) -> App:
    for user in (NEW, OLD, KNOWN):
        fake.add_chat(user, "private", f"user {user}", members=(ACCOUNT,))
    # OLD has a history with the account from before: a message the monitor never saw.
    fake._acct_seq[ACCOUNT] += 1
    fake.chats[OLD].messages.append(FakeMessage(OLD, OLD, "давнее сообщение", datetime(2025, 1, 1, tzinfo=UTC),
                                                {ACCOUNT: fake._acct_seq[ACCOUNT]}))
    rm = RoleManagerModule({ADMIN: "admin", KNOWN: "player"})
    return await started(app_factory(cfg=cfg, modules=[rm, recorder]))


async def runs(app: App) -> list[tuple[str, int, str]]:
    rows = await app.db.pool.fetch("SELECT protocol, subject, status FROM protocol_runs ORDER BY id")
    return [(r["protocol"], int(r["subject"]), r["status"]) for r in rows]


async def test_first_contact_once_and_rm_first(app_factory: AppFactory, fake: FakeTelegram,
                                               cfg: dict[str, Any]) -> None:
    recorder = Recorder()
    app = await contact_app(app_factory, fake, cfg, recorder)
    fake.post(NEW, NEW, "здравствуйте")
    await app.idle()
    await wait_for(lambda: recorder.calls, what="the protocol")
    fake.post(NEW, NEW, "/start")  # the next messages: no protocol any more
    fake.post(NEW, NEW, "ещё вопрос")
    await app.idle()
    await app.protocols.run_pending()
    assert recorder.calls == [("contact.first", NEW, "guest")]  # RM wrote the user before the others acted
    assert await runs(app) == [("contact.first", NEW, "done")]
    row = await app.db.pool.fetchrow("SELECT role, source, had_dm, contact_protocol FROM rm_users WHERE tg_id = $1",
                                     NEW)
    assert dict(row) == {"role": "guest", "source": "dm", "had_dm": True, "contact_protocol": "first"}
    assert app.store.directory.user_role(NEW) == "guest"
    assert [t for _, chat, t, _ in fake.sent if chat == NEW] == []  # nothing is said to the user


async def test_second_contact_for_a_user_with_history(app_factory: AppFactory, fake: FakeTelegram,
                                                      cfg: dict[str, Any]) -> None:
    recorder = Recorder()
    app = await contact_app(app_factory, fake, cfg, recorder)
    fake.post(OLD, OLD, "снова пишу")
    await app.idle()
    await wait_for(lambda: recorder.calls, what="the protocol")
    assert recorder.calls == [("contact.second", OLD, "guest")]
    assert await app.db.pool.fetchval("SELECT contact_protocol FROM rm_users WHERE tg_id = $1", OLD) == "second"


async def test_a_known_user_gets_no_protocol(app_factory: AppFactory, fake: FakeTelegram,
                                             cfg: dict[str, Any]) -> None:
    recorder = Recorder()
    app = await contact_app(app_factory, fake, cfg, recorder)
    # Imported users (e.g. from Cash Monitor Dev) are in the base: no protocol for them either.
    await app.db.pool.execute("INSERT INTO rm_users (tg_id, source, had_dm) VALUES ($1, 'import:cmd', true)", NEW)
    fake.post(KNOWN, KNOWN, "привет")
    fake.post(NEW, NEW, "привет")  # the directory did not know them yet: the base decides
    await app.idle()
    await app.protocols.run_pending()
    assert recorder.calls == [] and await runs(app) == []
    assert app.store.directory.user_role(KNOWN) == "player"
    assert app.store.directory.user_role(NEW) == "guest"
