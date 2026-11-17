"""RM: /grole and /rm ? on the fake Telegram and a real PostgreSQL."""

from __future__ import annotations

from typing import Any

import pytest

from autocash.core.app import App
from autocash.core.config.loader import ConfigStore
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, KASSA, LOGS, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001
GROUP = -1003845234572634  # a supergroup with no role: "other" by default
BASIC = -4456727448  # a basic group with no role
BARE = 3845234572634


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"]}
    config["group_roles"] = ["other", "kassa"]
    config["modules"] = {"role_manager": {}}
    config["web"] = {"git_commit": False}
    return config


async def rm_app(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> App:
    fake.add_chat(GROUP, "supergroup", members=(ACCOUNT,))
    fake.add_chat(BASIC, "group", members=(ACCOUNT,))
    return await started(app_factory(cfg=cfg, modules=[RoleManagerModule()]))


async def stored(app: App, chat_id: int) -> int:
    return int(await app.db.pool.fetchval("SELECT count(*) FROM messages WHERE chat_id = $1", chat_id))


async def ask(fake: FakeTelegram, chat_id: int, text: str, sender: int = ADMIN) -> str:
    before = len(sent_by(fake, chat_id))
    fake.post(chat_id, sender, text)
    await wait_for(lambda: len(sent_by(fake, chat_id)) > before, what=f"answer to {text}")
    return sent_by(fake, chat_id)[-1][1]


async def test_group_without_role_is_other_and_not_read(app_factory: AppFactory, fake: FakeTelegram,
                                                        cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    assert app.store.current.source(GROUP).roles == frozenset({"other"})
    assert app.store.current.source(GROUP).policy == "presence"
    fake.post(GROUP, PLAYER, "note: not read")
    fake.post(KASSA, PLAYER, "note: read")  # a configured source is read as before
    await app.idle()
    assert await stored(app, GROUP) == 0
    assert await stored(app, KASSA) == 1


async def test_grole_sets_the_role_logs_it_and_tells_nothing_to_the_group(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    before = app.store.current.version
    answer = await ask(fake, KASSA, f"/grole {BARE} kassa")
    assert answer == f"Группа {BARE}: роль kassa (было: роль other (по умолчанию))."
    assert app.store.current.config.groups == {BARE: "kassa"}
    assert app.store.current.version != before
    # The file holds the change: a restart reads the same roles.
    assert ConfigStore(app.store.path).current.config.groups == {BARE: "kassa"}
    await wait_for(lambda: any("RM: группа" in t for _, t in sent_by(fake, LOGS)), what="the change in logs")
    assert sent_by(fake, GROUP) == []  # the group itself is told nothing
    row = await app.db.pool.fetchrow("SELECT author, summary FROM config_changes")
    assert row["author"] == ADMIN and "RM" in row["summary"]
    # From now on the group is read.
    fake.post(GROUP, PLAYER, "note: now read")
    await app.idle()
    assert await stored(app, GROUP) == 1


async def test_every_id_form_and_show_and_remove(app_factory: AppFactory, fake: FakeTelegram,
                                                 cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    assert "роль kassa (было" in await ask(fake, KASSA, "/grole -1003845234572634 kassa")
    assert await ask(fake, KASSA, "/grole 1003845234572634") == f"Группа {BARE}: роль kassa"
    assert "уже установлена" in await ask(fake, KASSA, f"/grole -{BARE} kassa")
    assert "(было: роль kassa)" in await ask(fake, KASSA, f"/grole {BARE} -kassa")
    assert app.store.current.config.groups == {}
    assert "4456727448: роль kassa" in await ask(fake, KASSA, "/grole -4456727448 kassa")
    assert app.store.current.config.groups == {4456727448: "kassa"}


async def test_refusals_change_nothing(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    version = app.store.current.version
    assert "Недостаточно прав" in await ask(fake, KASSA, f"/grole {BARE} kassa", sender=PLAYER)
    assert "не найдена" in await ask(fake, KASSA, "/grole 123456789 kassa")
    assert "роли 'bogus' нет" in await ask(fake, KASSA, f"/grole {BARE} bogus")
    assert "а не kassa" in await ask(fake, KASSA, f"/grole {BARE} -kassa")
    assert "Использование" in await ask(fake, KASSA, "/grole")
    assert app.store.current.version == version
    assert await app.db.pool.fetchval("SELECT count(*) FROM config_changes") == 0


async def test_rm_question_in_a_group_that_is_not_read(app_factory: AppFactory, fake: FakeTelegram,
                                                       cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    fake.post(GROUP, PLAYER, "note: not read")
    assert await ask(fake, GROUP, "/rm ?") == f"Группа {BARE}: роль other (по умолчанию)"
    await app.idle()
    # Only the command is stored, the rest of the group is still not read.
    texts = [r["text"] for r in await app.db.pool.fetch("SELECT text FROM messages WHERE chat_id = $1", GROUP)]
    assert texts == ["/rm ?"]
    # The command of someone without rights is refused in the same way.
    assert "Недостаточно прав" in await ask(fake, GROUP, "/rm ?", sender=PLAYER)
