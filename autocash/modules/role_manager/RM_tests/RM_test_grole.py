"""RM: roles of groups and users in PostgreSQL; /grole, /urole, /rm on the fake Telegram."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from autocash.core.app import App
from autocash.core.directory import Directory
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, KASSA, LOGS, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001
DEV = 8002
MANAGER = 8003
GROUP = -1003845234572634  # a supergroup with no role: "other" by default
BASIC = -4456727448  # a basic group with no role
BARE = 3845234572634


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["modules"] = {"role_manager": {}}
    return config


async def rm_app(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> App:
    fake.add_chat(GROUP, "supergroup", members=(ACCOUNT,))
    fake.add_chat(BASIC, "group", members=(ACCOUNT,))
    users = {ADMIN: "admin", DEV: "dev", MANAGER: "manager"}  # written into rm_users at start
    return await started(app_factory(cfg=cfg, modules=[RoleManagerModule(users)]))


async def stored(app: App, chat_id: int) -> int:
    return int(await app.db.pool.fetchval("SELECT count(*) FROM messages WHERE chat_id = $1", chat_id))


async def ask(fake: FakeTelegram, chat_id: int, text: str, sender: int = ADMIN) -> str:
    before = len(sent_by(fake, chat_id))
    fake.post(chat_id, sender, text)
    await wait_for(lambda: len(sent_by(fake, chat_id)) > before, what=f"answer to {text}")
    return sent_by(fake, chat_id)[-1][1]


def logs(fake: FakeTelegram) -> list[str]:
    return [t for _, t in sent_by(fake, LOGS)]


async def test_tables_roles_and_the_seed(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    roles = {(r["code"], r["scope"]) for r in await app.db.pool.fetch("SELECT code, scope FROM rm_roles")}
    assert {c for c, s in roles if s == "user"} == {"guest", "player", "casher", "support", "manager", "admin", "dev"}
    assert {c for c, s in roles if s == "group"} == {"other", "tech", "dop", "ww"}
    users = {r["tg_id"]: r["role"] for r in await app.db.pool.fetch("SELECT tg_id, role FROM rm_users")}
    assert users == {ADMIN: "admin", DEV: "dev", MANAGER: "manager"}
    rights = await app.db.pool.fetch("SELECT role, command FROM rm_permissions")
    assert [(r["role"], r["command"]) for r in rights] == [("admin", "*")]
    directory = app.store.directory
    assert directory.allowed(DEV) == {"*"} and directory.allowed(ADMIN) == {"*"}
    assert directory.allowed(MANAGER) == set() and directory.allowed(PLAYER) == set()
    assert directory.user_role(PLAYER) == "guest"  # not in the table: a guest


async def test_group_without_role_is_other_and_not_read(app_factory: AppFactory, fake: FakeTelegram,
                                                        cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    assert app.store.current.source(GROUP).roles == frozenset({"other"})
    assert app.store.current.source(GROUP).policy == "presence"
    fake.post(GROUP, PLAYER, "note: not read")
    fake.post(KASSA, PLAYER, "note: read")  # a source of the configuration is read as before
    await app.idle()
    assert await stored(app, GROUP) == 0
    assert await stored(app, KASSA) == 1


async def test_grole_sets_the_role_logs_it_and_tells_nothing_to_the_group(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    answer = await ask(fake, KASSA, f"/grole {BARE} tech")
    assert answer == f"Группа {BARE}: роль tech (было: роль other (по умолчанию))."
    row = await app.db.pool.fetchrow("SELECT role, updated_by FROM rm_groups WHERE group_tg_id = $1", BARE)
    assert (row["role"], row["updated_by"]) == ("tech", ADMIN)
    history = await app.db.pool.fetchrow("SELECT author, before, after FROM rm_history WHERE entity = 'group'")
    assert (history["author"], history["before"], history["after"]) == (ADMIN, None, "tech")
    await wait_for(lambda: any(t.startswith("RM: группа") for t in logs(fake)), what="the change in logs")
    assert sent_by(fake, GROUP) == []  # the group itself is told nothing
    fake.post(GROUP, PLAYER, "note: now read")  # tech is read (policy full)
    await app.idle()
    assert await stored(app, GROUP) == 1


async def test_every_id_form_show_and_remove(app_factory: AppFactory, fake: FakeTelegram,
                                             cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    assert "роль dop (было" in await ask(fake, KASSA, "/grole -1003845234572634 dop")
    assert await ask(fake, KASSA, "/grole 1003845234572634") == f"Группа {BARE}: роль dop"
    assert "уже установлена" in await ask(fake, KASSA, f"/grole -{BARE} dop")
    assert "роль other (было: роль dop)" in await ask(fake, KASSA, f"/grole {BARE} -dop")
    assert "4456727448: роль ww" in await ask(fake, KASSA, "/rm set -4456727448 ww")  # a group role: a group
    assert app.store.directory.groups == {BARE: "other", 4456727448: "ww"}


async def test_urole_and_rm_set_for_users(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    assert await ask(fake, KASSA, f"/urole {MANAGER}") == f"Пользователь {MANAGER}: роль manager"
    answer = await ask(fake, KASSA, f"/urole {MANAGER} support")
    assert answer == f"Пользователь {MANAGER}: роль support (было: manager)."
    assert "роль admin (было: support)" in await ask(fake, KASSA, f"/rm set {MANAGER} Admin")  # a user role: a user
    assert app.store.directory.allowed(MANAGER) == {"*"}
    assert await app.db.pool.fetchval("SELECT role FROM rm_users WHERE tg_id = $1", MANAGER) == "admin"
    # No such user: the author is told, nothing changes.
    assert "нет в базе пользователей" in await ask(fake, KASSA, f"/urole {PLAYER} admin")
    assert "нет в базе пользователей" in await ask(fake, KASSA, f"/rm set {PLAYER} admin")
    assert "роли 'bogus' нет" in await ask(fake, KASSA, f"/rm set {MANAGER} bogus")
    assert "роли пользователя 'tech' нет" in await ask(fake, KASSA, f"/urole {MANAGER} tech")


async def test_without_rights_nothing_in_the_chat_a_notice_in_logs(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    fake.post(KASSA, MANAGER, f"/grole {BARE} tech")  # a manager has no commands for now
    fake.post(GROUP, PLAYER, "/rm ?")  # a guest, in a group that is not read
    fake.post(GROUP, PLAYER, "/rm ?")
    fake.post(KASSA, PLAYER, "/start")  # not our command: no reaction at all
    await app.idle()
    await wait_for(lambda: len([t for t in logs(fake) if t.startswith("Отказ в правах")]) == 2, what="notices")
    notices = [t for t in logs(fake) if t.startswith("Отказ в правах")]
    assert any(f"пользователь {MANAGER} (роль manager), команда /grole" in t for t in notices)
    assert any(f"пользователь {PLAYER} (роль guest), команда /rm" in t for t in notices)  # once: 1 per 10 min
    assert sent_by(fake, KASSA) == [] and sent_by(fake, GROUP) == []
    assert app.store.directory.groups == {}
    denied = await app.db.pool.fetchval("SELECT count(*) FROM trace_module WHERE reason_code = 'ACCESS_DENIED'")
    assert denied == 3


async def test_dev_may_always_run_commands(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    await app.db.pool.execute("DELETE FROM rm_permissions")  # even with no rights in the table
    await app.modules[0].reload()  # type: ignore[attr-defined]
    assert "роль tech" in await ask(fake, KASSA, f"/grole {BARE} tech", sender=DEV)


async def test_rm_question_in_a_group_that_is_not_read(app_factory: AppFactory, fake: FakeTelegram,
                                                       cfg: dict[str, Any]) -> None:
    app = await rm_app(app_factory, fake, cfg)
    fake.post(GROUP, PLAYER, "note: not read")
    assert await ask(fake, GROUP, "/rm ?") == f"Группа {BARE}: роль other (по умолчанию)"
    await app.idle()
    # Only the command is stored, the rest of the group is still not read.
    texts = [r["text"] for r in await app.db.pool.fetch("SELECT text FROM messages WHERE chat_id = $1", GROUP)]
    assert texts == ["/rm ?"]


async def test_roles_survive_a_restart_without_postgres(app_factory: AppFactory, fake: FakeTelegram,
                                                        cfg: dict[str, Any], tmp_path: Path) -> None:
    await rm_app(app_factory, fake, cfg)
    await ask(fake, KASSA, f"/grole {BARE} tech")
    # The snapshot next to the buffer holds the roles: read before PostgreSQL is reachable.
    snapshot = Directory()
    assert snapshot.load(tmp_path / "directory.json")
    assert snapshot.groups == {BARE: "tech"} and snapshot.user_role(ADMIN) == "admin"
    assert snapshot.policy("tech") == "full" and snapshot.allowed(DEV) == {"*"}
