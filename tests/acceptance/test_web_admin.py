"""Stage 5: the web admin (criteria 1-3, access, sending through the outbound queue)."""

from __future__ import annotations

import copy
import html
import re
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import yaml

from autocash.core.app import App
from autocash.core.config.loader import ConfigStore
from autocash.web.auth import hash_password
from tests.conftest import ACCOUNT, KASSA, KASSA2, PLAYER, AppFactory, write_config
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001
MANAGER = 8002
PASSWORD = "correct horse battery"


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"], MANAGER: ["manager"]}
    config["web"] = {"enabled": True, "port": 0, "git_commit": False}
    config["modules"] = {"demo": {}}
    return config


async def web_app(app_factory: AppFactory, cfg: dict[str, Any]) -> App:
    app = await started(app_factory(cfg=cfg))
    for user in (ADMIN, MANAGER):
        await app.db.pool.execute("INSERT INTO web_users (tg_id, password_hash) VALUES ($1, $2)",
                                  user, hash_password(PASSWORD))
    return app


def base(app: App) -> str:
    return f"http://127.0.0.1:{app.web.port}"


async def login(app: App, user: int = ADMIN, password: str = PASSWORD) -> tuple[aiohttp.ClientSession, str]:
    session = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True))
    async with session.post(base(app) + "/login", data={"tg_id": str(user), "password": password}) as r:
        page = await r.text()
    m = re.search(r'name="csrf" value="([^"]+)"', page)
    return session, m.group(1) if m else ""


async def post(session: aiohttp.ClientSession, app: App, path: str, data: dict[str, str]) -> tuple[int, str]:
    async with session.post(base(app) + path, data=data) as r:
        return r.status, await r.text()


# ---- criterion 1: a change through the admin equals the same edit of the file -------------------------------
async def test_change_through_admin_equals_file_edit(app_factory: AppFactory, cfg: dict[str, Any],
                                                     tmp_path: Path) -> None:
    app = await web_app(app_factory, cfg)
    session, csrf = await login(app)
    async with session:
        before = app.store.current.version
        status, page = await post(session, app, "/config/source", {
            "csrf": csrf, "base_version": before, "chat_id": str(KASSA2), "name": "kassa 2",
            "roles": "kassa payment", "template": "kassa", "policy": "", "priority": "5"})
        assert status == 200 and "Применено" in page, page[-2000:]
        status, page = await post(session, app, "/config/toggle", {
            "csrf": csrf, "base_version": app.store.current.version, "kind": "module", "id": "demo", "value": "off"})
        assert "Применено" in page
    # The same edit made by hand in a file gives the same version.
    by_hand = copy.deepcopy(cfg)
    next(s for s in by_hand["sources"] if s["chat_id"] == KASSA2)["roles"] = ["kassa", "payment"]
    by_hand["modules"] = {"demo": {"enabled": False}}
    hand = ConfigStore(write_config(tmp_path / "hand.yaml", by_hand))
    assert app.store.current.version == hand.current.version != before
    assert app.store.current.config.modules["demo"].enabled is False
    # The file on disk holds the change: a restart reads the same configuration.
    assert ConfigStore(app.store.path).current.version == hand.current.version
    # The version is in the journal, with the author of the change.
    await wait_for(lambda: _has_version(app, hand.current.version), what="config_versions row")
    rows = await app.db.pool.fetch("SELECT author, version_before, version_after, diff FROM config_changes ORDER BY id")
    assert [r["author"] for r in rows] == [ADMIN, ADMIN]
    assert rows[0]["version_before"] == before and rows[-1]["version_after"] == hand.current.version
    assert "payment" in rows[0]["diff"]


async def _has_version(app: App, version: str) -> bool:
    return bool(await app.db.pool.fetchval("SELECT count(*) FROM config_versions WHERE version = $1", version))


# ---- criterion 2: an invalid change is refused, the configuration stays ----------------------------------------
async def test_invalid_change_is_refused_with_a_clear_error(app_factory: AppFactory, cfg: dict[str, Any]) -> None:
    app = await web_app(app_factory, cfg)
    file_before = app.store.path.read_bytes()
    version = app.store.current.version
    session, csrf = await login(app)
    async with session:
        sources = yaml.safe_dump([{"chat_id": KASSA, "roles": ["kassa"]}, {"chat_id": KASSA, "roles": ["x"]}])
        status, page = await post(session, app, "/config/section",
                                  {"csrf": csrf, "base_version": version, "key": "sources", "text": sources})
        assert status == 200
        assert "изменение отклонено" in page and "duplicate source chat_id" in page
        assert html.escape(sources.splitlines()[0]) in page  # the text stays in the form to be fixed
        _, page = await post(session, app, "/config/section",
                             {"csrf": csrf, "base_version": version, "key": "queue", "text": "maxsize: [1, 2"})
        assert "ошибка YAML" in page
        _, page = await post(session, app, "/config/section",
                             {"csrf": csrf, "base_version": version, "key": "reactions",
                              "text": "rules:\n- id: r1\n  when: {type: [x]}\n  if: {bogus: [1, 2]}\n"})
        assert "изменение отклонено" in page and "bogus" in page
        # A change made from a stale page is refused too (another admin changed the config meanwhile).
        _, page = await post(session, app, "/config/toggle",
                             {"csrf": csrf, "base_version": "000000000000", "kind": "module", "id": "demo",
                              "value": "off"})
        assert "конфигурация уже изменилась" in page
    assert app.store.current.version == version
    assert app.store.path.read_bytes() == file_before
    failed = await app.db.pool.fetchval("SELECT count(*) FROM admin_audit WHERE action = 'config' AND NOT ok")
    assert failed == 4
    assert await app.db.pool.fetchval("SELECT count(*) FROM config_changes") == 0


# ---- criterion 3: traces and the pool are the same as /trace and /accounts --------------------------------
async def test_trace_and_pool_pages_match_the_commands(app_factory: AppFactory, fake: FakeTelegram,
                                                       cfg: dict[str, Any]) -> None:
    app = await web_app(app_factory, cfg)
    fake.post(KASSA, PLAYER, "/ping")
    await wait_for(lambda: sent_by(fake, KASSA) == [(ACCOUNT, "pong")], what="pong")
    await app.idle()
    trace_id = str(await app.db.pool.fetchval("SELECT trace_id FROM messages WHERE text = '/ping'"))
    session, _ = await login(app)
    async with session:
        async with session.get(base(app) + f"/trace?q={trace_id}") as r:
            page = await r.text()
        assert html.escape(await app.commands.run("trace", [trace_id])) in page
        async with session.get(base(app) + f"/trace?chat={KASSA}") as r:
            listing = await r.text()
        assert trace_id in listing
        for _ in range(3):  # /accounts shows ages in seconds: compare texts built at the same second
            async with session.get(base(app) + "/pool") as r:
                pool_page = await r.text()
            if html.escape(app.commands.accounts()) in pool_page:
                break
        else:
            pytest.fail("the pool page differs from /accounts")


# ---- access ----------------------------------------------------------------------------------------------------
async def test_access_only_for_admin_roles_and_with_csrf(app_factory: AppFactory, cfg: dict[str, Any]) -> None:
    app = await web_app(app_factory, cfg)
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as anon:
        async with anon.get(base(app) + "/config") as r:
            assert str(r.url).endswith("/login")
        async with anon.post(base(app) + "/send", data={"chat": str(KASSA), "text": "x"}) as r:
            assert r.status == 403
    wrong, _ = await login(app, ADMIN, "not the password")
    async with wrong, wrong.get(base(app) + "/") as r:
        assert str(r.url).endswith("/login")
    manager, _ = await login(app, MANAGER)  # a manager has no web rights by default
    async with manager, manager.get(base(app) + "/") as r:
        assert str(r.url).endswith("/login")
    session, csrf = await login(app)
    async with session:
        status, _ = await post(session, app, "/config/toggle",
                               {"csrf": "forged", "kind": "module", "id": "demo", "value": "off"})
        assert status == 403
        status, _ = await post(session, app, "/logout", {"csrf": csrf})
        async with session.get(base(app) + "/") as r:
            assert str(r.url).endswith("/login")
    assert app.store.current.config.modules["demo"].enabled is True
    logins = await app.db.pool.fetchval("SELECT count(*) FROM admin_audit WHERE action = 'login'")
    assert logins == 1  # only the admin got in: not the wrong password, not the manager


# ---- sending goes through the outbound queue ---------------------------------------------------------------
async def test_send_goes_through_the_outbound_queue(app_factory: AppFactory, fake: FakeTelegram,
                                                    cfg: dict[str, Any]) -> None:
    app = await web_app(app_factory, cfg)
    session, csrf = await login(app)
    async with session:
        status, page = await post(session, app, "/send", {"csrf": csrf, "chat": str(KASSA), "text": "привет из админки",
                                                          "reply_to": ""})
        assert status == 200 and "Поставлено в очередь отправки" in page
    await wait_for(lambda: sent_by(fake, KASSA) == [(ACCOUNT, "привет из админки")], what="sent")
    row = await app.db.pool.fetchrow("SELECT module, rule, kind, status FROM outbound_tasks")
    assert (row["module"], row["rule"], row["kind"]) == ("web", "web.send", "notify")
    assert await app.db.pool.fetchval("SELECT author FROM admin_audit WHERE action = 'send'") == ADMIN
