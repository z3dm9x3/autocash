"""QK2: QuickCast2 on the fake Telegram and a real PostgreSQL (QK2_readme.md, D-QK2-1..4)."""

from __future__ import annotations

import copy
import time
from typing import Any

import pytest

from autocash.core.app import App
from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.modules.quickcast2.QK2_module import QuickCast2Module
from autocash.modules.quickkast.QK_module import QuickKastModule
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, KASSA2, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

CLIENT = 9401
OTHER = 9402
ADMIN = 8001
TECH = KASSA2
HELLO = "Здравствуйте! Чем могу помочь?"


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["modules"] = {"role_manager": {}, "quickkast": {}, "quickcast2": {}}
    return config


async def qk2_app(app_factory: AppFactory, fake: FakeTelegram,
                  cfg: dict[str, Any]) -> tuple[App, QuickKastModule, QuickCast2Module]:
    for user in (CLIENT, OTHER):
        if user not in fake.chats:
            fake.add_chat(user, "private", f"user {user}", members=(ACCOUNT,))
    qk, qk2 = QuickKastModule(), QuickCast2Module()
    app = await started(app_factory(cfg=cfg, modules=[RoleManagerModule({ADMIN: "admin"}), qk, qk2]))
    pool = app.db.pool
    await qk.catalog.add_variant(pool, "standard", "зд", HELLO)
    await qk.catalog.add_variant(pool, "standard", "реки", "Реквизиты: вариант 1")
    await qk.catalog.add_variant(pool, "standard", "реки", "Реквизиты: вариант 2")
    return app, qk, qk2


def texts(fake: FakeTelegram, chat: int) -> list[str]:
    return [text for _, text in sent_by(fake, chat)]


async def events(app: App) -> list[Any]:
    return list(await app.db.pool.fetch("SELECT * FROM qk2_events ORDER BY id"))


async def test_one_catalog_with_quickkast(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    _, qk, qk2 = await qk2_app(app_factory, fake, cfg)
    assert qk2.catalog is qk.catalog  # D-QK2-3: a /qk change is seen by both at once


async def test_draft_with_backslash_sends_the_template_and_empties_the_draft(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app, _, _ = await qk2_app(app_factory, fake, cfg)
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")
    await wait_for(lambda: texts(fake, CLIENT) == [HELLO], what="the template")
    assert fake.drafts[(ACCOUNT, CLIENT)] == ""  # sent with clear_draft: the input field is emptied
    assert fake.deletes == []  # nothing was sent, nothing is deleted
    await wait_for(lambda: events(app), what="qk2_events")
    [ev] = await events(app)
    assert (ev["entered"], ev["kind"], ev["trigger"], ev["status"]) == ("зд\\", "template", "зд", "done")
    assert ev["received_at"] <= ev["matched_at"] <= ev["stored_at"] <= ev["sent_at"]
    await app.idle()
    rows = await app.db.pool.fetch("SELECT rule, priority, status FROM outbound_tasks WHERE module = 'quickcast2'")
    assert [(r["rule"], r["priority"], r["status"]) for r in rows] == [("qk2.send", 0, "delivered")]


async def test_variants_list_and_missing(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    await qk2_app(app_factory, fake, cfg)
    fake.set_draft(ACCOUNT, CLIENT, "реки-1\\")
    await wait_for(lambda: texts(fake, CLIENT) == ["Реквизиты: вариант 1"], what="variant 1")
    fake.set_draft(ACCOUNT, CLIENT, "реки-list\\")
    await wait_for(lambda: len(texts(fake, CLIENT)) == 2, what="the list")
    assert texts(fake, CLIENT)[1].startswith("QuickKast реки: 2 вариант(а):")
    fake.set_draft(ACCOUNT, CLIENT, "реки-9\\")
    await wait_for(lambda: len(texts(fake, CLIENT)) == 3, what="missing variant")
    assert texts(fake, CLIENT)[2] == "Для триггера реки доступно только 2 вариант(а)."


async def test_drafts_that_are_not_triggers_are_left_alone(app_factory: AppFactory, fake: FakeTelegram,
                                                           cfg: dict[str, Any]) -> None:
    app, _, _ = await qk2_app(app_factory, fake, cfg)
    fake.set_draft(ACCOUNT, CLIENT, "зд")  # still typing: no backslash
    fake.set_draft(ACCOUNT, CLIENT, "нет такого\\")
    fake.set_draft(ACCOUNT, CLIENT, "\\")
    fake.set_draft(ACCOUNT, ACCOUNT, "зд\\")  # Saved Messages
    fake.post(CLIENT, ACCOUNT, "зд\\")  # a SENT message is not a draft (QuickKast does not know "зд\" either)
    await app.idle()
    assert texts(fake, CLIENT) == [] and await events(app) == []
    assert fake.drafts[(ACCOUNT, CLIENT)] == "\\"  # nobody touched the draft


async def test_the_same_draft_synced_twice_fires_once(app_factory: AppFactory, fake: FakeTelegram,
                                                      cfg: dict[str, Any]) -> None:
    app, _, qk2 = await qk2_app(app_factory, fake, cfg)
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")  # the client saved it again before the template went out
    await wait_for(lambda: texts(fake, CLIENT) == [HELLO], what="the template")
    await app.idle()
    assert texts(fake, CLIENT) == [HELLO] and len(await events(app)) == 1 and qk2.fired == 1


async def test_x_of_quickkast_switches_quickcast2_off_in_the_dialog(app_factory: AppFactory, fake: FakeTelegram,
                                                                   cfg: dict[str, Any]) -> None:
    await qk2_app(app_factory, fake, cfg)
    fake.post(CLIENT, ACCOUNT, "x")
    await wait_for(lambda: len(fake.deletes) == 1, what="x handled by QuickKast")
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")
    fake.set_draft(ACCOUNT, OTHER, "зд\\")
    await wait_for(lambda: texts(fake, OTHER) == [HELLO], what="another dialog")
    assert texts(fake, CLIENT) == []


async def test_paused_account_leaves_the_draft(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any],
                                               caplog: pytest.LogCaptureFixture) -> None:
    app, _, _ = await qk2_app(app_factory, fake, cfg)
    app.pool.accounts[ACCOUNT].limiter.pause("send", 30)
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")
    await wait_for(lambda: events(app), what="qk2_events")
    [ev] = await events(app)
    assert (ev["status"], ev["reason"]) == ("refused", "FLOOD_WAIT")
    assert texts(fake, CLIENT) == [] and fake.drafts[(ACCOUNT, CLIENT)] == "зд\\"
    assert any("[logs] QK2: черновик «зд\\»" in r.getMessage() for r in caplog.records)


async def test_qk2_goes_first_through_a_slow_antiflood(app_factory: AppFactory, fake: FakeTelegram,
                                                       cfg: dict[str, Any]) -> None:
    slow = copy.deepcopy(cfg)
    slow["outbox"]["antiflood"]["user"] = {"per_account_per_minute": 20, "per_chat_interval_s": 2.0,
                                           "group_per_minute": 10}
    slow["pool"]["api_rate_per_s"] = {"user": 1.0, "bot": 1.0}
    app, _, _ = await qk2_app(app_factory, fake, slow)
    create = app.modules[0].services.outbox  # type: ignore[union-attr]
    assert create is not None
    for i in range(5):
        await create(OutboundDraft(chat_id=OTHER, text=f"обычная задача {i}", rule="test", kind="client_reply",
                                   via_account=ACCOUNT, dedup_key=f"test:{i}"), "test")
    await wait_for(lambda: len(texts(fake, OTHER)) >= 1, what="the regular queue runs")
    start = time.monotonic()
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")
    await wait_for(lambda: texts(fake, CLIENT) == [HELLO], timeout=2.0, what="QK2 through the slow limits")
    assert time.monotonic() - start < 0.5
    assert len(texts(fake, OTHER)) < 5


async def test_commands(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app, _, qk2 = await qk2_app(app_factory, fake, cfg)
    await app.db.pool.execute("INSERT INTO rm_groups (group_tg_id, role) VALUES ($1, 'tech')", 1000000002)
    rm = next(m for m in app.modules if isinstance(m, RoleManagerModule))
    await rm.reload()

    async def ask(text: str) -> str:
        before = len(sent_by(fake, TECH))
        fake.post(TECH, ADMIN, text)
        await wait_for(lambda: len(sent_by(fake, TECH)) > before, what=f"answer to {text}")
        return sent_by(fake, TECH)[-1][1]

    assert (await ask("/qk2")).startswith("QuickCast2 — шаблон без отправки триггера")
    assert (await ask("/qk2 off")).startswith("QuickCast2: OFF")
    fake.set_draft(ACCOUNT, CLIENT, "зд\\")
    await app.idle()
    assert texts(fake, CLIENT) == [] and qk2.enabled is False
    assert await app.db.pool.fetchval("SELECT enabled FROM qk2_settings") is False
    assert (await ask("/qk2 on")).startswith("QuickCast2: ON")
    fake.set_draft(ACCOUNT, CLIENT, "зд-1\\")
    await wait_for(lambda: texts(fake, CLIENT) == [HELLO], what="on again")
    await wait_for(lambda: events(app), what="qk2_events")
    assert "1 шаблонов" in await ask("/qk2 stats")
