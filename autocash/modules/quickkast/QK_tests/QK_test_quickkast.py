"""QK: QuickKast on the fake Telegram and a real PostgreSQL (QK_readme.md, D-QK-1..5)."""

from __future__ import annotations

import copy
import time
from typing import Any

import pytest

from autocash.core.app import App
from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.modules.quickkast.QK_catalog import Catalog
from autocash.modules.quickkast.QK_module import QuickKastModule
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, KASSA, KASSA2, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

CLIENT = 9301  # the dialog of the operator (account ACCOUNT) with a client
OTHER = 9302
ADMIN = 8001
TECH = KASSA2


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["modules"] = {"role_manager": {}, "quickkast": {}}
    return config


async def qk_app(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> tuple[App, QuickKastModule]:
    for user in (CLIENT, OTHER):
        if user not in fake.chats:
            fake.add_chat(user, "private", f"user {user}", members=(ACCOUNT,))
    qk = QuickKastModule()
    app = await started(app_factory(cfg=cfg, modules=[RoleManagerModule({ADMIN: "admin"}), qk]))
    pool = app.db.pool
    await qk.catalog.add_variant(pool, "standard", "прив", "Здравствуйте! Чем могу помочь?")
    await qk.catalog.add_variant(pool, "standard", "реки", "Реквизиты: вариант 1")
    await qk.catalog.add_variant(pool, "standard", "реки", "Реквизиты: вариант 2")
    return app, qk


def texts(fake: FakeTelegram, chat: int) -> list[str]:
    return [text for _, text in sent_by(fake, chat)]


async def events(app: App) -> list[Any]:
    return list(await app.db.pool.fetch("SELECT * FROM qk_events ORDER BY id"))


# ---- the catalog (pure) -----------------------------------------------------------------------------
def test_resolve_variants_list_and_missing() -> None:
    cat = Catalog()
    cat.templates[("standard", "реки")] = [(1, "один"), (2, "два")]
    assert cat.resolve("нет такого") is None
    one = cat.resolve("реки-2")
    assert one is not None and (one.kind, one.template, one.variant) == ("template", "два", 2)
    listed = cat.resolve("реки-list")
    assert listed is not None and listed.kind == "list" and listed.count == 2
    missing = cat.resolve("реки-5")
    assert missing is not None and missing.kind == "missing_variant" and missing.count == 2
    rnd = cat.resolve("реки")
    assert rnd is not None and rnd.template in ("один", "два")
    assert cat.resolve("Реки") is None  # the trigger is exact


# ---- the express path ---------------------------------------------------------------------------------
async def test_trigger_is_deleted_and_the_template_sent(app_factory: AppFactory, fake: FakeTelegram,
                                                        cfg: dict[str, Any]) -> None:
    app, _ = await qk_app(app_factory, fake, cfg)
    trigger = fake.post(CLIENT, ACCOUNT, "прив")  # the operator typed it on the account
    await wait_for(lambda: texts(fake, CLIENT) == ["Здравствуйте! Чем могу помочь?"], what="the template")
    await wait_for(lambda: (ACCOUNT, CLIENT, trigger.id_for(ACCOUNT)) in fake.deletes, what="the delete")
    await wait_for(lambda: events(app), what="qk_events")
    [ev] = await events(app)
    assert (ev["kind"], ev["trigger"], ev["status"], ev["variant"]) == ("template", "прив", "done", 1)
    assert ev["received_at"] <= ev["matched_at"] <= ev["stored_at"] <= ev["sent_at"]
    await app.idle()
    rows = await app.db.pool.fetch("SELECT rule, priority, status, binding FROM outbound_tasks WHERE module = "
                                   "'quickkast' ORDER BY id")
    assert [(r["rule"], r["priority"], r["status"]) for r in rows] == [("qk.delete", 0, "delivered"),
                                                                       ("qk.send", 0, "delivered")]


async def test_numbered_and_list_variants(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    await qk_app(app_factory, fake, cfg)
    fake.post(CLIENT, ACCOUNT, "реки-2")
    await wait_for(lambda: texts(fake, CLIENT) == ["Реквизиты: вариант 2"], what="variant 2")
    fake.post(CLIENT, ACCOUNT, "реки-list")
    await wait_for(lambda: len(texts(fake, CLIENT)) == 2, what="the list")
    assert texts(fake, CLIENT)[1].startswith("QuickKast реки: 2 вариант(а):")
    fake.post(CLIENT, ACCOUNT, "реки-7")
    await wait_for(lambda: len(texts(fake, CLIENT)) == 3, what="missing variant")
    assert texts(fake, CLIENT)[2] == "Для триггера реки доступно только 2 вариант(а)."


async def test_not_a_trigger_and_not_the_operator_are_left_alone(app_factory: AppFactory, fake: FakeTelegram,
                                                                  cfg: dict[str, Any]) -> None:
    app, _ = await qk_app(app_factory, fake, cfg)
    fake.post(CLIENT, CLIENT, "прив")  # the client wrote it: not a trigger
    fake.post(CLIENT, ACCOUNT, "прив, как дела")  # not exact
    fake.post(KASSA, PLAYER, "прив")  # a group
    await app.idle()
    assert texts(fake, CLIENT) == [] and fake.deletes == [] and await events(app) == []


async def test_template_echo_does_not_fire_again(app_factory: AppFactory, fake: FakeTelegram,
                                                 cfg: dict[str, Any]) -> None:
    app, qk = await qk_app(app_factory, fake, cfg)
    await qk.catalog.add_variant(app.db.pool, "standard", "эхо", "эхо")  # the template equals its trigger
    fake.post(CLIENT, ACCOUNT, "эхо")
    await wait_for(lambda: texts(fake, CLIENT) == ["эхо"], what="the template")
    await app.idle()
    assert texts(fake, CLIENT) == ["эхо"] and len(fake.deletes) == 1  # our own send is origin=system


async def test_x_switches_the_dialog_off_and_v_on(app_factory: AppFactory, fake: FakeTelegram,
                                                  cfg: dict[str, Any]) -> None:
    app, _ = await qk_app(app_factory, fake, cfg)
    fake.post(CLIENT, ACCOUNT, "х")  # Cyrillic
    await wait_for(lambda: len(fake.deletes) == 1, what="x deleted")
    fake.post(CLIENT, ACCOUNT, "прив")
    fake.post(OTHER, ACCOUNT, "прив")  # another dialog is not affected
    await wait_for(lambda: texts(fake, OTHER) == ["Здравствуйте! Чем могу помочь?"], what="other dialog")
    await app.idle()
    assert texts(fake, CLIENT) == []
    await wait_for(lambda: app.db.pool.fetchval("SELECT NOT enabled FROM qk_chat_state WHERE chat_id = $1", CLIENT),
                   what="the switch stored")
    fake.post(CLIENT, ACCOUNT, "v")
    await wait_for(lambda: len(fake.deletes) == 3, what="v deleted")
    fake.post(CLIENT, ACCOUNT, "прив")
    await wait_for(lambda: texts(fake, CLIENT) == ["Здравствуйте! Чем могу помочь?"], what="on again")


async def test_paused_account_leaves_the_trigger_and_tells_logs(app_factory: AppFactory, fake: FakeTelegram,
                                                                cfg: dict[str, Any],
                                                                caplog: pytest.LogCaptureFixture) -> None:
    app, _ = await qk_app(app_factory, fake, cfg)
    app.pool.accounts[ACCOUNT].limiter.pause("send", 30)  # Telegram's FloodWait on sends
    fake.post(CLIENT, ACCOUNT, "прив")
    await wait_for(lambda: events(app), what="qk_events")
    [ev] = await events(app)
    assert (ev["status"], ev["reason"]) == ("refused", "FLOOD_WAIT")
    assert texts(fake, CLIENT) == [] and fake.deletes == []  # a late template is worse than none (D-QK-3)
    # The only account is paused, so the notice waits for it in the queue of `logs`; it is issued at once.
    assert any("[logs] QK: триггер «прив»" in r.getMessage() for r in caplog.records)


async def test_qk_goes_first_through_a_slow_antiflood_and_a_busy_account(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    """D-QK-2: the live limits (20 writes a minute, 2 s per chat, 1 API call a second) and a queue of
    regular tasks of the account: QK still answers at once, and the regular tasks wait after it."""
    slow = copy.deepcopy(cfg)
    slow["outbox"]["antiflood"]["user"] = {"per_account_per_minute": 20, "per_chat_interval_s": 2.0,
                                           "group_per_minute": 10}
    slow["pool"]["api_rate_per_s"] = {"user": 1.0, "bot": 1.0}
    app, _ = await qk_app(app_factory, fake, slow)
    create = app.modules[0].services.outbox  # type: ignore[union-attr]
    assert create is not None
    for i in range(5):
        await create(OutboundDraft(chat_id=OTHER, text=f"обычная задача {i}", rule="test", kind="client_reply",
                                   via_account=ACCOUNT, dedup_key=f"test:{i}"), "test")
    await wait_for(lambda: len(texts(fake, OTHER)) >= 1, what="the regular queue runs")
    start = time.monotonic()
    fake.post(CLIENT, ACCOUNT, "прив")
    await wait_for(lambda: texts(fake, CLIENT) == ["Здравствуйте! Чем могу помочь?"] and len(fake.deletes) == 1,
                   timeout=2.0, what="QK through the slow limits")
    took = time.monotonic() - start
    assert took < 0.5, took
    assert len(texts(fake, OTHER)) < 5  # the regular tasks are still paced
    await wait_for(lambda: events(app), what="qk_events")
    [ev] = await events(app)
    total = (max(ev["deleted_at"], ev["sent_at"]) - ev["received_at"]).total_seconds()
    assert ev["status"] == "done" and total < 0.5


async def test_same_update_twice_fires_once(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app, _ = await qk_app(app_factory, fake, cfg)
    msg = fake.post(CLIENT, ACCOUNT, "прив")
    fake._push("new", msg, fake.chats[CLIENT])  # the same update again (push + difference)
    await wait_for(lambda: len(texts(fake, CLIENT)) == 1, what="the template")
    await app.idle()
    assert len(texts(fake, CLIENT)) == 1 and len(await events(app)) == 1


# ---- /qk ---------------------------------------------------------------------------------------------
async def test_commands_in_a_tech_group(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app, qk = await qk_app(app_factory, fake, cfg)
    await app.db.pool.execute("INSERT INTO rm_groups (group_tg_id, role) VALUES ($1, 'tech')", 1000000002)
    rm = next(m for m in app.modules if isinstance(m, RoleManagerModule))
    await rm.reload()

    async def ask(chat: int, text: str, user: int = ADMIN, **kw: Any) -> str:
        before = len(sent_by(fake, chat))
        fake.post(chat, user, text, **kw)
        await wait_for(lambda: len(sent_by(fake, chat)) > before, what=f"answer to {text}")
        return sent_by(fake, chat)[-1][1]

    assert "только в группе с ролью tech" in await ask(KASSA, "/qk list")
    assert await ask(TECH, "/qk add пока До свидания!\nХорошего дня") == \
        "QuickKast «пока», вариант 1, сохранён в профиле standard."
    assert qk.catalog.variants("standard", "пока") == [(1, "До свидания!\nХорошего дня")]
    note = fake.post(TECH, ADMIN, "Спасибо за ожидание")
    answer = await ask(TECH, "/qk add пока", reply_to=note.id_for(0))
    assert answer.endswith("вариант 2, сохранён в профиле standard.")
    listing = await ask(TECH, "/qk list")
    assert "пока-2 — Спасибо за ожидание" in listing and "реки-1 — Реквизиты: вариант 1" in listing
    assert await ask(TECH, "/qk delete пока-1") == "QuickKast «пока-1» удалён из профиля standard."
    assert qk.catalog.variants("standard", "пока") == [(1, "Спасибо за ожидание")]
    assert "создан" in await ask(TECH, "/qk add profile Ночь")
    assert await ask(TECH, "/qk set ночь") == "Активный профиль QuickKast: Ночь."
    await ask(TECH, "/qk add прив Доброй ночи")
    fake.post(CLIENT, ACCOUNT, "прив")
    await wait_for(lambda: texts(fake, CLIENT) == ["Доброй ночи"], what="the template of the profile")
    assert await ask(TECH, "/qk set") == "Активный профиль QuickKast: standard."
    assert (await ask(TECH, "/qk off")).startswith("QuickKast: OFF")
    fake.post(CLIENT, ACCOUNT, "прив")
    await app.idle()
    assert texts(fake, CLIENT) == ["Доброй ночи"]
    assert (await ask(TECH, "/qk stats")).startswith("QuickKast за 24 часа: 1 шаблонов")
    before = len(sent_by(fake, TECH))
    fake.post(TECH, PLAYER, "/qk on")  # no rights: nothing in the chat
    await app.idle()
    assert len(sent_by(fake, TECH)) == before
    # A restart reads the catalog back.
    fresh = Catalog()
    await fresh.load(app.db.pool)
    assert fresh.active == "standard" and not fresh.enabled
    assert fresh.variants("Ночь", "прив") == [(1, "Доброй ночи")]
