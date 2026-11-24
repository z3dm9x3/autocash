"""TS: tickets and C-ticket through Autosup, on the fake Telegram and a real PostgreSQL."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autocash.core.app import App
from autocash.modules.autosup.AS_module import AutosupModule
from autocash.modules.players import players_store
from autocash.modules.players.players_module import PlayersModule
from autocash.modules.role_manager.RM_module import RoleManagerModule
from autocash.modules.ticket_system import TS_codes
from autocash.modules.ticket_system.TS_module import TicketSystemModule
from tests.conftest import ACCOUNT, LOGS, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

P1, P2, P3, P4, P5 = 9401, 9402, 9403, 9404, 9405
CASHER = 9406
ADMIN = 8001


@pytest.fixture
def cfg(config: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    # A long first minute: a trigger must type the ticket at once, not when the minute ends.
    # The ID request of Autosup comes much later, so it does not get in the way.
    # The account works all day (AS, D-AS-8): no away reply, whatever the time of the run.
    folder = tmp_path / "accounts" / f"acc_{ACCOUNT} test"
    folder.mkdir(parents=True)
    (folder / "account.yaml").write_text('work_hours: "00:00-24:00"\n', encoding="utf-8")
    config["modules"] = {"role_manager": {}, "players": {},
                         "autosup": {"config": {"grace_s": 600.0, "accounts_dir": str(tmp_path / "accounts")}},
                         "ticket_system": {"config": {"burst_s": 60.0}}}
    return config


async def ts_app(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> App:
    for user in (P1, P2, P3, P4, P5, CASHER):
        if user not in fake.chats:
            fake.add_chat(user, "private", f"user {user}", members=(ACCOUNT,))
    rm = RoleManagerModule({ADMIN: "admin", CASHER: "casher", P5: "player"})
    return await started(app_factory(cfg=cfg, modules=[rm, PlayersModule(), AutosupModule(), TicketSystemModule()]))


async def tickets(app: App, tg_id: int) -> list[dict[str, Any]]:
    rows = await app.db.pool.fetch("SELECT state, ticket, reason, trigger_rules, trigger_text, messages "
                                   "FROM ts_tickets WHERE tg_id = $1 ORDER BY id", tg_id)
    return [{**dict(r), "trigger_rules": list(r["trigger_rules"])} for r in rows]


def logs(fake: FakeTelegram) -> list[str]:
    return [text for _, text in sent_by(fake, LOGS) if text.startswith("TS: ")]


async def typed(app: App, tg_id: int) -> bool:
    rows = await tickets(app, tg_id)
    return bool(rows) and rows[0]["ticket"] is not None


async def test_a_deposit_word_types_the_ticket_at_once_and_logs_it(app_factory: AppFactory, fake: FakeTelegram,
                                                                   cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P1, P1, "Здравствуйте, пополнил на 500, не пришло")
    await wait_for(lambda: typed(app, P1), timeout=10, what="C-ticket at once (the minute is 60 s)")
    assert await tickets(app, P1) == [{"state": "open", "ticket": "D", "reason": "triggers",
                                       "trigger_rules": ["d.popoln"], "trigger_text": "пополнил", "messages": 1}]
    await wait_for(lambda: logs(fake), what="the record in logs")
    assert logs(fake)[0].startswith("TS: тикет #") and f"— D · игрок {P1} · аккаунт " in logs(fake)[0]
    assert logs(fake)[0].endswith("· триггер: «пополнил»")
    assert [t for _, chat, t, _ in fake.sent if chat == P1] == []  # nothing to the player
    assert await app.db.pool.fetchval("SELECT protocol FROM protocol_runs WHERE protocol LIKE 'ticket.%'") == "ticket.d"


async def test_a_trigger_after_a_greeting_within_the_minute(app_factory: AppFactory, fake: FakeTelegram,
                                                            cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P2, P2, "Здравствуйте")
    fake.post(P2, P2, "уже сутки жду")  # "сутки" is no trigger
    fake.post(P2, P2, "Не пришёл деп")
    await wait_for(lambda: typed(app, P2), timeout=10, what="C-ticket")
    rows = await tickets(app, P2)
    assert [(r["ticket"], r["trigger_text"], r["messages"]) for r in rows] == [("D", "деп", 3)]


async def test_no_trigger_in_the_minute_is_q_and_q_stays_q(app_factory: AppFactory, fake: FakeTelegram,
                                                           cfg: dict[str, Any]) -> None:
    cfg["modules"]["ticket_system"]["config"]["burst_s"] = 1.0
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P3, P3, "Здравствуйте")
    await wait_for(lambda: typed(app, P3), what="Q after the minute")
    assert [(r["ticket"], r["reason"]) for r in await tickets(app, P3)] == [("Q", "none")]
    await wait_for(lambda: logs(fake), what="the record in logs")
    assert re.fullmatch(rf"TS: тикет #\d+ — Q · игрок {P3} · аккаунт .+ · триггер: нет за первую минуту",
                        logs(fake)[0])
    fake.post(P3, P3, "Не пришёл депозит")  # Q is not turned into D for now; tickets are not closed
    await app.idle()
    await app.protocols.run_pending()
    assert [(r["state"], r["ticket"], r["messages"]) for r in await tickets(app, P3)] == [("open", "Q", 2)]
    assert len(logs(fake)) == 1


async def test_withdrawal_mixed_and_the_first_trigger_wins(app_factory: AppFactory, fake: FakeTelegram,
                                                           cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P1, P1, "Вывод не пришёл за 24 часа")
    fake.post(P1, P1, "пополнял через СБП")  # a second trigger: the first one has typed the ticket already
    fake.post(P2, P2, "пополнял через тг бота, как вывести деньги?")
    await wait_for(lambda: typed(app, P1), timeout=10, what="C-ticket P1")
    await wait_for(lambda: typed(app, P2), timeout=10, what="C-ticket P2")
    await app.idle()
    assert [(r["ticket"], r["trigger_text"]) for r in await tickets(app, P1)] == [("WW", "вывод")]
    assert [(r["ticket"], r["reason"]) for r in await tickets(app, P2)] == [("Q", "mixed")]
    await wait_for(lambda: len(logs(fake)) == 2, what="two records in logs")
    assert any("— Q ·" in t and "депозит и вывод в одном сообщении" in t for t in logs(fake))


async def test_a_pdf_is_a_deposit_and_the_player_is_shown_with_nick_and_melid(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    await app.db.pool.execute("UPDATE rm_users SET username = 'nick5' WHERE tg_id = $1", P5)
    await players_store.add_link(app.db.pool, P5, "1234567890", source="test")
    fake.post(P5, P5, "", media_type="pdf")
    await wait_for(lambda: logs(fake), what="the record in logs")
    assert f"— D · игрок {P5} (@nick5, MelID 1234567890) · аккаунт " in logs(fake)[0]
    assert logs(fake)[0].endswith("· триггер: «PDF»")


async def test_staff_makes_no_tickets(app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    fake.post(CASHER, CASHER, "пополнение принято")
    fake.post(P4, P4, "депозит")
    await wait_for(lambda: typed(app, P4), timeout=10, what="C-ticket")
    assert await tickets(app, CASHER) == []


async def test_tickets_and_logs_do_not_depend_on_the_as_switch(app_factory: AppFactory, fake: FakeTelegram,
                                                               cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    await app.db.pool.execute("UPDATE as_settings SET enabled = false")
    next(m for m in app.modules if isinstance(m, AutosupModule)).enabled = False
    fake.post(P1, P1, "чек отправил, депозит не пришёл")
    await wait_for(lambda: logs(fake), what="the record in logs")
    assert [r["ticket"] for r in await tickets(app, P1)] == ["D"]
    assert [t for _, chat, t, _ in fake.sent if chat == P1] == []


async def test_messages_written_before_ts_started_make_no_tickets(app_factory: AppFactory, fake: FakeTelegram,
                                                                  cfg: dict[str, Any]) -> None:
    """A late delivery (catch-up after a restart, history) is not a ticket after the fact (D-TS-4)."""
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P1, P1, "Не пришёл депозит", date=datetime.now(UTC) - timedelta(hours=2))
    fake.post(P2, P2, "Не пришёл депозит")  # a live message: the pipeline is the same
    await wait_for(lambda: typed(app, P2), timeout=10, what="C-ticket of the live message")
    await app.idle()
    await app.protocols.run_pending()
    assert await tickets(app, P1) == []
    assert len(logs(fake)) == 1 and f"игрок {P2}" in logs(fake)[0]


async def test_a_code_from_the_base_of_codes_makes_ww(app_factory: AppFactory, fake: FakeTelegram,
                                                     cfg: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """D-TS-5: a code the base knows types the ticket WW at once, even without a withdrawal word."""
    async def known(conn: Any, codes: Any) -> str | None:
        return "tfVQ" if "tfVQ" in codes else None

    monkeypatch.setattr(TS_codes, "find", known)
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P1, P1, "Здравствуйте, tfVQ")
    await wait_for(lambda: typed(app, P1), timeout=10, what="C-ticket by the code")
    assert [(r["ticket"], r["reason"], r["trigger_text"]) for r in await tickets(app, P1)] == [("WW", "code", "tfVQ")]
    await wait_for(lambda: logs(fake), what="the record in logs")
    assert logs(fake)[0].endswith("· триггер: код вывода «tfVQ» найден в базе кодов")


async def test_a_code_the_base_does_not_know_leaves_it_to_the_triggers(app_factory: AppFactory, fake: FakeTelegram,
                                                                      cfg: dict[str, Any]) -> None:
    """The base is a stub now: a code is never found, the triggers decide; no trigger in the minute -> Q."""
    cfg["modules"]["ticket_system"]["config"]["burst_s"] = 3.0
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P1, P1, "1 758 918 313 / rywT")  # a code alone: not found, no trigger
    fake.post(P2, P2, "wp8s")
    fake.post(P2, P2, "пополнил вчера")  # a code not found, then a deposit word -> D
    await wait_for(lambda: typed(app, P2), timeout=10, what="C-ticket P2")
    await wait_for(lambda: typed(app, P1), what="Q after the minute")
    assert [(r["ticket"], r["reason"]) for r in await tickets(app, P1)] == [("Q", "none")]
    assert [(r["ticket"], r["trigger_text"]) for r in await tickets(app, P2)] == [("D", "пополнил")]
    codes = [list(r["codes"]) for r in await app.db.pool.fetch(
        "SELECT m.codes FROM ts_ticket_messages m JOIN ts_tickets t ON t.id = m.ticket_id "
        "WHERE t.tg_id = ANY($1::bigint[]) ORDER BY m.msg_date, m.msg_id", [P1, P2])]
    assert sorted(codes) == [[], ["rywT"], ["wp8s"]]


async def test_a_typed_ticket_is_not_searched_any_more(app_factory: AppFactory, fake: FakeTelegram,
                                                       cfg: dict[str, Any]) -> None:
    app = await ts_app(app_factory, fake, cfg)
    fake.post(P1, P1, "Не пришёл депозит")
    await wait_for(lambda: typed(app, P1), timeout=10, what="C-ticket")
    fake.post(P1, P1, "и вывод тоже, код tfVQ")
    await wait_for(lambda: _messages(app, P1, 2), what="the second message in the ticket")
    rows = await app.db.pool.fetch("SELECT d_rules, w_rules, codes FROM ts_ticket_messages m JOIN ts_tickets t "
                                   "ON t.id = m.ticket_id WHERE t.tg_id = $1 ORDER BY m.msg_id", P1)
    assert [(list(r["d_rules"]), list(r["w_rules"]), list(r["codes"])) for r in rows] == [
        (["d.deposit"], [], []), ([], [], [])]  # after D nothing is looked for
    assert [r["ticket"] for r in await tickets(app, P1)] == ["D"]


async def _messages(app: App, tg_id: int, n: int) -> bool:
    return any(r["messages"] >= n for r in await tickets(app, tg_id))
