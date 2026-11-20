"""Stage 2: modules "deposits" and "players" on a PROVISIONAL kassa template.

The texts below follow the TZ and the examples of the old project. The exact wording of the
bot's confirmation, of a cancellation and of the /turn_off answer is not known: replace them
with recorded messages (tests/fixtures/messages/README.md) when they arrive; the module logic
does not depend on the wording, only the template does.
"""

from __future__ import annotations

import asyncio
import copy
import time
from datetime import UTC, datetime
from typing import Any

import pytest

from autocash.core.app import App
from autocash.core.record import MessageRecord
from autocash.modules.api import Context, Event
from autocash.modules.deposits.module import DepositsModule
from autocash.modules.players import PL_store
from autocash.modules.players.PL_module import PlayersModule
from tests.conftest import BOT, CASHER, KASSA, KASSA2, LOGS, AppFactory, write_config
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001
TG, MEL = "7287295969", "1481598133"

KASSA_TEMPLATE: list[dict[str, Any]] = [
    {"id": "deposit_request", "type": "deposit.request",
     "when": {"sender_role": ["kassa_bot"], "header_regex": r"^ДЕПОЗИТ\s+\S"},
     "fields": {"deposit_id": "Deposit ID", "tgid": "Пользователь.id", "username": "Пользователь.name",
                "mel_id": "Номер счёта", "amount": "Сумма пополнения", "method": "Метод оплаты.method",
                "cancelled_by": "Отменил", "cancel_reason": "Причина"},
     "order_key": ["deposit_id"], "priority": 3},
    {"id": "deposit_receipt", "type": "deposit.receipt",
     "when": {"sender_role": ["kassa_bot"], "header": ["ДЕПОЗИТ"]},
     "fields": {"deposit_id": "Deposit ID", "amount": "Сумма", "markers": "$markers"}, "order_key": ["deposit_id"]},
    {"id": "deposit_sent", "type": "deposit.sent",  # PROVISIONAL wording
     "when": {"sender_role": ["kassa_bot"], "text_regex": "^Реквизиты успешно отправлены"},
     "fields": {"tgid": "ID", "bonus": "Бонус", "total": "Итоговая сумма"}},
    {"id": "requisites_card", "type": "deposit.requisites", "when": {"sender_role": ["casher"], "has_all": ["card"]}},
    {"id": "requisites_phone", "type": "deposit.requisites", "when": {"sender_role": ["casher"], "has_all": ["phone"]}},
    {"id": "method_off", "type": "method.off",  # PROVISIONAL wording
     "when": {"sender_role": ["kassa_bot"], "text_regex": "^Метод отключ"},
     "fields": {"method": "Метод", "method_id": "ID метода"}},
    {"id": "method_on", "type": "method.on",  # PROVISIONAL wording
     "when": {"sender_role": ["kassa_bot"], "text_regex": "^Метод включ"},
     "fields": {"method": "Метод", "method_id": "ID метода"}},
]


def request(did: str, tg: str = TG, mel: str = MEL, amount: str = "1 000.00", extra: str = "") -> str:
    return (f"[ДЕПОЗИТ ТРЕУГОЛЬНИК]\nDeposit ID: {did}\nПользователь: @x ID: {tg}\nТип пользователя: USER\n"
            f"Номер счёта: {mel}\nСумма пополнения: {amount}\nМетод оплаты: Top Pay 1 000 - 500 000{extra}")


def trusted(tg: str = TG, amount: str = "1 000.00") -> str:
    return (f"[ДЕПОЗИТ ПРЕДУСТАНОВЛЕННАЯ СУММА]\nПользователь: @x ID: {tg}\nТип пользователя: TRUSTED\n"
            f"Номер счёта: {MEL}\nСумма пополнения: {amount}\nМетод оплаты: Top Pay 1 000 - 500 000")


def sent(tg: str = TG, bonus: str = "50", total: str = "1 050.00") -> str:
    return f"Реквизиты успешно отправлены пользователю\nID: {tg}\nБонус: {bonus}\nИтоговая сумма: {total}"


def receipt(did: str, amount: str = "1 050.00") -> str:
    return f"[ДЕПОЗИТ]\nDeposit ID: {did}\nСумма: {amount}"


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"]}
    base = [r for r in config["templates"]["kassa"]["rules"] if r["id"] not in {t["id"] for t in KASSA_TEMPLATE}]
    config["templates"]["kassa"]["rules"] = KASSA_TEMPLATE + base
    config["modules"] = {"deposits": {"config": {}}, "players": {"config": {}}}
    return config


async def make(app_factory: AppFactory, cfg: dict[str, Any], binding: list[str] | None = None) -> App:
    if binding is not None:
        cfg["modules"]["deposits"]["config"]["binding"] = binding
    return await started(app_factory(cfg=cfg, modules=[DepositsModule(), PlayersModule()]))


async def dep(app: App, did: str) -> Any:
    return await app.db.pool.fetchrow("SELECT * FROM deposits WHERE deposit_id = $1", did)


async def ask(app: App, fake: FakeTelegram, text: str, chat: int = KASSA) -> str:
    before = len(sent_by(fake, chat))
    fake.post(chat, ADMIN, text)
    await wait_for(lambda: len(sent_by(fake, chat)) > before, what=f"reply to {text}")
    return sent_by(fake, chat)[before][1]


# ---- the whole life of a request ------------------------------------------------------------------------
async def test_request_goes_through_every_stage(app_factory: AppFactory, fake: FakeTelegram,
                                                cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    req = fake.post(KASSA, BOT, request("100"))
    await app.idle()
    fake.post(KASSA, CASHER, "Т-Банк 2200 1234 5678 9012", reply_to=req.id_for(0))
    fake.post(KASSA, BOT, sent())
    fake.post(KASSA, BOT, receipt("100"))
    await app.idle()
    rcpt = fake.chats[KASSA].messages[-1]
    fake.edit(rcpt, receipt("100") + "\n✅")
    await app.idle()

    d = await dep(app, "100")
    assert (d["stage"], d["status"]) == ("final", "success")
    assert d["tgid"] == TG and d["mel_id"] == MEL and d["method"] == "Top Pay" and str(d["amount"]) == "1000.00"
    assert str(d["bonus"]) == "50" and str(d["total"]) == "1050.00" and d["cashier_id"] == CASHER
    assert d["requisites"] == '[{"kind": "card", "value": "2200 1234 5678 9012"}]'  # as written (D-RM-5)
    assert d["created_at"] <= d["requisites_at"] <= d["sent_at"] <= d["receipt_at"] <= d["final_at"]
    assert d["marks"] == "[]"  # 1000 + 50 bonus = 1050, as in the receipt
    # Every stage is recorded once (their order in time is checked on the columns above: messages
    # posted in one burst share a timestamp on Windows, whose clock ticks every ~16 ms).
    stages = [r["stage"] for r in await app.db.pool.fetch("SELECT stage FROM deposit_events WHERE deposit_id = '100'")]
    assert sorted(stages) == sorted(["created", "requisites", "sent", "receipt", "final"])

    # The player: TGID <-> MelID, with the name history.
    player = await app.db.pool.fetch("SELECT kind, value, player_id FROM pl_nodes ORDER BY kind")
    assert [(r["kind"], r["value"]) for r in player] == [("mel", MEL), ("tg", TG)]
    assert len({r["player_id"] for r in player}) == 1
    assert await app.db.pool.fetchval("SELECT username FROM pl_names WHERE tg_id = $1", int(TG)) == "x"

    # Statistics from the daily aggregates; /trace by Deposit ID.
    summ = await ask(app, fake, "/summ")
    assert "1 заявок" in summ and "успешных 100%" in summ and "средняя сумма 1000.00" in summ
    assert f"[{CASHER}]" in await ask(app, fake, "/summ by=cashier")
    trace = await ask(app, fake, "/trace 100")
    assert trace.count("Входящая") >= 4 and "deposit.receipt" in trace
    card = await ask(app, fake, f"/player {TG}")
    assert f"MelID: {MEL}" in card and "@x" in card
    assert "100 касса" in await ask(app, fake, f"/player_deposits mel:{MEL}")


async def test_receipt_amount_other_than_request_plus_bonus_is_marked(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, BOT, request("150"))
    await app.idle()
    fake.post(KASSA, BOT, receipt("150", amount="1 100.00"))  # the receipt may come before the bonus is known
    fake.post(KASSA, BOT, sent(bonus="50"))
    await app.idle()
    marks = _json((await dep(app, "150"))["marks"])
    assert marks == [{"kind": "amount_mismatch", "expected": "1050.00", "got": "1100.00"}]


async def test_two_requisites_in_a_row_are_tied_by_the_bot_confirmation(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg, binding=["confirmation"])
    fake.post(KASSA, BOT, request("200"))
    await app.idle()
    fake.post(KASSA, CASHER, "Сбер 2202 0000 1111 2222")
    fake.post(KASSA, CASHER, "или по телефону +79001112233")
    fake.post(KASSA, BOT, sent())
    await app.idle()
    d = await dep(app, "200")
    assert d["stage"] == "sent"
    assert d["requisites"] == ('[{"kind": "card", "value": "2202 0000 1111 2222"}, '
                               '{"kind": "phone", "value": "+79001112233"}]')


async def test_confirmation_before_the_request_is_applied_when_the_request_comes(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    """Messages of different ordering keys may be processed in any order."""
    app = await make(app_factory, cfg)
    module = next(m for m in app.modules if isinstance(m, DepositsModule))
    now = time.time()
    # Feed the module directly in the reverse order: confirmation first, then the request.
    await replay(app, module, [_rec(KASSA, 11, sent(), "deposit.sent", {"tgid": TG, "bonus": "50"}, now),
                               _rec(KASSA, 10, request("300"), "deposit.request",
                                    {"deposit_id": "300", "tgid": TG, "amount": "1 000.00"}, now - 1)])
    d = await dep(app, "300")
    assert d["stage"] == "sent" and str(d["bonus"]) == "50"


async def test_redirect_cancel_and_final_are_fixed(app_factory: AppFactory, fake: FakeTelegram,
                                                   cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, BOT, request("400"))
    first = fake.post(KASSA, BOT, request("401"))
    await app.idle()

    # The method is switched off: the bot redirects its open requests; one goes on in kassa 2.
    fake.post(KASSA, BOT, "Метод отключён\nМетод: Top Pay\nID метода: 5")
    await app.idle()
    fake.post(KASSA2, BOT, request("400"))
    await app.idle()
    d = await dep(app, "400")
    assert d["chat_id"] == KASSA2 and [k["chat_id"] for k in _json(d["kassas"])] == [KASSA, KASSA2]
    assert any(m["kind"] == "redirected" for m in _json(d["marks"]))
    assert await app.db.pool.fetchval("SELECT enabled FROM payment_methods WHERE chat_id = $1", KASSA) is False
    assert "Выключенные методы: Top Pay" in await ask(app, fake, "/summ")

    # A cancellation is an edit of the request; after it nothing changes the status.
    fake.edit(first, request("401", extra="\nОтменил: @operator\nПричина: дубль"))
    await app.idle()
    fake.post(KASSA, BOT, receipt("401"))
    await app.idle()
    d = await dep(app, "401")
    assert (d["stage"], d["status"]) == ("cancelled", "cancelled")
    ev = await app.db.pool.fetchrow("SELECT data FROM deposit_events WHERE deposit_id = '401' AND stage = 'cancelled'")
    assert _json(ev["data"]) == {"by": "@operator", "reason": "дубль"}

    # ✅ then ❌: the first final is fixed (FT-З.5).
    fake.post(KASSA2, BOT, receipt("400"))
    await app.idle()
    r = fake.chats[KASSA2].messages[-1]
    fake.edit(r, receipt("400") + "\n✅")
    await app.idle()
    fake.edit(r, receipt("400") + "\n❌")
    await app.idle()
    d = await dep(app, "400")
    assert d["status"] == "success"


async def test_request_without_deposit_id_is_tied_by_tgid_amount_and_time(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, BOT, request("500"))
    await app.idle()
    fake.post(KASSA, BOT, trusted())  # same player, same amount, minutes later: the same request
    fake.post(KASSA, BOT, trusted(tg="999", amount="777.00"))  # nobody to match: its own request
    await app.idle()
    rows = await app.db.pool.fetch("SELECT deposit_id, tgid FROM deposits ORDER BY created_at")
    assert rows[0]["deposit_id"] == "500" and len(rows) == 2
    assert rows[1]["deposit_id"].startswith("nodid:") and rows[1]["tgid"] == "999"


# ---- repeat processing --------------------------------------------------------------------------------------
async def test_processing_the_same_history_again_gives_the_same_state(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    req = fake.post(KASSA, BOT, request("600"))
    await app.idle()
    fake.post(KASSA, CASHER, "2200 1234 5678 9012", reply_to=req.id_for(0))
    fake.post(KASSA, BOT, sent())
    fake.post(KASSA, BOT, request("601", tg="111", mel=MEL))  # the same MelID: the same player
    await app.idle()
    before = await snapshot(app)
    rows = await app.db.pool.fetch("SELECT * FROM messages WHERE msg_type LIKE 'deposit.%' ORDER BY ingest_id")
    for module in app.modules:
        if isinstance(module, (DepositsModule, PlayersModule)):
            await replay(app, module, [MessageRecord.from_row(r) for r in rows])
    assert await snapshot(app) == before


async def snapshot(app: App) -> list[Any]:
    out = []
    for sql in ("SELECT deposit_id, chat_id, kassas, stage, status, tgid, mel_id, amount, bonus, requisites, marks, "
                "cashier_id, created_at, requisites_at, sent_at FROM deposits ORDER BY 1",
                "SELECT deposit_id, trace_id, stage FROM deposit_events ORDER BY 1, 2, 3",
                "SELECT kind, value, player_id FROM pl_nodes ORDER BY 1, 2",
                "SELECT link_id, tg_id, mel_id, state, first_seen, last_seen FROM pl_links ORDER BY 1",
                "SELECT id, kind, into_player, from_player, link_id, moved FROM pl_journal ORDER BY id",
                "SELECT * FROM deposit_daily ORDER BY 1, 2, 3, 4"):
        out.append([tuple(r) for r in await app.db.pool.fetch(sql)])
    return out


# ---- players ------------------------------------------------------------------------------------------------
async def test_players_join_without_limit_and_a_cut_link_splits_them(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, BOT, request("700", tg="1", mel="M1"))
    fake.post(KASSA, BOT, request("701", tg="2", mel="M1"))  # the same MelID: player {1, 2, M1}
    await app.idle()
    assert await players(app) == [["mel:M1", "tg:1", "tg:2"]]

    fake.post(KASSA, BOT, request("702", tg="3", mel="M2"))  # another player {3, M2}
    await app.idle()
    first = await PL_store.player_of(app.db.pool, "tg", "1")
    second = await PL_store.player_of(app.db.pool, "tg", "3")
    fake.post(KASSA, BOT, request("703", tg="3", mel="M1"))  # TGID 3 used M1 too: one person (D-PL-1)
    await app.idle()
    assert await players(app) == [["mel:M1", "mel:M2", "tg:1", "tg:2", "tg:3"]]
    # The older player stays; the younger id points at it.
    assert await PL_store.player_of(app.db.pool, "mel", "M2") == first
    assert second is not None and await PL_store.current(app.db.pool, second) == first
    # Without a prefix the number is searched as TGID and as MelID.
    assert "Игрок #" in await ask(app, fake, "/player M2")

    # A link cut by hand: the player is rebuilt from the remaining links (D-PL-2).
    assert "разорвана" in await ask(app, fake, "/unlink 3 M1")
    assert await players(app) == [["mel:M1", "tg:1", "tg:2"], ["mel:M2", "tg:3"]]
    assert await PL_store.player_of(app.db.pool, "mel", "M1") == first
    # The same pair again does not glue the parts back: the link stays cut.
    fake.post(KASSA, BOT, request("704", tg="3", mel="M1"))
    await app.idle()
    assert len(await players(app)) == 2
    assert "восстановлена" in await ask(app, fake, "/relink 3 M1")
    assert len(await players(app)) == 1


async def test_ambiguous_identifier_asks_for_a_prefix(app_factory: AppFactory, fake: FakeTelegram,
                                                     cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, BOT, request("800", tg="555", mel="A"))
    fake.post(KASSA, BOT, request("801", tg="4444", mel="555"))  # "555": a TGID of one player, a MelID of another
    await app.idle()
    reply = await ask(app, fake, "/player 555")
    assert "Уточните: tg:555 или mel:555" in reply
    assert "TGID: 555" in await ask(app, fake, "/player tg:555")


async def test_sanction_marks_keep_author_and_time(app_factory: AppFactory, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)  # the tables of the module exist when the module is connected

    async with app.db.pool.acquire() as conn:
        assert await PL_store.add_mark(conn, "scam", "tg", TG, author="8001", text="чек подделан",
                                    trace_id="00000000-0000-0000-0000-000000000001")
        assert not await PL_store.add_mark(conn, "scam", "tg", TG, author="8001",
                                        trace_id="00000000-0000-0000-0000-000000000001")  # repeat: no new mark
        await PL_store.add_mark(conn, "unban", "tg", TG, author="8002")
    rows = await app.db.pool.fetch("SELECT kind, author, created_at FROM pl_marks ORDER BY id")
    assert [(r["kind"], r["author"]) for r in rows] == [("scam", "8001"), ("unban", "8002")]
    assert all(r["created_at"] is not None for r in rows)


# ---- reconciliation, stuck requests ---------------------------------------------------------------------------
async def test_reconciliation_rereads_a_missed_final_and_stuck_requests_are_reported(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, BOT, request("900"))
    fake.post(KASSA, BOT, receipt("900"))
    fake.post(KASSA, BOT, request("901"))
    await app.idle()
    # The final arrives as an edit that the push channel lost.
    lost = fake.chats[KASSA].messages[1]
    lost.text, lost.edit_date = receipt("900") + "\n✅", datetime.now(UTC)
    module = next(m for m in app.modules if isinstance(m, DepositsModule))
    assert await module.reconcile_open(0, 100) > 0
    await app.idle()
    assert (await dep(app, "900"))["status"] == "success"

    cfg2 = copy.deepcopy(cfg)
    cfg2["modules"]["deposits"]["config"]["stuck_after_h"] = 0
    write_config(app.store.path, cfg2)
    assert app.reload_config()[0]
    await module.periodic()
    assert (await dep(app, "901"))["stuck"] is True
    await wait_for(lambda: any("Заявки без финала" in t and "901" in t for _, t in sent_by(fake, LOGS)),
                   what="stuck notice")
    assert "зависших 1" in await ask(app, fake, "/summ")


# ---- reports at x10 volume -------------------------------------------------------------------------------------
async def test_reports_answer_within_a_second_at_ten_times_the_volume(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    module = next(m for m in app.modules if isinstance(m, DepositsModule))
    # x10: ~100 kassas, 30 days, several cashiers and methods; 300 000 deposits of players.
    await app.db.pool.execute(
        "INSERT INTO deposit_daily SELECT current_date - d, -1001000000000 - k, c, 'm' || m, 20, 15, 3, 1, 1, 20000, "
        "18, 1800, 19, 9000 FROM generate_series(0, 29) d, generate_series(1, 100) k, generate_series(1, 5) c, "
        "generate_series(1, 3) m", timeout=600)
    await app.db.pool.execute(
        "INSERT INTO deposits (deposit_id, chat_id, tgid, mel_id, amount, stage, status, created_at) "
        "SELECT 'x' || i, -1001000000000 - i % 100, (i % 50000)::text, 'mel' || (i % 60000), 1000, 'final', "
        "'success', now() - make_interval(secs => i) FROM generate_series(1, 300000) i", timeout=600)
    await app.db.pool.execute("ANALYZE deposit_daily; ANALYZE deposits", timeout=600)
    for args in (["30", "all"], ["30", "all", "by=cashier"], ["7", str(KASSA)], ["30", "all", "by=method"]):
        started_at = time.perf_counter()
        text = await module.summary(args)
        assert time.perf_counter() - started_at < 1.0, args
        assert "заявок" in text
    started_at = time.perf_counter()
    fake.post(KASSA, BOT, request("x1", tg="777", mel="mel777"))
    await app.idle()
    players_module = next(m for m in app.modules if isinstance(m, PlayersModule))
    text = await players_module.run("player_deposits", ["tg:777", "30"], "8001",
                                    players_module.cfg(_ctx(app, players_module)))
    assert "Депозиты игрока" in text


# ---- helpers ---------------------------------------------------------------------------------------------------
async def players(app: App) -> list[list[str]]:
    rows = await app.db.pool.fetch("SELECT player_id, kind || ':' || value AS ident FROM pl_nodes")
    groups: dict[int, list[str]] = {}
    for r in rows:
        groups.setdefault(r["player_id"], []).append(r["ident"])
    return sorted(sorted(g) for g in groups.values())


def _ctx(app: App, module: Any, rec: MessageRecord | None = None) -> Context:
    mcfg = app.store.current.config.modules.get(module.name)
    record = rec or _rec(KASSA, 0, "", "", {}, time.time())
    return Context(Event(record), module.name, app.db, mcfg.config if mcfg else {}, app.store.current.version)


async def replay(app: App, module: Any, records: list[MessageRecord]) -> None:
    for rec in records:
        await module.handle(Event(rec), _ctx(app, module, rec))
    await asyncio.sleep(0)


def _rec(chat: int, msg_id: int, text: str, msg_type: str, fields: dict[str, Any], at: float) -> MessageRecord:
    import uuid

    return MessageRecord(
        trace_id=str(uuid.uuid4()), ingest_id=10_000 + msg_id, scope_id=0, chat_id=chat, msg_id=msg_id, version=0,
        msg_date=at, account_id=111, event="new", source="live", policy="full", origin="external", sender_id=BOT,
        sender_role="kassa_bot", can_command=True, source_roles=["kassa"], src_priority=1, reply_to=None, text=text,
        features=[], config_version="", received_at=at, msg_type=msg_type, fields=fields)


def _json(value: Any) -> Any:
    import json

    return json.loads(value) if isinstance(value, str) else value
