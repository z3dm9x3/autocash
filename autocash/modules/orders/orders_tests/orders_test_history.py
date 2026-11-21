"""orders: the history read through dblink, users and players from it, /dev in logs, the delete of the reply.

The archive is a second database of the test cluster with the tables of Cash Monitor Dev that the scan reads
(public.raw_messages, public.chat_registry); the cards follow the real formats of
tests/fixtures/messages/orders_cards.jsonl.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from autocash.core.app import App
from autocash.modules.orders.orders_module import OrdersModule
from autocash.modules.players import players_store
from autocash.modules.players.players_module import PlayersModule
from autocash.modules.role_manager.RM_module import RoleManagerModule
from tests.conftest import ACCOUNT, LOGS, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import ACC2, join_all, sent_by, started, wait_for

DEV = 8002
BOT = 7185799444
KASSA_CHAT = -1001000000101
WW_CHAT = -1001000000102
P1, P2, P3 = 9001, 9002, 9003
CASHIER, CASHIER2 = 6100, 6200
NOW = datetime.now(UTC).replace(microsecond=0)


def request(dep: int, tg: int, mel: int, *, user_type: str = "USER", tail: str = "") -> str:
    return (f"[ДЕПОЗИТ ТРЕУГОЛЬНИК]\nDeposit ID: {dep}\nПользователь: @p{tg} ID: {tg}\nТип пользователя: {user_type}\n"
            f"Успешных депозитов: 🟢 0 (всего: 3)\nНомер счёта: {mel}\nСумма пополнения: 5000.00\n"
            f"Метод оплаты: [СБП] [Карты] BLADE [PDF] 3 000 - 500 000\nОператор, выдайте реквизиты!{tail}")


def receipt(dep: int, tg: int, mel: int, tail: str = "") -> str:
    return (f"[ДЕПОЗИТ]\nDeposit ID: {dep}\nПользователь: @p{tg} ID: {tg}\nТип пользователя: USER\n"
            f"Успешных депозитов: 🟢 1\nНомер счёта: {mel}\nСумма пополнения: 5 000.61\n"
            f"Метод оплаты: [СБП] [Карты] BLADE [PDF] 3 000 - 500 000{tail}")


def withdrawal(tg: int, mel: int) -> str:
    return (f"[ВЫВОД]\nПользователь: @p{tg} ID: {tg}\n\nПлатформа: 58\nНомер счёта: {mel}\nКод: Ab1c\n"
            "Сумма вывода: 7 000\nМетод оплаты: СБП Kamelot 💫 1 000 - 3 000 000\nРеквизиты: 79000000000\n"
            "Банк: Т банк\n\n✅ Успех: Операция выполнена успешно")


OK = "\n\n✅ Успех: Операция прошла успешно"
CANCELLED = "\n\n❌ Заявка #{dep} отменена\nИнициатор: пользователь · Причина: Отменена пользователем"

# (sender, chat, text, media, forwarded): in the order of raw_messages.id
ARCHIVE: list[tuple[int, int, str, str | None, bool]] = [
    (BOT, KASSA_CHAT, request(101, P1, 111), None, False),
    (BOT, KASSA_CHAT, receipt(101, P1, 111, OK), "MessageMediaDocument", False),
    (BOT, KASSA_CHAT, request(102, P1, 222, tail=CANCELLED.format(dep=102)), None, False),
    (BOT, KASSA_CHAT, request(103, P2, 222), None, False),
    (BOT, KASSA_CHAT, receipt(103, P2, 222), "MessageMediaDocument", False),  # its final comes later by an edit
    (CASHIER, KASSA_CHAT, "+79000000000\nСбер\nИванов И И", None, False),  # a cashier writes in the kassa group
    (BOT, KASSA_CHAT, request(104, CASHIER, 333), None, False),  # the cashier made a deposit for a player
    (BOT, KASSA_CHAT, request(105, CASHIER2, 444, user_type="CASHIER"), None, False),
    (BOT, KASSA_CHAT, request(106, P3, 555), None, True),  # a forwarded copy: not a card of this kassa
    (BOT, WW_CHAT, withdrawal(P1, 111), None, False),
    (PLAYER, -1001000000999, "привет", None, False),  # another group: its people are not staff
]


@pytest.fixture
async def archive(pg_dsn: str) -> AsyncIterator[str]:
    """A second database of the test cluster in the role of the archive of Cash Monitor Dev."""
    base, name = pg_dsn.rsplit("/", 1)
    src = f"{name}a"
    admin = await asyncpg.connect(f"{base}/postgres")
    await admin.execute(f"CREATE DATABASE {src}")
    await admin.close()
    conn = await asyncpg.connect(f"{base}/{src}")
    await conn.execute(
        "CREATE TABLE raw_messages (id bigserial PRIMARY KEY, telegram_account_id bigint, chat_id bigint, "
        "telegram_message_id bigint, sender_id bigint, reply_to_message_id bigint, message_date timestamptz, "
        "edit_date timestamptz, text text, raw_payload jsonb, is_out boolean NOT NULL DEFAULT false);"
        "CREATE TABLE chat_registry (id serial, telegram_account_id bigint, chat_id bigint, title text, "
        "updated_at timestamptz NOT NULL DEFAULT now())")
    for i, (sender, chat, text, media, fwd) in enumerate(ARCHIVE, 1):
        payload = {"fwd_from": {"_": "MessageFwdHeader"} if fwd else None, "media": {"_": media} if media else None}
        await conn.execute(
            "INSERT INTO raw_messages (telegram_account_id, chat_id, telegram_message_id, sender_id, message_date, "
            "text, raw_payload) VALUES (1, $1, $2, $3, $4, $5, $6::jsonb)",
            chat, 100 + i, sender, NOW - timedelta(hours=1) + timedelta(minutes=i), text, json.dumps(payload))
    await conn.execute("INSERT INTO chat_registry (telegram_account_id, chat_id, title) VALUES "
                       f"(1, {KASSA_CHAT}, 'Blade Dop'), (1, {WW_CHAT}, 'Kamelot WW')")
    await conn.close()
    target = await asyncpg.connect(pg_dsn)
    await target.execute("CREATE EXTENSION IF NOT EXISTS dblink")
    await target.close()
    try:
        yield f"{base}/{src}"
    finally:
        admin = await asyncpg.connect(f"{base}/postgres")
        await admin.execute(f"DROP DATABASE IF EXISTS {src} WITH (FORCE)")
        await admin.close()


def modules_cfg(config: dict[str, Any], *, history: bool) -> dict[str, Any]:
    config["modules"] = {"role_manager": {}, "players": {}, "orders": {"config": {
        "delete_after_s": 1,
        "history": {"enabled": history, "batch": 3, "interval_s": 0.05, "idle_interval_s": 0.2,
                    "refresh_interval_s": 0.3, "settle_interval_s": 0.3, "error_pause_s": 0.5},
    }}}
    return config


async def orders_app(app_factory: AppFactory, cfg: dict[str, Any], **kw: Any) -> App:
    modules = [RoleManagerModule({DEV: "dev"}), PlayersModule(), OrdersModule()]
    return await started(app_factory(cfg=cfg, modules=modules, **kw))


async def status(app: App, deposit_id: int) -> str | None:
    value: str | None = await app.db.pool.fetchval("SELECT status FROM orders_deposits WHERE deposit_id = $1",
                                                   deposit_id)
    return value


async def answer(fake: FakeTelegram, text: str, sender: int = DEV, expect: str = "") -> tuple[int, str]:
    """Post a command in logs; the reply (msg id of the command, text of the reply)."""
    before = len(sent_by(fake, LOGS))
    msg = fake.post(LOGS, sender, text)
    await wait_for(lambda: any(expect in t for _, t in sent_by(fake, LOGS)[before:]), what=f"answer to {text}")
    return msg.id_for(ACCOUNT), next(t for _, t in sent_by(fake, LOGS)[before:] if expect in t)


async def test_the_history_fills_orders_users_and_players(app_factory: AppFactory, fake: FakeTelegram,
                                                          config: dict[str, Any], archive: str,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTOCASH_HISTORY_DSN", archive)
    app = await orders_app(app_factory, modules_cfg(config, history=True))
    pool = app.db.pool

    async def settled() -> bool:
        return bool(await pool.fetchval("SELECT settled_at IS NOT NULL FROM orders_history"))

    await wait_for(settled, timeout=60, what="the first pass and its settle")
    assert await status(app, 101) == "success"
    assert await status(app, 102) == "cancelled"
    assert await status(app, 103) == "receipt"
    assert await status(app, 106) is None  # the forwarded copy is not read
    w = await pool.fetchrow("SELECT * FROM orders_withdrawals")
    assert (w["tg_id"], w["mel_id"], w["result"], w["amount"]) == (P1, "111", "success", 7000)
    assert await pool.fetchval("SELECT title FROM orders_history_chats WHERE chat_id = $1", KASSA_CHAT) == "Blade Dop"

    # Users: players of the cards, staff of the kassa groups; the in-memory directory knows them.
    rows = await pool.fetch("SELECT tg_id, role FROM rm_users WHERE source = 'history'")
    roles = {r["tg_id"]: r["role"] for r in rows}
    assert roles == {P1: "player", P2: "player", CASHIER: "casher", CASHIER2: "casher"}
    assert app.store.directory.user_role(P2) == "player" and app.store.directory.user_role(CASHIER) == "casher"
    assert PLAYER not in roles  # wrote in a group without cards of a kassa

    # Players: P1 and P2 share MelID 222; the pairs of the cashiers join nothing.
    async def joined() -> bool:
        return await players_store.player_of(pool, "tg", P2) is not None

    await wait_for(joined, timeout=30, what="the pairs joined into players")
    pid = await players_store.player_of(pool, "tg", P1)
    assert pid is not None and pid == await players_store.player_of(pool, "tg", P2)
    assert await players_store.idents(pool, pid) == {"tg": [str(P1), str(P2)], "mel": ["111", "222"]}
    staff = {(r["tg_id"], r["mel_id"]) for r in await pool.fetch("SELECT * FROM players_links WHERE state = 'staff'")}
    assert staff == {(CASHIER, "333"), (CASHIER2, "444")}
    assert await players_store.player_of(pool, "mel", "333") is None

    # The final of an open card comes by an edit after the scan: the refresh reads it again.
    conn = await asyncpg.connect(archive)
    await conn.execute("UPDATE raw_messages SET text = $1, edit_date = now() WHERE telegram_message_id = 105",
                       receipt(103, P2, 222, OK))
    await conn.close()

    async def refreshed() -> bool:
        return await status(app, 103) == "success"

    await wait_for(refreshed, timeout=30, what="the refreshed receipt")

    # /dev in logs: the deposits of the whole player, then the command and the reply are deleted.
    fake.chats[LOGS].admins.add(ACCOUNT)
    command_id, text = await answer(fake, "/dev dep player 9002", expect="Депозиты игрока")
    assert "#101" in text and "#102" in text and "#103" in text and "Blade Dop" in text
    reply_id = next(m for a, c, t, m in fake.sent if c == LOGS and t == text)
    await wait_for(lambda: {command_id, reply_id} <= {m for _, c, m in fake.deletes if c == LOGS}, timeout=20,
                   what="the command and the reply deleted")
    _, ww = await answer(fake, "/dev ww mel 111", expect="Выводы MelID 111")
    assert "Kamelot WW" in ww and "7 000" in ww
    _, hist = await answer(fake, "/dev hist", expect="История Cash Monitor Dev")
    assert "Первый проход: завершён" in hist
    assert any("orders: история прочитана" in t for _, t in sent_by(fake, LOGS))


async def test_a_delete_falls_back_to_another_account_and_fails_only_when_all_failed(
        app_factory: AppFactory, fake: FakeTelegram, config: dict[str, Any]) -> None:
    join_all(fake, ACC2)
    app = await orders_app(app_factory, modules_cfg(config, history=False), accounts=(ACCOUNT, ACC2))
    fake.chats[LOGS].admins.add(ACC2)  # only the second account may delete messages of people
    command_id, _ = await answer(fake, "/dev hist", expect="История")
    await wait_for(lambda: (ACC2, LOGS, command_id) in fake.deletes, timeout=20, what="the command deleted by ACC2")

    fake.chats[LOGS].admins.clear()  # nobody may: every account tries once, then the task ends
    command_id, _ = await answer(fake, "/dev hist", expect="История")

    async def failed() -> Any:
        return await app.db.pool.fetchrow(
            "SELECT status, reason_code FROM outbound_tasks WHERE action = 'delete' AND target_msg_id = $1 "
            "AND finished_at IS NOT NULL", command_id)

    await wait_for(failed, timeout=20, what="the delete given up")
    row = await failed()
    assert (row["status"], row["reason_code"]) == ("not_delivered", "DELETE_FAILED")
    tried = await app.db.pool.fetch(
        "SELECT a.account_id FROM outbound_attempts a JOIN outbound_tasks t ON t.id = a.task_id "
        "WHERE t.action = 'delete' AND t.target_msg_id = $1 AND a.result = 'not_deleted'", command_id)
    assert sorted(r["account_id"] for r in tried) == [ACCOUNT, ACC2]
    await wait_for(lambda: any("не удалено" in t for _, t in sent_by(fake, LOGS)), what="the failure in logs")
    assert (ACCOUNT, LOGS, command_id) not in fake.deletes and (ACC2, LOGS, command_id) not in fake.deletes


async def test_dev_needs_rights_and_nothing_else_of_logs_is_read(app_factory: AppFactory, fake: FakeTelegram,
                                                                 config: dict[str, Any]) -> None:
    app = await orders_app(app_factory, modules_cfg(config, history=False))
    before = len(sent_by(fake, LOGS))
    fake.post(LOGS, PLAYER, "/dev dep mel 111")  # a guest: no answer in the chat
    fake.post(LOGS, DEV, "просто текст в logs")
    await answer(fake, "/dev", expect="Команды:")
    replies = [t for _, t in sent_by(fake, LOGS)[before:] if "Депозит" in t]
    assert replies == []
    assert await app.db.pool.fetchval("SELECT count(*) FROM messages WHERE chat_id = $1 AND msg_type NOT LIKE "
                                      "'command.%'", LOGS) == 0
