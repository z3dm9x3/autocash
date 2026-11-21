"""orders: SQL parsing of the cards and the deposit computed from its cards, on a real PostgreSQL.

The cards are real ones of the archive of Cash Monitor Dev with the numbers and names replaced
(tests/fixtures/messages/orders_cards.jsonl, AGENTS.md rule 6).
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import pytest

ROOT = Path(__file__).resolve().parents[4]
MIGRATIONS = Path(__file__).resolve().parents[1] / "orders_migrations"
FIXTURE = ROOT / "tests/fixtures/messages/orders_cards.jsonl"
CARDS = [json.loads(line) for line in FIXTURE.read_text("utf-8").splitlines()]
T0 = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


@pytest.fixture
async def db(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    for f in sorted(MIGRATIONS.glob("*.sql")):
        await pool.execute(f.read_text(encoding="utf-8"))
    try:
        yield pool
    finally:
        await pool.close()


async def parse(db: asyncpg.Pool, text: str, media: str | None = None) -> dict[str, Any] | None:
    raw = await db.fetchval("SELECT orders_parse($1, $2)", text, media or "")
    return json.loads(raw) if raw is not None else None


def card(pred: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    return next(c for c in CARDS if pred(c))


def expected_kind(c: dict[str, Any]) -> str:
    if c["kind"] == "ww":
        return "withdrawal"
    if c["kind"] == "dep_request":
        polygon_check = c["text"].startswith("[ДЕПОЗИТ ПОЛИГОН]") and c["media"] in ("MessageMediaDocument",
                                                                                     "MessageMediaPhoto")
        return "receipt" if polygon_check else "request"
    return c["kind"].removeprefix("dep_")


async def test_every_real_card_is_parsed_with_its_kind_and_keys(db: asyncpg.Pool) -> None:
    for c in CARDS:
        f = await parse(db, c["text"], c["media"])
        assert f is not None, c["text"][:60]
        assert f["kind"] == expected_kind(c), c["text"][:60]
        if f["kind"] == "withdrawal":
            assert {"tg_id", "mel_id", "platform", "amount", "method", "result"} <= f.keys(), f
            continue
        assert f["deposit_id"].isdigit(), f
        if f["kind"] in ("request", "receipt"):
            assert {"tg_id", "mel_id", "amount", "method", "user_type"} <= f.keys(), f
    assert await parse(db, "Реквизиты успешно отправлены пользователю ID 123") is None
    assert await parse(db, "") is None


async def test_fields_of_each_kind(db: asyncpg.Pool) -> None:
    pulpy = await parse(db, card(lambda c: c["text"].startswith("[ДЕПОЗИТ PULPY") and "отменена" not in c["text"])
                        ["text"])
    assert pulpy is not None
    assert pulpy["amount"] == 1000 and pulpy["method"] == "Aegis Service QR" and "outcome" not in pulpy

    cancelled = await parse(db, card(lambda c: c["text"].startswith("[ДЕПОЗИТ PULPY") and "отменена" in c["text"])
                            ["text"])
    assert cancelled is not None
    assert cancelled["outcome"] == "cancelled" and cancelled["cancel_by"] == "пользователь"
    assert cancelled["cancel_reason"] == "Отменена пользователем"

    ok = await parse(db, card(lambda c: c["kind"] == "dep_receipt" and "✅ Успех" in c["text"])["text"],
                     "MessageMediaDocument")
    assert ok is not None
    assert ok["outcome"] == "success" and ok["amount"] == 5000.61 and ok["method"] == "[СБП] [Карты]  Misuta"

    rejected = await parse(db, card(lambda c: c["kind"] == "dep_receipt" and "Отклонена кассиром" in c["text"])
                           ["text"], "MessageMediaDocument")
    assert rejected is not None and rejected["outcome"] == "rejected" and rejected["cancel_by"] == "кассир"

    bare = card(lambda c: c["text"].startswith("[ДЕПОЗИТ ПОЛИГОН]") and c["text"].rstrip().endswith("❌"))
    polygon = await parse(db, bare["text"], bare["media"])
    assert polygon is not None
    assert polygon["kind"] == "receipt" and polygon["outcome"] == "rejected" and polygon["user_type"] == "ADMIN"
    assert polygon["method"] == "Полигон Новый ⬡" and polygon["tg_id"].isdigit()  # «ID: <TGID>» of Polygon

    accepted = await parse(db, card(lambda c: c["kind"] == "dep_accepted" and "@" in c["text"])["text"])
    assert accepted is not None and accepted["outcome"] == "success" and accepted["username"].startswith("user")

    wrong_code = await parse(db, card(lambda c: c["kind"] == "ww" and "неверный код" in c["text"])["text"])
    assert wrong_code is not None and wrong_code["result"] == "fail"
    assert wrong_code["result_text"] == "Введен неверный код! Повторите ввод секретного кода."
    assert wrong_code["result_code"].isdigit() and wrong_code["amount"] == 13000

    bare_ok = await parse(db, card(lambda c: c["kind"] == "ww" and c["text"].rstrip().endswith("\n✅"))["text"])
    assert bare_ok is not None and bare_ok["result"] == "bare_ok"
    waiting = await parse(db, card(lambda c: c["kind"] == "ww" and not re.search("[✅❌]", c["text"]))["text"])
    assert waiting is not None and waiting["result"] == "none"

    # The MelID as the live intake gives it: a link (Telethon markdown).
    linked = await parse(db, "[ДЕПОЗИТ ТРЕУГОЛЬНИК]\nDeposit ID: 7\nПользователь: @x ID: 5\n"
                             "Номер счёта: [1481598133](tel:1481598133)\nСумма пополнения: 500.00")
    assert linked is not None and linked["mel_id"] == "1481598133"
    assert await db.fetchval("SELECT orders_num('36 000,77 руб')") == Decimal("36000.77")
    assert await db.fetchval("SELECT orders_num('нет')") is None


def with_id(text: str, deposit_id: int) -> str:
    text = re.sub(r"Deposit ID: \d+", f"Deposit ID: {deposit_id}", text)
    return re.sub(r"#\d+", f"#{deposit_id}", text)


async def apply(db: asyncpg.Pool, msg_id: int, text: str, at: datetime, media: str = "",
                chat_id: int = -1001000000001) -> None:
    f = await db.fetchval("SELECT orders_apply($1, $2, 7185799444, NULL, $3, NULL, $4, $5, 'test', NULL)",
                          chat_id, msg_id, at, media, text)
    assert f is not None
    deposit_id = json.loads(f).get("deposit_id")
    if deposit_id:
        await db.execute("SELECT orders_deposit_recompute($1)", int(deposit_id))


async def deposit(db: asyncpg.Pool, deposit_id: int) -> Any:
    return await db.fetchrow("SELECT * FROM orders_deposits WHERE deposit_id = $1", deposit_id)


async def test_a_deposit_is_computed_from_its_cards_in_any_order(db: asyncpg.Pool) -> None:
    request = card(lambda c: c["kind"] == "dep_request" and not c["media"] and "отменена" not in c["text"]
                   and c["text"].startswith("[ДЕПОЗИТ PULPY"))["text"]
    receipt_ok = card(lambda c: c["kind"] == "dep_receipt" and "✅ Успех" in c["text"])["text"]
    receipt_wait = card(lambda c: c["kind"] == "dep_receipt" and c["text"].rstrip().endswith("Top Pay 1 000"))["text"]
    accepted = card(lambda c: c["kind"] == "dep_accepted")["text"]
    cancel = card(lambda c: c["kind"] == "dep_cancel")["text"]

    # The receipt with its success comes before the request: the result is the same.
    await apply(db, 2, with_id(receipt_ok, 1), T0 + timedelta(minutes=5), "MessageMediaDocument")
    await apply(db, 1, with_id(request, 1), T0)
    d = await deposit(db, 1)
    assert d["status"] == "success" and d["amount"] == 1000 and d["receipt_amount"] == Decimal("5000.61")
    assert d["created_at"] == T0 and d["chats"] == [-1001000000001]

    # A request cancelled by a separate message.
    await apply(db, 3, with_id(request, 2), T0)
    assert (await deposit(db, 2))["status"] == "open"
    await apply(db, 4, with_id(cancel, 2), T0 + timedelta(minutes=1))
    d = await deposit(db, 2)
    assert d["status"] == "cancelled" and d["cancel_by"] == "пользователь"
    assert d["final_at"] == T0 + timedelta(minutes=1)

    # Top Pay: the receipt has no final; «Депозит принят» closes it with the credited amount.
    await apply(db, 5, with_id(request, 3), T0)
    await apply(db, 6, with_id(receipt_wait, 3), T0 + timedelta(minutes=2), "MessageMediaDocument")
    assert (await deposit(db, 3))["status"] == "receipt"
    await apply(db, 7, with_id(accepted, 3), T0 + timedelta(minutes=3))
    d = await deposit(db, 3)
    assert d["status"] == "success" and float(d["receipt_amount"]) > 0 and d["final_at"] == T0 + timedelta(minutes=3)

    # A redirect: the second kassa takes the same Deposit ID.
    await apply(db, 8, with_id(request, 4), T0, chat_id=-1001000000005)
    await apply(db, 9, with_id(request, 4), T0 + timedelta(minutes=1), chat_id=-1001000000006)
    d = await deposit(db, 4)
    assert d["chats"] == [-1001000000005, -1001000000006] and d["chat_id"] == -1001000000006

    # An older copy of a card never replaces a newer one.
    await db.execute("SELECT orders_apply(-1001000000001, 2, 1, NULL, $1, NULL, 'MessageMediaDocument', $2, 'test', "
                     "NULL)", T0 - timedelta(days=1), with_id(receipt_wait, 1))
    await db.execute("SELECT orders_deposit_recompute(1)")
    assert (await deposit(db, 1))["status"] == "success"
