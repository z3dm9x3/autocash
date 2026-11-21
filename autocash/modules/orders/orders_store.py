"""orders: reads of deposits and withdrawals for the commands /dev dep and /dev ww (orders_readme.md)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

HISTORY_SOURCE = "cash_monitor"


@dataclass(slots=True)
class Found:
    rows: list[Any]  # the latest first, at most `limit`
    total: int
    by_status: dict[str, tuple[int, float]]  # status -> (count, sum of amounts)
    first: Any
    last: Any


def _where(tgs: Sequence[int], mels: Sequence[str]) -> str:
    return "(tg_id = ANY($1::bigint[]) OR mel_id = ANY($2::text[]))"


async def deposits(db: Any, tgs: Sequence[int], mels: Sequence[str], limit: int) -> Found:
    """Deposits of these TGIDs or MelIDs; the amount of a success is what was credited (the receipt)."""
    args = ([int(t) for t in tgs], [str(m) for m in mels])
    rows = await db.fetch(
        "SELECT d.deposit_id, d.tg_id, d.mel_id, d.status, d.amount, d.receipt_amount, d.method, d.chat_id, "
        "d.cancel_by, d.cancel_reason, d.created_at, coalesce(c.title, '') AS title FROM orders_deposits d "
        "LEFT JOIN orders_history_chats c ON c.chat_id = d.chat_id "
        f"WHERE {_where(*args)} ORDER BY d.created_at DESC NULLS LAST, d.deposit_id DESC LIMIT $3", *args, limit)
    stats = await db.fetch(
        "SELECT status, count(*) AS n, coalesce(sum(coalesce(receipt_amount, amount)), 0)::float8 AS s, "
        "min(created_at) AS first, max(created_at) AS last FROM orders_deposits "
        f"WHERE {_where(*args)} GROUP BY status", *args)
    return _found(rows, stats)


async def withdrawals(db: Any, tgs: Sequence[int], mels: Sequence[str], limit: int) -> Found:
    args = ([int(t) for t in tgs], [str(m) for m in mels])
    rows = await db.fetch(
        "SELECT w.chat_id, w.msg_id, w.tg_id, w.mel_id, w.amount, w.result, w.result_text, w.result_code, "
        "w.method, w.message_date AS created_at, coalesce(c.title, '') AS title FROM orders_withdrawals w "
        "LEFT JOIN orders_history_chats c ON c.chat_id = w.chat_id "
        f"WHERE {_where(*args)} ORDER BY w.message_date DESC, w.msg_id DESC LIMIT $3", *args, limit)
    stats = await db.fetch(
        "SELECT result AS status, count(*) AS n, coalesce(sum(amount), 0)::float8 AS s, "
        "min(message_date) AS first, max(message_date) AS last FROM orders_withdrawals "
        f"WHERE {_where(*args)} GROUP BY result", *args)
    return _found(rows, stats)


def _found(rows: list[Any], stats: list[Any]) -> Found:
    by = {str(r["status"]): (int(r["n"]), float(r["s"])) for r in stats}
    firsts = [r["first"] for r in stats if r["first"] is not None]
    lasts = [r["last"] for r in stats if r["last"] is not None]
    return Found(list(rows), sum(n for n, _ in by.values()), by, min(firsts, default=None), max(lasts, default=None))


async def history(db: Any, source: str = HISTORY_SOURCE) -> dict[str, Any]:
    """The state of the history scan and what it filled (for /dev hist)."""
    row = await db.fetchrow("SELECT * FROM orders_history WHERE source = $1", source)
    counts = await db.fetchrow(
        "SELECT (SELECT count(*) FROM orders_deposits) AS deposits, "
        "(SELECT count(*) FROM orders_withdrawals) AS withdrawals, "
        "(SELECT count(*) FROM orders_history_chats WHERE cards > 0) AS kassas, "
        "(SELECT count(*) FROM players_links WHERE source LIKE 'history:%') AS links, "
        "(SELECT count(*) FROM players_links WHERE source LIKE 'history:%' AND state = 'staff') AS staff_links, "
        "(SELECT count(*) FROM players WHERE merged_into IS NULL "
        " AND EXISTS (SELECT 1 FROM players_nodes n WHERE n.player_id = players.player_id)) AS players")
    return {"state": dict(row) if row is not None else None, **dict(counts)}
