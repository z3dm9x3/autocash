"""Deposits: stages of a payment request in a kassa (FT-З.1..З.11).

All writes of one kassa are serialized by an advisory lock on the chat (then on the deposit),
so messages of different ordering keys (Deposit ID, the chat) can be processed in parallel
modules-wise without racing on the same row. Each stage change is first recorded in
deposit_events with the key (deposit, task, stage): a repeated run finds it and changes
nothing (FT-8.2).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

OPEN_STAGES = ("created", "requisites", "sent", "receipt")
_NUM = re.compile(r"-?\d[\d  ]*(?:[.,]\d+)?")


def amount(value: Any) -> Decimal | None:
    """'5 001.34', '4 001,36', '3500.00' -> Decimal."""
    if value is None:
        return None
    m = _NUM.search(str(value))
    if not m:
        return None
    try:
        return Decimal(m.group(0).replace(" ", "").replace(" ", "").replace(",", "."))
    except InvalidOperation:
        return None


def requisites(features: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Card and phone numbers of the requisites, as written (no masking: decision D-RM-5)."""
    return [{"kind": f["k"], "value": str(f["v"])} for f in features if f["k"] in ("card", "phone")]


@dataclass(slots=True)
class Msg:
    trace_id: str
    chat_id: int
    msg_id: int
    at: datetime
    sender_id: int | None
    reply_to: int | None
    event: str  # new | edit
    fields: dict[str, Any] = field(default_factory=dict)
    features: list[dict[str, Any]] = field(default_factory=list)


async def lock_chat(conn: asyncpg.Connection, chat_id: int) -> None:
    await conn.execute("SELECT pg_advisory_xact_lock(1, hashtext($1::text))", str(chat_id))


async def lock_deposit(conn: asyncpg.Connection, deposit_id: str) -> None:
    await conn.execute("SELECT pg_advisory_xact_lock(2, hashtext($1))", deposit_id)


async def record(conn: asyncpg.Connection, deposit_id: str, m: Msg, stage: str,
                 data: dict[str, Any] | None = None) -> bool:
    """Record a stage change; False when this task already recorded it (a repeat)."""
    fresh = await conn.fetchval(
        "INSERT INTO deposit_events (deposit_id, trace_id, stage, chat_id, msg_id, at, data) "
        "VALUES ($1, $2::uuid, $3, $4, $5, $6, $7::jsonb) ON CONFLICT DO NOTHING RETURNING 1",
        deposit_id, m.trace_id, stage, m.chat_id, m.msg_id, m.at, json.dumps(data or {}, default=str))
    return bool(fresh)


async def get(conn: asyncpg.Connection, deposit_id: str) -> asyncpg.Record | None:
    row: asyncpg.Record | None = await conn.fetchrow("SELECT * FROM deposits WHERE deposit_id = $1 FOR UPDATE",
                                                     deposit_id)
    return row


async def unsent_for_tgid(conn: asyncpg.Connection, chat_id: int, tgid: str,
                          before: datetime) -> asyncpg.Record | None:
    """The player's latest open request in the kassa that has no confirmation yet.

    Any stage: the receipt (keyed by Deposit ID) may have been processed before the
    confirmation (keyed by the chat)."""
    row: asyncpg.Record | None = await conn.fetchrow(
        "SELECT * FROM deposits WHERE chat_id = $1 AND tgid = $2 AND status = 'open' AND sent_at IS NULL "
        "AND created_at <= $3 ORDER BY created_at DESC LIMIT 1 FOR UPDATE", chat_id, tgid, before)
    return row


async def pend(conn: asyncpg.Connection, m: Msg, kind: str, tgid: str | None = None,
               data: dict[str, Any] | None = None) -> None:
    await conn.execute(
        "INSERT INTO deposit_pending (chat_id, msg_id, kind, trace_id, reply_to, tgid, sender_id, data, at) "
        "VALUES ($1, $2, $3, $4::uuid, $5, $6, $7, $8::jsonb, $9) ON CONFLICT DO NOTHING",
        m.chat_id, m.msg_id, kind, m.trace_id, m.reply_to, tgid, m.sender_id,
        json.dumps(data or {}, default=str, ensure_ascii=False), m.at)


async def refresh_daily(conn: asyncpg.Connection, chat_id: int, created_at: datetime, tz: str) -> None:
    """Recompute the daily aggregate of the kassa for the day of this deposit (FT-З.8)."""
    await conn.execute("DELETE FROM deposit_daily WHERE chat_id = $1 AND day = ($2::timestamptz AT TIME ZONE $3)::date",
                       chat_id, created_at, tz)
    await conn.execute(
        """
        WITH d AS (
            SELECT ($2::timestamptz AT TIME ZONE $3)::date AS day
        ), src AS (
            SELECT coalesce(cashier_id, 0) AS cashier_id, coalesce(method, '') AS method, status, stuck, amount,
                   extract(epoch FROM requisites_at - created_at) AS req_s,
                   extract(epoch FROM final_at - created_at) AS fin_s
            FROM deposits, d
            WHERE chat_id = $1 AND (created_at AT TIME ZONE $3)::date = d.day
        ), agg AS (
            SELECT cashier_id, method, count(*) AS created,
                   count(*) FILTER (WHERE status = 'success') AS success,
                   count(*) FILTER (WHERE status = 'rejected') AS rejected,
                   count(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                   count(*) FILTER (WHERE stuck) AS stuck,
                   coalesce(sum(amount), 0) AS amount_sum,
                   count(req_s) AS requisites_n, coalesce(sum(req_s), 0) AS requisites_sec,
                   count(fin_s) AS final_n, coalesce(sum(fin_s), 0) AS final_sec
            FROM src GROUP BY cashier_id, method
        )
        INSERT INTO deposit_daily (day, chat_id, cashier_id, method, created, success, rejected, cancelled, stuck,
                                   amount_sum, requisites_n, requisites_sec, final_n, final_sec)
        SELECT d.day, $1, cashier_id, method, created, success, rejected, cancelled, stuck, amount_sum,
               requisites_n, requisites_sec, final_n, final_sec FROM agg, d
        """,
        chat_id, created_at, tz,
    )
