"""TS: what other modules call - the ticket system takes facts in and hands results on (TS_readme.md, D-TS-1).

TS writes nothing to Telegram. A module that talks to players (Autosup) passes each private message of a
player here with `receive`. While the ticket has no type (the first `burst_s` from its first message),
C-ticket checks every message: a withdrawal code found in the base of codes -> WW; else the triggers
(D only -> D, WW only -> WW, both -> Q); nothing in the whole minute -> Q (TS_module, in the background).
Once typed, nothing is searched any more. The type is written with the protocol ticket.d | ticket.ww |
ticket.q in the same transaction; those protocols are how TS hands its results to the other modules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg
from pydantic import BaseModel, Field

from autocash.core.protocols import emit
from autocash.modules.ticket_system import TS_codes
from autocash.modules.ticket_system.TS_rules import (
    DEFAULT_CODE_EXCLUSIONS,
    DEFAULT_RULES,
    WW,
    CompiledRule,
    D,
    Hit,
    RuleCfg,
    code_candidates,
    compile_rules,
    decide,
    hits,
)

log = logging.getLogger(__name__)

NAME = "ticket_system"
PROTOCOLS = ("ticket.d", "ticket.ww", "ticket.q")


class TSConfig(BaseModel):
    burst_s: float = 60.0  # C-ticket looks for a trigger in the messages of the ticket's first minute
    rules: list[RuleCfg] = Field(default_factory=lambda: [RuleCfg.model_validate(r) for r in DEFAULT_RULES])
    code_exclusions: list[str] = Field(default_factory=lambda: list(DEFAULT_CODE_EXCLUSIONS))  # never codes


@dataclass(frozen=True, slots=True)
class PlayerMessage:
    tg_id: int
    account_id: int
    msg_id: int
    msg_date: datetime
    text: str
    media_type: str | None


_compiled: dict[str, list[CompiledRule]] = {}

# Nothing after the fact (D-TS-4): only messages written after TS started make tickets. Set by
# TicketSystemModule.start(); None (TS not started) -> nothing is taken.
_accept_from: datetime | None = None


def accept_from(moment: datetime | None) -> None:
    global _accept_from
    _accept_from = moment


def config(runtime: Any) -> TSConfig | None:
    """The configuration of TS, or None when TS is not configured or switched off (then it takes nothing)."""
    mod = runtime.config.modules.get(NAME)
    if mod is None or not mod.enabled:
        return None
    return TSConfig.model_validate(mod.config)


def rules(cfg: TSConfig) -> list[CompiledRule]:
    key = repr([r.model_dump() for r in cfg.rules])
    if key not in _compiled:
        _compiled.clear()
        _compiled[key] = compile_rules(cfg.rules)
    return _compiled[key]


async def receive(pool: asyncpg.Pool, runtime: Any, msg: PlayerMessage) -> bool:
    """A private message of a player into their open ticket (a new one if there is none).

    Returns True when a protocol was written (the caller wakes the protocols). A repeat of the same message
    changes nothing.
    """
    cfg = config(runtime)
    if cfg is None or _accept_from is None or msg.msg_date < _accept_from:
        return False  # TS off, not started, or a message written before it started (history, catch-up)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO ts_tickets (tg_id, account_id, first_msg_id, opened_at, decide_by, last_msg_at) "
            "VALUES ($1, $2, $3, $4, $4::timestamptz + make_interval(secs => $5::float8), $4) "
            "ON CONFLICT (tg_id) WHERE state = 'open' DO NOTHING",
            msg.tg_id, msg.account_id, msg.msg_id, msg.msg_date, cfg.burst_s)
        ticket = await conn.fetchrow("SELECT id, ticket, decide_by FROM ts_tickets WHERE tg_id = $1 "
                                     "AND state = 'open' FOR UPDATE", msg.tg_id)
        ticket_id = int(ticket["id"])
        # C-ticket looks only while the type is open: a typed ticket (D, WW or Q) is not searched any more.
        searching = ticket["ticket"] is None and msg.msg_date <= ticket["decide_by"]
        codes = code_candidates(msg.text, cfg.code_exclusions) if searching else []
        found = hits(msg.text, msg.media_type, rules(cfg)) if searching else []
        added = await conn.fetchval(
            "INSERT INTO ts_ticket_messages (account_id, msg_id, ticket_id, msg_date, media_type, d_rules, w_rules, "
            "codes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING RETURNING ticket_id",
            msg.account_id, msg.msg_id, ticket_id, msg.msg_date, msg.media_type,
            sorted({h.rule for h in found if h.kind == D}), sorted({h.rule for h in found if h.kind == WW}), codes)
        if added is None:
            return False  # this message is in already (a repeat after a crash)
        await conn.execute("UPDATE ts_tickets SET messages = messages + 1, last_msg_at = greatest(last_msg_at, $2), "
                           "updated_at = now() WHERE id = $1", ticket_id, msg.msg_date)
        if not searching:
            return False
        # 1. A code the player sent: a known withdrawal code -> WW at once (D-TS-5).
        known = await TS_codes.find(conn, codes) if codes else None
        if known is not None:
            return await set_ticket(conn, ticket_id, WW, "code", [Hit("w.code", WW, known)], msg.msg_id)
        # 2. The triggers: D only -> D, WW only -> WW, both -> Q; nothing -> wait (Q when the minute ends).
        decided = decide(found)
        if decided is None:
            return False
        return await set_ticket(conn, ticket_id, decided[0], decided[1], found, msg.msg_id)


async def set_ticket(conn: asyncpg.Connection, ticket_id: int, ticket: str, reason: str, found: list[Hit],
                     msg_id: int | None) -> bool:
    """Write the type and the protocol ticket.<type> in one transaction (D-RM-12)."""
    fragments = list(dict.fromkeys(h.fragment for h in found))
    row = await conn.fetchrow(
        "UPDATE ts_tickets SET ticket = $2, reason = $3, trigger_rules = $4, trigger_text = $5, trigger_msg_id = $6, "
        "classified_at = now(), updated_at = now() WHERE id = $1 RETURNING tg_id, account_id",
        ticket_id, ticket, reason, sorted({h.rule for h in found}), ", ".join(fragments) or None, msg_id)
    log.info("TS: C-ticket: ticket %s of %s -> %s (%s: %s)", ticket_id, row["tg_id"], ticket, reason, fragments)
    return await emit(conn, f"ticket.{ticket.lower()}", ticket_id, {
        "ticket_id": ticket_id, "tg_id": int(row["tg_id"]), "account_id": int(row["account_id"]),
        "ticket": ticket, "reason": reason, "rules": sorted({h.rule for h in found}), "trigger": fragments,
        "msg_id": msg_id})
