"""Internal actions of the core over entities (FT-8.4): lookup, upsert, link, set_state, match, report.

Writes go through the module Context and are committed together with the module's processing
mark (FT-8.3), so a repeated run of the same event changes nothing. Reads go straight to
PostgreSQL. These actions know nothing about deposits or players (P-1): modules pass types,
keys and attributes.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Protocol

import asyncpg


class Writes(Protocol):
    """What the actions need from a module Context."""

    def execute(self, sql: str, *args: Any) -> None: ...


class Reads(Protocol):
    async def fetchrow(self, sql: str, *args: Any) -> Any: ...

    async def fetch(self, sql: str, *args: Any) -> list[Any]: ...


# ---- lookup / upsert / link / set_state ------------------------------------------------------------
async def lookup(db: Reads, entity_type: str, key: str) -> dict[str, Any] | None:
    row = await db.fetchrow(
        "SELECT entity_key, state, attrs, state_until, created_at, updated_at FROM entities "
        "WHERE entity_type = $1 AND entity_key = $2", entity_type, key)
    if row is None:
        return None
    attrs = row["attrs"]
    return {"key": row["entity_key"], "state": row["state"], "state_until": row["state_until"],
            "attrs": json.loads(attrs) if isinstance(attrs, str) else dict(attrs or {}),
            "created_at": row["created_at"], "updated_at": row["updated_at"]}


def upsert(ctx: Writes, entity_type: str, key: str, *, state: str | None = None,
           attrs: dict[str, Any] | None = None) -> None:
    """Create or update; attributes are merged (new values win), the state changes only if given."""
    ctx.execute(
        "INSERT INTO entities (entity_type, entity_key, state, attrs) VALUES ($1, $2, $3, $4::jsonb) "
        "ON CONFLICT (entity_type, entity_key) DO UPDATE SET state = coalesce(EXCLUDED.state, entities.state), "
        "attrs = entities.attrs || EXCLUDED.attrs, updated_at = now()",
        entity_type, key, state, json.dumps(attrs or {}, ensure_ascii=False, default=str),
    )


def link(ctx: Writes, src: tuple[str, str], dst: tuple[str, str], relation: str = "",
         attrs: dict[str, Any] | None = None) -> None:
    """Link two entities, or a message (type "message", key "chat:msg") with an entity."""
    ctx.execute(
        "INSERT INTO entity_links (from_type, from_key, to_type, to_key, relation, attrs) "
        "VALUES ($1, $2, $3, $4, $5, $6::jsonb) ON CONFLICT DO NOTHING",
        src[0], src[1], dst[0], dst[1], relation, json.dumps(attrs or {}, ensure_ascii=False, default=str),
    )


def message_key(chat_id: int, msg_id: int) -> tuple[str, str]:
    return ("message", f"{chat_id}:{msg_id}")


def set_state(ctx: Writes, entity_type: str, key: str, state: str, *, timeout_s: float | None = None,
              timeout_state: str | None = None) -> None:
    """Change the state; with a timeout the entity moves to `timeout_state` when it passes."""
    if (timeout_s is None) != (timeout_state is None):
        raise ValueError("timeout_s and timeout_state go together")
    ctx.execute(
        "INSERT INTO entities (entity_type, entity_key, state, state_until, timeout_state) "
        "VALUES ($1, $2, $3, CASE WHEN $4::float8 IS NULL THEN NULL ELSE now() + make_interval(secs => $4) END, $5) "
        "ON CONFLICT (entity_type, entity_key) DO UPDATE SET state = EXCLUDED.state, "
        "state_until = EXCLUDED.state_until, timeout_state = EXCLUDED.timeout_state, updated_at = now()",
        entity_type, key, state, timeout_s, timeout_state,
    )


async def expire_states(conn: asyncpg.Connection) -> int:
    """Apply passed timeouts of set_state; called by the core maintenance."""
    status: str = await conn.execute(
        "UPDATE entities SET state = timeout_state, state_until = NULL, timeout_state = NULL, updated_at = now() "
        "WHERE state_until IS NOT NULL AND state_until <= now()")
    return int(status.split()[-1])


# ---- match ------------------------------------------------------------------------------------------
@dataclass(slots=True)
class Candidate:
    key: str
    score: float  # 0..1
    attrs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Window:
    field: str            # attribute holding an ISO time or epoch seconds
    center: datetime
    seconds: float


@dataclass(frozen=True, slots=True)
class Fuzzy:
    field: str
    value: str
    min_score: float = 0.8


_pool: ProcessPoolExecutor | None = None


def _executor() -> ProcessPoolExecutor:
    # Fuzzy matching runs outside the event loop, in a separate process (A-7).
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=1)
    return _pool


def shutdown_executor() -> None:
    global _pool
    if _pool is not None:
        _pool.shutdown(cancel_futures=True)
        _pool = None


def _fuzzy_scores(value: str, others: Sequence[str]) -> list[float]:
    value = value.casefold()
    return [SequenceMatcher(None, value, (o or "").casefold()).ratio() for o in others]


def _to_epoch(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return None


async def score(candidates: list[Candidate], *, window: Window | None = None,
                fuzzy: Fuzzy | None = None) -> list[Candidate]:
    """Score candidates: closeness in time and similarity of text multiply; best first."""
    out = []
    sims: list[float] = []
    if fuzzy is not None and candidates:
        values = [str(c.attrs.get(fuzzy.field) or "") for c in candidates]
        if len(values) > 50:
            sims = await asyncio.get_running_loop().run_in_executor(_executor(), _fuzzy_scores, fuzzy.value, values)
        else:
            sims = _fuzzy_scores(fuzzy.value, values)
    for i, cand in enumerate(candidates):
        s = cand.score
        if window is not None:
            at = _to_epoch(cand.attrs.get(window.field))
            if at is None:
                continue
            gap = abs(at - window.center.timestamp())
            if gap > window.seconds:
                continue
            s *= 1.0 - 0.5 * gap / max(window.seconds, 1e-9)
        if fuzzy is not None:
            if sims[i] < fuzzy.min_score:
                continue
            s *= sims[i]
        out.append(Candidate(cand.key, s, cand.attrs))
    out.sort(key=lambda c: (-c.score, c.key))
    return out


async def match(db: Reads, entity_type: str, *, exact: dict[str, Any] | None = None,
                states: list[str] | None = None, window: Window | None = None, fuzzy: Fuzzy | None = None,
                limit: int = 200) -> list[Candidate]:
    """Candidates among entities of a type: exact attributes, time window, fuzzy text (FT-8.4)."""
    rows = await db.fetch(
        "SELECT entity_key, attrs FROM entities WHERE entity_type = $1 AND attrs @> $2::jsonb "
        "AND ($3::text[] IS NULL OR state = ANY($3)) ORDER BY updated_at DESC LIMIT $4",
        entity_type, json.dumps(exact or {}, ensure_ascii=False, default=str), states, limit)
    cands = [Candidate(r["entity_key"], 1.0, json.loads(r["attrs"]) if isinstance(r["attrs"], str)
                       else dict(r["attrs"])) for r in rows]
    return await score(cands, window=window, fuzzy=fuzzy)


# ---- report ----------------------------------------------------------------------------------------
ReportFn = Callable[[asyncpg.Pool, dict[str, Any]], Awaitable[list[Any]]]
_REPORTS: dict[str, ReportFn] = {}


def register_report(name: str, fn: ReportFn) -> None:
    """Modules register named reports; `report()` runs them on the reports pool (NFT-15)."""
    if name in _REPORTS and _REPORTS[name] is not fn:
        raise ValueError(f"report {name!r} is already registered")
    _REPORTS[name] = fn


def sql_report(sql: str) -> ReportFn:
    """A report that is one parameterised query; params are passed by name in order of `$n`."""
    async def run(pool: asyncpg.Pool, params: dict[str, Any]) -> list[Any]:
        rows: list[Any] = await pool.fetch(sql, *params.values())
        return rows
    return run


async def report(pool: asyncpg.Pool, name: str, **params: Any) -> list[Any]:
    fn = _REPORTS.get(name)
    if fn is None:
        raise KeyError(f"unknown report {name!r}")
    return await fn(pool, params)
