"""Players: connected groups of identifiers TGID and MelID (FT-И.1..И.5).

The graph is kept in PostgreSQL: every identifier row points at its group (player_id). A new
TGID <-> MelID pair joins the two groups; a large join waits for a staff confirmation
(FT-И.3); every join is journaled with the identifiers it moved, so it can be split back.
Writes are serialized by an advisory lock and keyed by natural keys, so processing the same
event again changes nothing (FT-8.2).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import asyncpg

LOCK = 0x504C4159  # "PLAY"
KINDS = ("tg", "mel")


@dataclass(slots=True)
class Observed:
    player_id: int
    merged: int | None = None  # id of the merge journal row, when two groups were joined
    pending: int | None = None  # id of a merge waiting for confirmation
    new_pending: bool = False  # this event created that waiting merge


async def _player_of(conn: asyncpg.Connection, kind: str, value: str) -> int | None:
    pid: int | None = await conn.fetchval("SELECT player_id FROM player_idents WHERE kind = $1 AND value = $2",
                                          kind, value)
    return pid


async def _size(conn: asyncpg.Connection, player_id: int) -> int:
    n: int = await conn.fetchval("SELECT count(*) FROM player_idents WHERE player_id = $1", player_id)
    return n


async def _merge(conn: asyncpg.Connection, into: int, src: int, reason: dict[str, Any], author: str | None,
                 status: str = "done", merge_id: int | None = None) -> int:
    moved = await conn.fetch(
        "UPDATE player_idents SET player_id = $1 WHERE player_id = $2 RETURNING kind, value", into, src)
    moved_json = json.dumps([{"kind": r["kind"], "value": r["value"], "from": src} for r in moved])
    if merge_id is None:
        mid: int = await conn.fetchval(
            "INSERT INTO player_merges (into_player, from_player, reason, moved, status, author, decided_at) "
            "VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, now()) RETURNING id",
            into, src, json.dumps(reason, ensure_ascii=False), moved_json, status, author)
        return mid
    await conn.execute("UPDATE player_merges SET moved = $2::jsonb, status = 'done', author = $3, decided_at = now() "
                       "WHERE id = $1", merge_id, moved_json, author)
    return merge_id


async def observe_pair(conn: asyncpg.Connection, tgid: str, mel_id: str, *, trace_id: str | None,
                       threshold: int) -> Observed:
    """A deposit showed that this TGID uses this MelID (FT-И.1)."""
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", LOCK)
        fresh = await conn.fetchval(
            "INSERT INTO player_pairs (tgid, mel_id, trace_id) VALUES ($1, $2, $3::uuid) "
            "ON CONFLICT DO NOTHING RETURNING 1", tgid, mel_id, trace_id)
        pt, pm = await _player_of(conn, "tg", tgid), await _player_of(conn, "mel", mel_id)
        if not fresh:
            # The pair is known: a repeat of the same event, or a pair a staff member split apart.
            return Observed(pt or pm or 0)
        if pt is None and pm is None:
            pid: int = await conn.fetchval("SELECT nextval('players_seq')")
            await conn.executemany("INSERT INTO player_idents (kind, value, player_id) VALUES ($1, $2, $3)",
                                   [("tg", tgid, pid), ("mel", mel_id, pid)])
            return Observed(pid)
        if pt is None or pm is None:
            known = pt if pt is not None else pm
            assert known is not None
            kind, value = ("tg", tgid) if pt is None else ("mel", mel_id)
            await conn.execute("INSERT INTO player_idents (kind, value, player_id) VALUES ($1, $2, $3)",
                               kind, value, known)
            return Observed(known)
        if pt == pm:
            return Observed(pt)
        # Two groups meet: the larger keeps its id; a join above the threshold waits (FT-И.3).
        st, sm = await _size(conn, pt), await _size(conn, pm)
        into, src = (pt, pm) if (st, -pt) >= (sm, -pm) else (pm, pt)
        reason = {"tgid": tgid, "mel_id": mel_id, "trace_id": trace_id}
        if st + sm > threshold:
            existing = await conn.fetchval(
                "SELECT id FROM player_merges WHERE status = 'pending' AND "
                "least(into_player, from_player) = least($1::bigint, $2::bigint) AND "
                "greatest(into_player, from_player) = greatest($1::bigint, $2::bigint)", into, src)
            if existing is not None:
                return Observed(pt, pending=existing)
            mid: int = await conn.fetchval(
                "INSERT INTO player_merges (into_player, from_player, reason, status) "
                "VALUES ($1, $2, $3::jsonb, 'pending') RETURNING id", into, src, json.dumps(reason))
            return Observed(pt, pending=mid, new_pending=True)
        return Observed(into, merged=await _merge(conn, into, src, reason, None))


async def decide(conn: asyncpg.Connection, merge_id: int, *, accept: bool, author: str) -> str:
    """A staff decision on a pending join."""
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", LOCK)
        row = await conn.fetchrow("SELECT * FROM player_merges WHERE id = $1 FOR UPDATE", merge_id)
        if row is None or row["status"] != "pending":
            return "not_pending"
        if not accept:
            await conn.execute("UPDATE player_merges SET status = 'rejected', author = $2, decided_at = now() "
                               "WHERE id = $1", merge_id, author)
            return "rejected"
        # The groups may have changed since: join whatever groups the two identifiers are in now.
        into, src = row["into_player"], row["from_player"]
        if await _size(conn, src) == 0:
            return "gone"
        await _merge(conn, into, src, json.loads(row["reason"]) if isinstance(row["reason"], str)
                     else dict(row["reason"]), author, merge_id=merge_id)
        return "done"


async def split(conn: asyncpg.Connection, merge_id: int, *, author: str) -> str:
    """Manual split (FT-И.3): the identifiers moved by that join go back to their old group."""
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", LOCK)
        row = await conn.fetchrow("SELECT * FROM player_merges WHERE id = $1 FOR UPDATE", merge_id)
        if row is None or row["status"] != "done":
            return "not_done"
        moved = json.loads(row["moved"]) if isinstance(row["moved"], str) else row["moved"]
        await conn.executemany(
            "UPDATE player_idents SET player_id = $3 WHERE kind = $1 AND value = $2 AND player_id = $4",
            [(m["kind"], m["value"], m["from"], row["into_player"]) for m in moved])
        await conn.execute("UPDATE player_merges SET status = 'split', author = coalesce($2, author), "
                           "decided_at = now() WHERE id = $1", merge_id, author)
        return "split"


async def note_name(conn: asyncpg.Connection, tgid: str, username: str | None, name: str | None) -> None:
    """Names and usernames are kept as history (FT-И.2)."""
    await conn.execute("INSERT INTO player_names (tgid, username, name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                       tgid, (username or "").lstrip("@"), name or "")


async def add_mark(conn: asyncpg.Connection, kind: str, ident_kind: str, ident: str, *, author: str,
                   text: str | None = None, trace_id: str | None = None) -> bool:
    """Sanctions and marks: scam strike, ban, unban, complaint, request, note (FT-И.5)."""
    if ident_kind not in KINDS:
        raise ValueError(ident_kind)
    added = await conn.fetchval(
        "INSERT INTO player_marks (kind, ident_kind, ident, text, author, trace_id) "
        "VALUES ($1, $2, $3, $4, $5, $6::uuid) ON CONFLICT DO NOTHING RETURNING 1",
        kind, ident_kind, ident, text, author, trace_id)
    return bool(added)


@dataclass(slots=True)
class Resolution:
    player_id: int | None
    ambiguous: list[int]  # several players match an identifier without a prefix (FT-12.5)
    kind: str | None = None
    value: str = ""


async def resolve(db: Any, arg: str) -> Resolution:
    """"mel:123" / "tg:123" name the kind; without a prefix both kinds are searched."""
    kind, sep, value = arg.partition(":")
    if sep and kind in KINDS:
        pid = await db.fetchval("SELECT player_id FROM player_idents WHERE kind = $1 AND value = $2", kind, value)
        return Resolution(pid, [], kind, value)
    rows = await db.fetch("SELECT DISTINCT kind, player_id FROM player_idents WHERE value = $1 ORDER BY kind", arg)
    players = sorted({r["player_id"] for r in rows})
    if len(players) > 1:
        return Resolution(None, players, None, arg)
    return Resolution(players[0] if players else None, [], rows[0]["kind"] if rows else None, arg)


async def idents(db: Any, player_id: int) -> dict[str, list[str]]:
    rows = await db.fetch("SELECT kind, value FROM player_idents WHERE player_id = $1 ORDER BY kind, value",
                          player_id)
    out: dict[str, list[str]] = {"tg": [], "mel": []}
    for r in rows:
        out[r["kind"]].append(r["value"])
    return out
