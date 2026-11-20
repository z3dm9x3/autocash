"""PL: players as a graph of TGID <-> MelID links (PL_readme.md, D-PL-1..4).

- A link is one observed pair "this TGID used this MelID". Links are never deleted.
- A player is a connected group of active links: a TGID with two MelIDs, or a MelID used by two TGIDs,
  makes one player. There is no size limit (one person may have thousands of MelIDs).
- A new pair whose identifiers are both unknown makes a new player; a pair touching one known player joins
  it; a pair touching two players joins them: the older player (smaller id) stays, the younger one gets
  `merged_into`. Every identifier of an active link is in `pl_nodes` with its current player.
- A link cut by hand (state disabled) or kept as staff joins nothing; the player is recomputed from the
  remaining active links and may fall apart (split): the part with the player's first link keeps its id.

All writes go under one advisory lock, so joins never race. A repeat of the same pair changes nothing but
the time it was last seen (the time of the event, so a repeat of the same event changes nothing at all).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import asyncpg

LOCK = 0x504C4159  # "PLAY"
KINDS = ("tg", "mel")
STATES = ("active", "staff", "disabled")


@dataclass(slots=True)
class Linked:
    link_id: int
    player_id: int | None  # the player of the pair now; a staff/disabled link: the MelID's player, if any
    new_link: bool  # the pair was seen for the first time
    created: bool = False  # a new player was made
    joined: list[int] = field(default_factory=list)  # players joined into player_id by this pair


async def _lock(conn: asyncpg.Connection) -> None:
    await conn.execute("SELECT pg_advisory_xact_lock($1)", LOCK)


async def _node(conn: asyncpg.Connection, kind: str, value: str) -> int | None:
    pid: int | None = await conn.fetchval("SELECT player_id FROM pl_nodes WHERE kind = $1 AND value = $2",
                                          kind, value)
    return pid


async def _union(conn: asyncpg.Connection, link_id: int, tg_id: int, mel_id: str,
                 author: str | None = None) -> tuple[int, bool, list[int]]:
    """Put both identifiers of an active link into one player: (player, created, joined players)."""
    tg = str(tg_id)
    pt, pm = await _node(conn, "tg", tg), await _node(conn, "mel", mel_id)
    if pt is None and pm is None:
        pid: int = await conn.fetchval("INSERT INTO pl_players (first_link_id) VALUES ($1) RETURNING player_id",
                                       link_id)
        await conn.executemany("INSERT INTO pl_nodes (kind, value, player_id) VALUES ($1, $2, $3)",
                               [("tg", tg, pid), ("mel", mel_id, pid)])
        return pid, True, []
    if pt is None or pm is None:
        known = pt if pt is not None else pm
        assert known is not None
        kind, value = ("tg", tg) if pt is None else ("mel", mel_id)
        await conn.execute("INSERT INTO pl_nodes (kind, value, player_id) VALUES ($1, $2, $3)", kind, value, known)
        return known, False, []
    if pt == pm:
        return pt, False, []
    # Two players meet: the older one (smaller id) stays.
    into, src = min(pt, pm), max(pt, pm)
    moved = await conn.fetchval(
        "WITH m AS (UPDATE pl_nodes SET player_id = $1, since = now() WHERE player_id = $2 RETURNING 1) "
        "SELECT count(*) FROM m", into, src)
    # Ids already pointing at the younger player point at the one that stays: a chain is one step long.
    await conn.execute("UPDATE pl_players SET merged_into = $1, merged_at = now() "
                       "WHERE player_id = $2 OR merged_into = $2", into, src)
    await conn.execute(
        "INSERT INTO pl_journal (kind, into_player, from_player, link_id, moved, data, author) "
        "VALUES ('join', $1, $2, $3, $4, $5::jsonb, $6)",
        into, src, link_id, moved, json.dumps({"tg_id": tg_id, "mel_id": mel_id}), author)
    return into, False, [src]


async def add_link(pool: asyncpg.Pool, tg_id: int, mel_id: str, *, source: str, ref: str | None = None,
                   staff: bool = False, at: datetime | None = None) -> Linked:
    """A pair "this TGID used this MelID" was seen (by `source`, e.g. autosup or msg:deposit.request).

    `staff`: the TGID is a cashier or staff acting for players; the pair is kept but joins nothing.
    `at`: when it was seen (the event time; default now).
    """
    mel_id = str(mel_id).strip()
    async with pool.acquire() as conn, conn.transaction():
        await _lock(conn)
        row = await conn.fetchrow(
            "INSERT INTO pl_links (tg_id, mel_id, state, source, ref, first_seen, last_seen) "
            "VALUES ($1, $2, $3, $4, $5, coalesce($6, now()), coalesce($6, now())) "
            "ON CONFLICT (tg_id, mel_id) DO UPDATE SET "
            "first_seen = least(pl_links.first_seen, EXCLUDED.first_seen), "
            "last_seen = greatest(pl_links.last_seen, EXCLUDED.last_seen) "
            "RETURNING link_id, state, (xmax = 0) AS inserted",
            tg_id, mel_id, "staff" if staff else "active", source, ref, at)
        link_id, state, inserted = int(row["link_id"]), str(row["state"]), bool(row["inserted"])
        if state != "active":
            return Linked(link_id, await _node(conn, "mel", mel_id), inserted)
        pid, created, joined = await _union(conn, link_id, tg_id, mel_id)
        return Linked(link_id, pid, inserted, created, joined)


async def heal(pool: asyncpg.Pool) -> int:
    """Active links whose identifiers are missing from pl_nodes or sit in different players are joined.

    Runs at start: links copied by the migration (Autosup, RM) get their players here; normally a no-op.
    """
    async with pool.acquire() as conn, conn.transaction():
        await _lock(conn)
        rows = await conn.fetch(
            "SELECT l.link_id, l.tg_id, l.mel_id FROM pl_links l "
            "LEFT JOIN pl_nodes t ON t.kind = 'tg' AND t.value = l.tg_id::text "
            "LEFT JOIN pl_nodes m ON m.kind = 'mel' AND m.value = l.mel_id "
            "WHERE l.state = 'active' AND (t.player_id IS NULL OR m.player_id IS NULL OR t.player_id <> m.player_id) "
            "ORDER BY l.link_id")
        for r in rows:
            await _union(conn, int(r["link_id"]), int(r["tg_id"]), str(r["mel_id"]))
        return len(rows)


async def set_state(pool: asyncpg.Pool, tg_id: int, mel_id: str, state: str, *, author: str) -> str:
    """A link by hand: active (joins), disabled (cut) or staff. Returns missing | same | done.

    Cutting an active link recomputes its player from the remaining active links (split).
    """
    if state not in STATES:
        raise ValueError(state)
    async with pool.acquire() as conn, conn.transaction():
        await _lock(conn)
        row = await conn.fetchrow("SELECT link_id, state FROM pl_links WHERE tg_id = $1 AND mel_id = $2 FOR UPDATE",
                                  tg_id, mel_id)
        if row is None:
            return "missing"
        link_id, before = int(row["link_id"]), str(row["state"])
        if before == state:
            return "same"
        await conn.execute("UPDATE pl_links SET state = $2, changed_by = $3 WHERE link_id = $1", link_id, state, author)
        await conn.execute(
            "INSERT INTO pl_journal (kind, link_id, data, author) VALUES ('state', $1, $2::jsonb, $3)",
            link_id, json.dumps({"tg_id": tg_id, "mel_id": mel_id, "before": before, "after": state}), author)
        if state == "active":
            await _union(conn, link_id, tg_id, mel_id, author)
        elif before == "active":
            pid = await _node(conn, "tg", str(tg_id))
            if pid is not None:
                await _recompute(conn, pid, link_id, author)
        return "done"


async def _recompute(conn: asyncpg.Connection, player_id: int, link_id: int, author: str | None) -> None:
    """Rebuild a player from its active links: parts that lost their connection become new players."""
    nodes = await conn.fetch("SELECT kind, value FROM pl_nodes WHERE player_id = $1", player_id)
    tgs = [int(r["value"]) for r in nodes if r["kind"] == "tg"]
    mels = [str(r["value"]) for r in nodes if r["kind"] == "mel"]
    links = await conn.fetch(
        "SELECT link_id, tg_id, mel_id FROM pl_links WHERE state = 'active' "
        "AND tg_id = ANY($1::bigint[]) AND mel_id = ANY($2::text[])", tgs, mels)
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(x: tuple[str, str]) -> tuple[str, str]:
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for r in links:
        ra, rb = find(("tg", str(r["tg_id"]))), find(("mel", str(r["mel_id"])))
        if ra != rb:
            parent[ra] = rb
    parts: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for n in parent:
        parts.setdefault(find(n), []).append(n)
    part_links: dict[tuple[str, str], set[int]] = {}
    for r in links:
        part_links.setdefault(find(("tg", str(r["tg_id"]))), set()).add(int(r["link_id"]))
    loose = [(str(r["kind"]), str(r["value"])) for r in nodes if (str(r["kind"]), str(r["value"])) not in parent]
    if loose:  # identifiers left without an active link belong to no player
        await conn.executemany("DELETE FROM pl_nodes WHERE kind = $1 AND value = $2", loose)
    first_link = await conn.fetchval("SELECT first_link_id FROM pl_players WHERE player_id = $1", player_id)
    # The part with the player's first link keeps the id; without it - the largest part, then the oldest.
    roots = sorted(parts, key=lambda r: (first_link not in part_links[r], -len(parts[r]), min(part_links[r])))
    for root in roots[1:]:
        group = parts[root]
        new_pid: int = await conn.fetchval(
            "INSERT INTO pl_players (first_link_id) VALUES ($1) RETURNING player_id", min(part_links[root]))
        await conn.executemany("UPDATE pl_nodes SET player_id = $3, since = now() WHERE kind = $1 AND value = $2",
                               [(k, v, new_pid) for k, v in group])
        await conn.execute(
            "INSERT INTO pl_journal (kind, into_player, from_player, link_id, moved, data, author) "
            "VALUES ('split', $1, $2, $3, $4, '{}'::jsonb, $5)", new_pid, player_id, link_id, len(group), author)
    if loose and len(roots) <= 1:
        await conn.execute(
            "INSERT INTO pl_journal (kind, into_player, from_player, link_id, moved, data, author) "
            "VALUES ('split', NULL, $1, $2, $3, $4::jsonb, $5)",
            player_id, link_id, len(loose), json.dumps({"loose": [f"{k}:{v}" for k, v in loose]}), author)


# ---- reads -------------------------------------------------------------------------------------------------
async def player_of(db: Any, kind: str, value: str | int) -> int | None:
    pid: int | None = await db.fetchval("SELECT player_id FROM pl_nodes WHERE kind = $1 AND value = $2",
                                        kind, str(value))
    return pid


async def current(db: Any, player_id: int) -> int:
    """The player an old id stands for now (after joins the younger id points at the one that stayed)."""
    into = await db.fetchval("SELECT merged_into FROM pl_players WHERE player_id = $1", player_id)
    return int(into) if into is not None else player_id


async def idents(db: Any, player_id: int) -> dict[str, list[str]]:
    rows = await db.fetch("SELECT kind, value FROM pl_nodes WHERE player_id = $1 ORDER BY kind, value", player_id)
    out: dict[str, list[str]] = {"tg": [], "mel": []}
    for r in rows:
        out[r["kind"]].append(r["value"])
    return out


async def mels_of_tg(db: Any, tg_id: int) -> list[str]:
    """Every MelID of the person behind this Telegram account (empty: the account is in no player)."""
    pid = await player_of(db, "tg", tg_id)
    return [] if pid is None else (await idents(db, pid))["mel"]


async def links_of(db: Any, tgs: Iterable[int], mels: Iterable[str]) -> list[Any]:
    """Every link touching these identifiers, any state (for the card of a player)."""
    rows: list[Any] = await db.fetch(
        "SELECT link_id, tg_id, mel_id, state, source, first_seen, last_seen FROM pl_links "
        "WHERE tg_id = ANY($1::bigint[]) OR mel_id = ANY($2::text[]) ORDER BY link_id",
        [int(t) for t in tgs], list(mels))
    return rows


@dataclass(slots=True)
class Resolution:
    player_id: int | None
    ambiguous: list[int]  # several players match an identifier without a prefix (FT-12.5)
    kind: str | None = None
    value: str = ""


async def resolve(db: Any, arg: str) -> Resolution:
    """"mel:123" / "tg:123" name the kind; "#5" / "p:5" is a player id; without a prefix both kinds are searched."""
    arg = arg.strip()
    kind, sep, value = arg.partition(":")
    if (arg.startswith("#") and arg[1:].isdigit()) or (sep and kind == "p" and value.isdigit()):
        pid = int(arg[1:] if arg.startswith("#") else value)
        exists = await db.fetchval("SELECT 1 FROM pl_players WHERE player_id = $1", pid)
        return Resolution(await current(db, pid) if exists else None, [], "player", str(pid))
    if sep and kind in KINDS:
        return Resolution(await player_of(db, kind, value), [], kind, value)
    rows = await db.fetch("SELECT DISTINCT kind, player_id FROM pl_nodes WHERE value = $1 ORDER BY kind", arg)
    players = sorted({int(r["player_id"]) for r in rows})
    if len(players) > 1:
        return Resolution(None, players, None, arg)
    return Resolution(players[0] if players else None, [], rows[0]["kind"] if rows else None, arg)


# ---- names and marks ---------------------------------------------------------------------------------------
async def note_name(conn: Any, tg_id: int, username: str | None, name: str | None) -> None:
    """Names and usernames are kept as history (FT-И.2)."""
    await conn.execute("INSERT INTO pl_names (tg_id, username, name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                       tg_id, (username or "").lstrip("@"), name or "")


async def add_mark(conn: Any, kind: str, ident_kind: str, ident: str, *, author: str,
                   text: str | None = None, trace_id: str | None = None) -> bool:
    """Sanctions and marks: scam strike, ban, unban, complaint, request, note (FT-И.5)."""
    if ident_kind not in KINDS:
        raise ValueError(ident_kind)
    added = await conn.fetchval(
        "INSERT INTO pl_marks (kind, ident_kind, ident, text, author, trace_id) "
        "VALUES ($1, $2, $3, $4, $5, $6::uuid) ON CONFLICT DO NOTHING RETURNING 1",
        kind, ident_kind, ident, text, author, trace_id)
    return bool(added)
