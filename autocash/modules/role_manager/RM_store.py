"""RM: reads and writes of the tables rm_* (RM_migrations/001_rm_directory.sql)."""

from __future__ import annotations

from typing import Any, cast

import asyncpg

from autocash.core.config.schema import Policy
from autocash.core.directory import RoleDef
from autocash.core.protocols import emit

_HISTORY = "INSERT INTO rm_history (author, entity, tg_id, before, after) VALUES ($1, $2, $3, $4, $5)"


async def load(pool: asyncpg.Pool) -> tuple[list[RoleDef], dict[str, set[str]], dict[int, str], dict[int, str]]:
    """Everything the Directory of the core holds."""
    async with pool.acquire() as conn:
        roles = [RoleDef(r["code"], r["scope"], r["title"], cast(Policy | None, r["policy"]))
                 for r in await conn.fetch("SELECT code, scope, title, policy FROM rm_roles ORDER BY scope, sort")]
        permissions: dict[str, set[str]] = {}
        for r in await conn.fetch("SELECT role, command FROM rm_permissions"):
            permissions.setdefault(r["role"], set()).add(r["command"])
        users = {int(r["tg_id"]): r["role"] for r in await conn.fetch("SELECT tg_id, role FROM rm_users")}
        groups = {int(r["group_tg_id"]): r["role"] for r in await conn.fetch("SELECT group_tg_id, role FROM rm_groups")}
    return roles, permissions, users, groups


async def add_users(pool: asyncpg.Pool, users: dict[int, str], *, source: str) -> list[int]:
    """Users that rm_users does not have yet; the role of a user who is there is not changed."""
    added: list[int] = []
    async with pool.acquire() as conn, conn.transaction():
        for tg_id, role in users.items():
            row = await conn.fetchrow(
                "INSERT INTO rm_users (tg_id, role, source) VALUES ($1, $2, $3) ON CONFLICT (tg_id) DO NOTHING "
                "RETURNING tg_id", tg_id, role, source)
            if row is not None:
                await conn.execute("INSERT INTO rm_history (author, entity, tg_id, before, after) "
                                   "VALUES (NULL, 'user', $1, NULL, $2)", tg_id, role)
                added.append(tg_id)
    return added


async def add_contact(pool: asyncpg.Pool, tg_id: int, protocol: str, data: dict[str, Any]) -> bool:
    """A user who wrote in private for the first time, with the run of the contact protocol, in one
    transaction (D-RM-11, D-RM-12). False: the user is already there (no protocol)."""
    kind = protocol.removeprefix("contact.")
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            "INSERT INTO rm_users (tg_id, role, source, had_dm, contact_protocol, contact_at) "
            "VALUES ($1, 'guest', 'dm', true, $2, now()) ON CONFLICT (tg_id) DO NOTHING RETURNING tg_id",
            tg_id, kind)
        if row is None:
            return False
        await conn.execute(_HISTORY, None, "user", tg_id, None, "guest")
        await emit(conn, protocol, tg_id, data)
        return True


async def dialog_seen(pool: asyncpg.Pool, chat_id: int, before_ingest_id: int) -> bool:
    """Our base has messages of this private dialog before the given one (any pool account)."""
    return bool(await pool.fetchval(
        "SELECT EXISTS (SELECT 1 FROM messages WHERE chat_id = $1 AND ingest_id < $2)", chat_id, before_ingest_id))


async def user(pool: asyncpg.Pool, tg_id: int) -> Any:
    return await pool.fetchrow("SELECT user_id, tg_id, username, role FROM rm_users WHERE tg_id = $1", tg_id)


async def set_user_role(pool: asyncpg.Pool, tg_id: int, role: str, author: int) -> str | None:
    """The role of an existing user; returns the previous role (None: there is no such user)."""
    async with pool.acquire() as conn, conn.transaction():
        before = await conn.fetchval("SELECT role FROM rm_users WHERE tg_id = $1 FOR UPDATE", tg_id)
        if before is None:
            return None
        await conn.execute("UPDATE rm_users SET role = $2, updated_at = now(), updated_by = $3 WHERE tg_id = $1",
                           tg_id, role, author)
        await conn.execute(_HISTORY, author, "user", tg_id, before, role)
        return str(before)


async def set_group_role(pool: asyncpg.Pool, group_tg_id: int, role: str, author: int) -> str | None:
    """The role of a group (the row is made if there is none); returns the previous role."""
    async with pool.acquire() as conn, conn.transaction():
        before = await conn.fetchval("SELECT role FROM rm_groups WHERE group_tg_id = $1 FOR UPDATE", group_tg_id)
        await conn.execute(
            "INSERT INTO rm_groups (group_tg_id, role, updated_by) VALUES ($1, $2, $3) ON CONFLICT (group_tg_id) "
            "DO UPDATE SET role = EXCLUDED.role, updated_at = now(), updated_by = EXCLUDED.updated_by",
            group_tg_id, role, author)
        await conn.execute(_HISTORY, author, "group", group_tg_id, before, role)
        return None if before is None else str(before)


async def member_groups(pool: asyncpg.Pool) -> list[int]:
    """Marked ids of the groups where accounts of the pool are."""
    rows = await pool.fetch("SELECT DISTINCT chat_id FROM chat_membership WHERE chat_id < 0")
    return [int(r["chat_id"]) for r in rows]
