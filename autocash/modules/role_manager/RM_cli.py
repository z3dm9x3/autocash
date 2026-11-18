"""RM: tools outside the running service (RM_readme.md).

    python -m autocash.modules.role_manager.RM_cli user <TGID> <role>
    python -m autocash.modules.role_manager.RM_cli import-cmd --source-ini <settings.ini of Cash Monitor Dev>

The database of Autocash: $AUTOCASH_PG_DSN, or `--env-file` with a line AUTOCASH_PG_DSN=...
No DSN and no password is ever printed.

`import-cmd` copies the Telegram users known to Cash Monitor Dev into rm_users (D-RM-13):
- the source is only READ: one read-only transaction with a statement timeout;
- no call to Telegram at all: FloodWait is impossible;
- new users get the role guest and source "import:cmd"; a user who is there keeps their role;
- users who had a private dialog with the account get had_dm = true (no first-contact protocol);
- roles of Cash Monitor Dev (ADMIN, CASHER) are NOT copied: they are written to a CSV for review.
"""

from __future__ import annotations

import argparse
import asyncio
import configparser
import csv
import os
import sys
from pathlib import Path
from typing import Any

import asyncpg

from autocash.modules.role_manager import RM_store as store

BATCH = 1000

# Cash Monitor Dev: where Telegram user ids are (read only). Usernames: the latest non-empty one.
_SOURCE_QUERIES: dict[str, str] = {
    "sa_deposits": """
        SELECT "TGID"::bigint AS tg_id,
               (array_agg(ltrim(trim("Username"), '@') ORDER BY "DATE" DESC)
                   FILTER (WHERE coalesce(trim("Username"), '') <> ''))[1] AS username
        FROM sa_deposits WHERE "TGID" ~ '^[0-9]{5,12}$' GROUP BY 1""",
    "raw_senders": "SELECT DISTINCT sender_id AS tg_id, NULL::text AS username FROM raw_messages WHERE sender_id > 0",
    "seeker_applications": """SELECT DISTINCT telegram_user_id AS tg_id, NULL::text AS username
                              FROM seeker_applications WHERE telegram_user_id > 0""",
    "seeker_identifiers": """SELECT DISTINCT telegram_user_id AS tg_id, NULL::text AS username
                             FROM seeker_player_identifiers WHERE telegram_user_id > 0""",
    "user_roles": """SELECT user_id AS tg_id, NULLIF(ltrim(trim(username), '@'), '') AS username
                     FROM user_roles WHERE user_id > 0""",
}
_PRIVATE = "SELECT DISTINCT chat_id FROM raw_messages WHERE is_private AND chat_id > 0"
_ACCOUNTS = "SELECT telegram_user_id FROM telegram_accounts WHERE telegram_user_id IS NOT NULL"
_ROLES = ("SELECT user_id, username, role::text AS role FROM user_roles "
          "WHERE role::text <> 'USER' ORDER BY role, user_id")


def _dsn_from_env_file(path: Path | None) -> str:
    if os.environ.get("AUTOCASH_PG_DSN"):
        return os.environ["AUTOCASH_PG_DSN"]
    if path is not None and path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("AUTOCASH_PG_DSN="):
                return line.split("=", 1)[1].strip()
    sys.exit("AUTOCASH_PG_DSN is not set (environment or --env-file)")


def _source_dsn(ini: Path | None) -> str:
    if os.environ.get("AUTOCASH_IMPORT_SOURCE_DSN"):
        return os.environ["AUTOCASH_IMPORT_SOURCE_DSN"]
    if ini is None:
        sys.exit("give --source-ini or AUTOCASH_IMPORT_SOURCE_DSN")
    cp = configparser.ConfigParser(interpolation=None)
    cp.read(ini, encoding="utf-8")
    dsn = cp.get("postgres", "dsn", fallback="")
    if not dsn:
        sys.exit(f"{ini}: no [postgres] dsn")
    return dsn


async def read_source(dsn: str) -> tuple[dict[int, str | None], set[int], dict[str, int], list[Any]]:
    """Users of Cash Monitor Dev: tg_id -> username, ids with a private dialog, counts by table, roles."""
    conn = await asyncpg.connect(dsn, timeout=15)
    try:
        async with conn.transaction(readonly=True):
            await conn.execute("SET LOCAL statement_timeout = '300s'")
            accounts = {int(r[0]) for r in await conn.fetch(_ACCOUNTS)}
            users: dict[int, str | None] = {}
            counts: dict[str, int] = {}
            for name, sql in _SOURCE_QUERIES.items():
                rows = await conn.fetch(sql)
                counts[name] = len(rows)
                for r in rows:
                    tg_id = int(r["tg_id"])
                    if tg_id in accounts:
                        continue
                    if r["username"] or tg_id not in users:
                        users[tg_id] = r["username"] or users.get(tg_id)
            private = {int(r[0]) for r in await conn.fetch(_PRIVATE)} - accounts
            for tg_id in private:
                users.setdefault(tg_id, None)
            roles = list(await conn.fetch(_ROLES))
    finally:
        await conn.close()
    return users, private, counts, roles


async def write_target(dsn: str, users: dict[int, str | None], private: set[int]) -> tuple[int, int]:
    """Insert in batches; returns (new users, users that were there already)."""
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    added = known = 0
    try:
        items = sorted(users.items())
        for start in range(0, len(items), BATCH):
            batch = items[start:start + BATCH]
            async with pool.acquire() as conn, conn.transaction():
                rows = await conn.fetch(
                    "INSERT INTO rm_users (tg_id, username, role, source, had_dm) "
                    "SELECT t.tg_id, t.username, 'guest', 'import:cmd', t.had_dm "
                    "FROM unnest($1::bigint[], $2::text[], $3::bool[]) AS t(tg_id, username, had_dm) "
                    "ON CONFLICT (tg_id) DO UPDATE SET username = coalesce(rm_users.username, EXCLUDED.username), "
                    "had_dm = rm_users.had_dm OR EXCLUDED.had_dm "
                    "RETURNING tg_id, (xmax = 0) AS inserted",
                    [t for t, _ in batch], [u for _, u in batch], [t in private for t, _ in batch])
                new = [int(r["tg_id"]) for r in rows if r["inserted"]]
                await conn.executemany(store._HISTORY, [(None, "user", t, None, "guest") for t in new])
                added += len(new)
                known += len(rows) - len(new)
            await asyncio.sleep(0.05)  # leave room to the running service
    finally:
        await pool.close()
    return added, known


async def import_cmd(args: argparse.Namespace) -> None:
    users, private, counts, roles = await read_source(_source_dsn(args.source_ini))
    print("read from Cash Monitor Dev:", ", ".join(f"{k} {v}" for k, v in counts.items()))
    print(f"unique users: {len(users)}, with a private dialog: {len(private)}")
    if args.dry_run:
        print("dry run: nothing is written")
        return
    added, known = await write_target(_dsn_from_env_file(args.env_file), users, private)
    print(f"rm_users: new {added}, already there {known}")
    if roles and args.roles_csv is not None:
        args.roles_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.roles_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["tg_id", "username", "role_in_cash_monitor_dev"])
            w.writerows([(r["user_id"], r["username"] or "", r["role"]) for r in roles])
        print(f"roles of Cash Monitor Dev for review ({len(roles)}): {args.roles_csv}")


async def set_user(args: argparse.Namespace) -> None:
    pool = await asyncpg.create_pool(_dsn_from_env_file(args.env_file), min_size=1, max_size=1)
    try:
        before = await store.set_user_role(pool, args.tg_id, args.role, 0)
        if before is None:
            await store.add_users(pool, {args.tg_id: args.role}, source="cli")
            print(f"user {args.tg_id}: added with role {args.role}")
        else:
            print(f"user {args.tg_id}: role {before} -> {args.role}")
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Autocash Role Manager tools")
    parser.add_argument("--env-file", type=Path, help="file with AUTOCASH_PG_DSN=... (instead of the environment)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("user", help="add a user or change the role (e.g. the first Dev)")
    u.add_argument("tg_id", type=int)
    u.add_argument("role", type=str.lower)
    i = sub.add_parser("import-cmd", help="copy the users of Cash Monitor Dev into rm_users")
    i.add_argument("--source-ini", type=Path, help="settings.ini of Cash Monitor Dev ([postgres] dsn)")
    i.add_argument("--roles-csv", type=Path, help="where to write the ADMIN/CASHER list for review")
    i.add_argument("--dry-run", action="store_true", help="read and count only")
    args = parser.parse_args()
    asyncio.run(set_user(args) if args.cmd == "user" else import_cmd(args))


if __name__ == "__main__":
    main()
