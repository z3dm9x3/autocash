"""Passwords of the web admin (stage 5, D-5-3).

    python -m autocash.cli.webuser set <telegram id>      # asks for the password twice
    python -m autocash.cli.webuser disable <telegram id>  # closes the access and its sessions
    python -m autocash.cli.webuser list

Rights are not here: they come from the configuration (`users` + `permissions`), so the admin
opens only for a user whose roles allow it. The password is never printed or logged.

Environment: AUTOCASH_PG_DSN (never printed).
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys

import asyncpg

from autocash.web.auth import hash_password

MIN_LEN = 10


async def _run(args: argparse.Namespace, dsn: str) -> int:
    conn = await asyncpg.connect(dsn)
    try:
        if args.action == "list":
            for row in await conn.fetch("SELECT tg_id, disabled, updated_at FROM web_users ORDER BY tg_id"):
                print(row["tg_id"], "отключён" if row["disabled"] else "активен", row["updated_at"])
            return 0
        if args.action == "disable":
            await conn.execute("UPDATE web_users SET disabled = true, updated_at = now() WHERE tg_id = $1", args.tg_id)
            await conn.execute("DELETE FROM web_sessions WHERE tg_id = $1", args.tg_id)
            print("Доступ закрыт, сессии удалены")
            return 0
        password = getpass.getpass("Пароль: ")
        if len(password) < MIN_LEN:
            print(f"Пароль короче {MIN_LEN} символов", file=sys.stderr)
            return 2
        if getpass.getpass("Ещё раз: ") != password:
            print("Пароли не совпадают", file=sys.stderr)
            return 2
        await conn.execute(
            "INSERT INTO web_users (tg_id, password_hash) VALUES ($1, $2) ON CONFLICT (tg_id) DO UPDATE "
            "SET password_hash = EXCLUDED.password_hash, disabled = false, updated_at = now()",
            args.tg_id, hash_password(password))
        await conn.execute("DELETE FROM web_sessions WHERE tg_id = $1", args.tg_id)
        print("Пароль задан. Права на админку дают роли пользователя в конфигурации (users, permissions).")
        return 0
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Autocash: users of the web admin")
    parser.add_argument("action", choices=["set", "disable", "list"])
    parser.add_argument("tg_id", type=int, nargs="?")
    args = parser.parse_args()
    if args.action != "list" and args.tg_id is None:
        parser.error("нужен Telegram ID")
    dsn = os.environ.get("AUTOCASH_PG_DSN")
    if not dsn:
        print("AUTOCASH_PG_DSN is not set", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(asyncio.run(_run(args, dsn)))


if __name__ == "__main__":
    main()
