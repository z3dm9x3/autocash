"""QK: one-time import of the QuickKast catalog of Cash Monitor Dev (QK_readme.md, D-QK-6).

Reads the monitor's PostgreSQL (tables quickkasts, quickkast_profiles; read-only) and adds its
profiles and templates to the Autocash catalog (qk_profiles, qk_templates). QuickKast in Autocash is
one catalog for every account, so the per-account catalogs of the monitor are merged; an equal
template of a trigger is not added twice, so the import may be repeated. The running Autocash reads
the catalog at start: restart it after the import (or the change is seen at the next start).

    python -m autocash.modules.quickkast.QK_import [--source-ini PATH] [--dry-run]

DSNs: the monitor's from `[postgres] dsn` of its settings.ini (or AUTOCASH_HISTORY_DSN); Autocash's from
AUTOCASH_PG_DSN (or `Documents/AutoCash Secrets/pg.env`). Neither is printed.
"""

from __future__ import annotations

import argparse
import asyncio
import configparser
import os
from pathlib import Path

import asyncpg

DEFAULT_INI = Path.home() / "Documents" / "ChatGPT" / "CASH MONITOR DEV" / "settings.ini"
SECRETS = Path.home() / "Documents" / "AutoCash Secrets" / "pg.env"

# Append a variant unless the same template of the trigger is there already (a repeated import adds nothing).
_ADD = """
INSERT INTO qk_templates (profile, trigger, variant, template)
SELECT $1, $2, coalesce(max(variant), 0) + 1, $3 FROM qk_templates WHERE profile = $1 AND trigger = $2
HAVING NOT EXISTS (SELECT 1 FROM qk_templates WHERE profile = $1 AND trigger = $2 AND template = $3)
RETURNING variant
"""


def source_dsn(ini: Path) -> str:
    env = os.environ.get("AUTOCASH_HISTORY_DSN")
    if env:
        return env
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(ini, encoding="utf-8-sig")
    if not parser.has_option("postgres", "dsn"):
        raise SystemExit(f"no [postgres] dsn in {ini}")
    return parser.get("postgres", "dsn").strip()


def target_dsn() -> str:
    env = os.environ.get("AUTOCASH_PG_DSN")
    if env:
        return env
    for line in SECRETS.read_text(encoding="utf-8").splitlines():
        if line.startswith("AUTOCASH_PG_DSN="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("AUTOCASH_PG_DSN is not set")


async def run(ini: Path, dry_run: bool) -> None:
    src = await asyncpg.connect(source_dsn(ini))
    try:
        await src.execute("SET default_transaction_read_only = on")
        profiles = [r["name"] for r in await src.fetch("SELECT DISTINCT name FROM quickkast_profiles ORDER BY name")]
        rows = await src.fetch("SELECT profile, trigger, template FROM quickkasts "
                               "ORDER BY telegram_account_id, profile, trigger, variant")
    finally:
        await src.close()
    dst = await asyncpg.connect(target_dsn())
    added = 0
    try:
        async with dst.transaction():
            names = {str(r["name"]).casefold(): str(r["name"]) for r in await dst.fetch("SELECT name FROM qk_profiles")}
            for name in [*profiles, *(r["profile"] for r in rows)]:
                if name.casefold() not in names:
                    await dst.execute("INSERT INTO qk_profiles (name) VALUES ($1)", name)
                    names[name.casefold()] = name
            for r in rows:
                profile = names[str(r["profile"]).casefold()]
                done = await dst.fetchval(_ADD, profile, r["trigger"], r["template"])
                added += done is not None
            if dry_run:
                raise _DryRun()
    except _DryRun:
        print(f"dry run: {added} of {len(rows)} templates would be added")
        return
    finally:
        await dst.close()
    listed = ", ".join(sorted(names.values()))
    print(f"QK import: {len(rows)} templates in the monitor, {added} added; profiles: {listed}")


class _DryRun(Exception):
    pass


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    ap.add_argument("--source-ini", type=Path, default=DEFAULT_INI)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    asyncio.run(run(args.source_ini, args.dry_run))


if __name__ == "__main__":
    main()
