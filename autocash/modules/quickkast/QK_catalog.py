"""QK: the catalog of QuickKast templates, kept in memory (QK_readme.md, D-QK-4).

The express hook resolves a trigger without any I/O: the whole catalog (switch, active profile,
profiles, templates, chats switched off) lives here. PostgreSQL is the store: it is read at start,
and every change of a command is written there first, then applied here.

Ported from Cash Monitor Dev (`rucash_monitor/modules/quickkast`): the same triggers and variants.
- `TRIGGER`: a random variant; `TRIGGER-N`: variant N; `TRIGGER-list`: every variant.
- The trigger is the whole message, exact (spaces around are ignored); the profile name ignores case.
"""

from __future__ import annotations

import random
import re
from dataclasses import dataclass, field

import asyncpg

STANDARD = "standard"
RESERVED_TRIGGERS = frozenset({"x", "х", "v"})  # x / х (Cyrillic): QK off in the chat; v: on
RESERVED_PROFILE = "profile"
VARIANT_SUFFIX = re.compile(r"^(.+)-(list|[1-9][0-9]*)$", re.I | re.S)


_SHARED: dict[int, tuple[object, Catalog]] = {}


def shared(pool: object) -> Catalog:
    """The one catalog of a database (D-QK2-3): QuickKast and QuickCast2 resolve the same templates, and a
    change of `/qk` is seen by both at once. Keyed by the pool, so every App (and test) has its own."""
    entry = _SHARED.get(id(pool))
    if entry is None or entry[0] is not pool:
        entry = _SHARED[id(pool)] = (pool, Catalog())
    return entry[1]


@dataclass(frozen=True, slots=True)
class Resolved:
    kind: str  # template / list / missing_variant
    trigger: str
    template: str | None = None
    variant: int | None = None
    count: int = 0
    rows: tuple[tuple[int, str], ...] = ()
    requested: int | None = None


@dataclass(slots=True)
class Catalog:
    enabled: bool = True
    active: str = STANDARD
    profiles: dict[str, str] = field(default_factory=lambda: {STANDARD: STANDARD})  # casefold -> name
    templates: dict[tuple[str, str], list[tuple[int, str]]] = field(default_factory=dict)  # (profile cf, trigger)
    chats_off: set[tuple[int, int]] = field(default_factory=set)  # (account, chat)
    loaded: bool = False

    # ---- reads (the express path: memory only) -----------------------------------------------------
    def variants(self, profile: str, trigger: str) -> list[tuple[int, str]]:
        return self.templates.get((profile.casefold(), trigger), [])

    def chat_enabled(self, account_id: int, chat_id: int) -> bool:
        return (account_id, chat_id) not in self.chats_off

    def find_profile(self, name: str) -> str | None:
        return self.profiles.get(name.strip().casefold())

    def resolve(self, entered: str, profile: str | None = None) -> Resolved | None:
        profile = profile or self.active
        suffix = VARIANT_SUFFIX.fullmatch(entered)
        if suffix:
            trigger, selector = suffix.groups()
            rows = tuple(self.variants(profile, trigger))
            if rows:
                if selector.casefold() == "list":
                    return Resolved("list", trigger, rows=rows, count=len(rows))
                requested = int(selector)
                if requested > len(rows):
                    return Resolved("missing_variant", trigger, count=len(rows), requested=requested)
                variant, template = rows[requested - 1]
                return Resolved("template", trigger, template, variant, len(rows))
        rows = tuple(self.variants(profile, entered))
        if not rows:
            return None
        variant, template = random.choice(rows)
        return Resolved("template", entered, template, variant, len(rows))

    def listing(self, profile: str) -> list[tuple[str, int, str]]:
        cf = profile.casefold()
        return sorted(((t, v, text) for (p, t), rows in self.templates.items() if p == cf for v, text in rows),
                      key=lambda r: (r[0].casefold(), r[1]))

    # ---- the store ---------------------------------------------------------------------------------
    async def load(self, pool: asyncpg.Pool) -> None:
        settings = await pool.fetchrow("SELECT enabled, active_profile FROM qk_settings WHERE id = 1")
        profiles = {str(r["name"]).casefold(): str(r["name"]) for r in await pool.fetch("SELECT name FROM qk_profiles")}
        templates: dict[tuple[str, str], list[tuple[int, str]]] = {}
        for r in await pool.fetch("SELECT profile, trigger, variant, template FROM qk_templates "
                                  "ORDER BY profile, trigger, variant"):
            templates.setdefault((str(r["profile"]).casefold(), str(r["trigger"])), []).append(
                (int(r["variant"]), str(r["template"])))
        off = {(int(r["account_id"]), int(r["chat_id"]))
               for r in await pool.fetch("SELECT account_id, chat_id FROM qk_chat_state WHERE NOT enabled")}
        profiles.setdefault(STANDARD, STANDARD)
        self.profiles, self.templates, self.chats_off = profiles, templates, off
        self.enabled = bool(settings["enabled"]) if settings is not None else True
        active = str(settings["active_profile"]) if settings is not None else STANDARD
        self.active = profiles.get(active.casefold(), STANDARD)
        self.loaded = True

    async def set_enabled(self, pool: asyncpg.Pool, enabled: bool, by: int | None) -> None:
        await pool.execute("UPDATE qk_settings SET enabled = $1, updated_by = $2, updated_at = now() WHERE id = 1",
                           enabled, by)
        self.enabled = enabled

    async def set_active(self, pool: asyncpg.Pool, profile: str, by: int | None) -> None:
        await pool.execute("UPDATE qk_settings SET active_profile = $1, updated_by = $2, updated_at = now() "
                           "WHERE id = 1", profile, by)
        self.active = profile

    async def create_profile(self, pool: asyncpg.Pool, name: str) -> bool:
        if name.casefold() in self.profiles:
            return False
        await pool.execute("INSERT INTO qk_profiles (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
        self.profiles[name.casefold()] = name
        return True

    async def add_variant(self, pool: asyncpg.Pool, profile: str, trigger: str, template: str) -> int:
        """Append a template once; returns its variant number (an existing equal template keeps its number)."""
        rows = self.templates.setdefault((profile.casefold(), trigger), [])
        for variant, text in rows:
            if text == template:
                return variant
        variant = int(await pool.fetchval(
            "INSERT INTO qk_templates (profile, trigger, variant, template) "
            "SELECT $1, $2, coalesce(max(variant), 0) + 1, $3 FROM qk_templates WHERE profile = $1 AND trigger = $2 "
            "RETURNING variant", self.profiles[profile.casefold()], trigger, template))
        rows.append((variant, template))
        rows.sort()
        return variant

    async def delete(self, pool: asyncpg.Pool, profile: str, trigger: str, variant: int | None = None) -> bool:
        """Delete a trigger, or one variant of it; the variants after it move up (as in Cash Monitor Dev)."""
        key = (profile.casefold(), trigger)
        rows = self.templates.get(key, [])
        name = self.profiles[profile.casefold()]
        if variant is None:
            if not rows:
                return False
            await pool.execute("DELETE FROM qk_templates WHERE profile = $1 AND trigger = $2", name, trigger)
            self.templates.pop(key, None)
            return True
        if variant < 1 or variant > len(rows):
            return False
        stored = rows[variant - 1][0]
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("DELETE FROM qk_templates WHERE profile = $1 AND trigger = $2 AND variant = $3",
                               name, trigger, stored)
            # Renumber: variant N is always the N-th of the list a user sees.
            await conn.execute(
                "UPDATE qk_templates t SET variant = -n.rn FROM (SELECT variant, "
                "row_number() OVER (ORDER BY variant) rn "
                "FROM qk_templates WHERE profile = $1 AND trigger = $2) n "
                "WHERE t.profile = $1 AND t.trigger = $2 AND t.variant = n.variant", name, trigger)
            await conn.execute("UPDATE qk_templates SET variant = -variant WHERE profile = $1 AND trigger = $2",
                               name, trigger)
        rest = [text for v, text in rows if v != stored]
        if rest:
            self.templates[key] = [(i + 1, text) for i, text in enumerate(rest)]
        else:
            self.templates.pop(key, None)
        return True

    def set_chat(self, account_id: int, chat_id: int, enabled: bool) -> None:
        """Memory first (the express path reads it at once); the caller writes the store."""
        if enabled:
            self.chats_off.discard((account_id, chat_id))
        else:
            self.chats_off.add((account_id, chat_id))


async def store_chat(pool: asyncpg.Pool, account_id: int, chat_id: int, enabled: bool) -> None:
    await pool.execute(
        "INSERT INTO qk_chat_state (account_id, chat_id, enabled) VALUES ($1, $2, $3) "
        "ON CONFLICT (account_id, chat_id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()",
        account_id, chat_id, enabled)
