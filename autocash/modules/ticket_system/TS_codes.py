"""TS: the base of withdrawal codes - a STUB (D-TS-5).

A code a player sends makes the ticket WW only if it is a real withdrawal code: one of the `[ВЫВОД]` cards
of the WW groups. That base does not exist yet, so every lookup answers "not found" and C-ticket goes on
with the triggers. When the base exists, replace `find` (one indexed query by the exact code, case matters);
the callers do not change.
"""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg


async def find(conn: asyncpg.Connection, codes: Sequence[str]) -> str | None:
    """The first of `codes` that is a known withdrawal code, or None. STUB: there is no base yet."""
    return None
