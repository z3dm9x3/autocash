"""OBX fuse: a FloodWait halves the limits of the account, once (D-OBX-7).

Any FloodWait on an account, whatever the method: both limits of that account (writes and reads
per minute) are halved, intervals doubled, and the group `logs` is told. Later FloodWaits do not
halve again: the account is paused for the time Telegram asked (the limiter of the pool). The
lowered limits are stored in `obx_account_limits`: they survive a restart and go back to normal
only by hand (`/obx reset <account | all>`). Limits are per account.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from autocash.core.storage.pg import Database

log = logging.getLogger(__name__)

TRIPPED = 0.5


class Fuse:
    def __init__(self, db: Database, notify: Callable[[str], None]) -> None:
        self._db = db
        self._notify = notify
        self.factors: dict[int, float] = {}  # account -> 1.0 or 0.5
        self._bg: set[asyncio.Task[None]] = set()

    def factor(self, account_id: int) -> float:
        return self.factors.get(account_id, 1.0)

    async def load(self) -> None:
        rows = await self._db.pool.fetch("SELECT account_id, factor FROM obx_account_limits")
        self.factors = {int(r["account_id"]): float(r["factor"]) for r in rows}
        tripped = [a for a, f in self.factors.items() if f < 1.0]
        if tripped:
            log.warning("OBX fuse: lowered limits stay for accounts %s (reset by hand: /obx reset)", tripped)

    def trip(self, account_id: int, method: str, seconds: float) -> None:
        """A FloodWait: halve the limits of the account if they are not halved yet."""
        log.warning("FloodWait %.0f s on %s of account %s", seconds, method, account_id)
        if self.factor(account_id) < 1.0:
            return  # tripped already: once only
        self.factors[account_id] = TRIPPED
        self._notify(f"Outbox: FloodWait {seconds:.0f} с на {method}, аккаунт {account_id}. "
                     f"Лимиты аккаунта снижены вдвое до ручного сброса (/obx reset {account_id}).")
        task = asyncio.get_running_loop().create_task(self._store(account_id, method, seconds))
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _store(self, account_id: int, method: str, seconds: float) -> None:
        try:
            await self._db.pool.execute(
                "INSERT INTO obx_account_limits (account_id, factor, tripped_at, tripped_method, tripped_wait_s) "
                "VALUES ($1, $2, now(), $3, $4) ON CONFLICT (account_id) DO UPDATE SET factor = EXCLUDED.factor, "
                "tripped_at = now(), tripped_method = EXCLUDED.tripped_method, "
                "tripped_wait_s = EXCLUDED.tripped_wait_s",
                account_id, TRIPPED, method, seconds)
        except Exception:
            log.exception("OBX fuse: the lowered limits of %s are not stored (they hold until a restart)", account_id)

    async def reset(self, account_id: int | None, author: int) -> list[int]:
        """Back to the full limits: one account, or every account (None). Returns the accounts reset."""
        accounts = [a for a, f in self.factors.items() if f < 1.0 and (account_id is None or a == account_id)]
        if accounts:
            await self._db.pool.execute(
                "UPDATE obx_account_limits SET factor = 1, reset_at = now(), reset_by = $2 "
                "WHERE account_id = ANY($1::bigint[])", accounts, author)
            for a in accounts:
                self.factors[a] = 1.0
        return accounts
