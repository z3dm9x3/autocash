"""Loading the history of chats (FT-2.8, stage 4).

History goes the same way as live updates (Gateway -> SQLite -> ...), marked source=backfill:
the sorter puts it in the lowest lane, the queue lets it through while the live lane is short
and keeps the order per key (FT-7.3, 7.5), and the core drops external actions of modules for
it (FT-8.8). Requests go through the limiter of the chat's reader with the backfill priority,
i.e. from what the live work leaves (FT-10.2). The progress of every chat is stored after each
batch is accepted: a restart or a FloodWait continues from there; a repeated batch is a
duplicate and changes nothing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any

from autocash.core.config.loader import ConfigStore
from autocash.core.pool.limiter import BACKFILL
from autocash.core.pool.pool import AccountPool
from autocash.core.pool.scheduler import READ_DUTY, Scheduler
from autocash.core.pool.state import AccountState
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.telegram.api import FloodWait, TelegramError

log = logging.getLogger(__name__)


class Backfill:
    def __init__(self, store: ConfigStore, db: Database, pool: AccountPool, scheduler: Scheduler,
                 backlog: Callable[[], Awaitable[int]] | None = None) -> None:
        self._store = store
        self._backlog = backlog  # events still in the pipeline
        self._db = db
        self._pool = pool
        self._scheduler = scheduler
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._next_at: dict[int, float] = {}  # chat -> monotonic time of its next request
        self.batches = 0
        self.flood_waits = 0
        self.throttled = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="backfill")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    # ---- control (commands /backfill) ------------------------------------------------------------
    async def begin(self, chats: list[int] | None, days: float | None = None) -> int:
        """Start (or restart) loading of the given chats; None: every source with policy full."""
        cfg = self._store.current.config.backfill
        since = datetime.now(UTC) - timedelta(days=days if days is not None else cfg.days)
        if chats is None:
            rt = self._store.current
            chats = [c for c, src in rt.sources.items() if src.policy == "full"]
        rows = await self._db.pool.fetch(
            "INSERT INTO backfill_jobs (chat_id, status, since) SELECT c, 'running', $2 FROM unnest($1::bigint[]) c "
            # A restart goes on from the stored offset: what is loaded is not loaded again.
            "ON CONFLICT (chat_id) DO UPDATE SET status = 'running', "
            "since = least(backfill_jobs.since, EXCLUDED.since), done_at = NULL, last_error = NULL, "
            "updated_at = now() RETURNING chat_id", chats, since)
        self._wake.set()
        return len(rows)

    async def pause(self) -> int:
        status: str = await self._db.pool.execute(
            "UPDATE backfill_jobs SET status = 'paused', updated_at = now() WHERE status = 'running'")
        return int(status.split()[-1])

    async def resume(self) -> int:
        status: str = await self._db.pool.execute(
            "UPDATE backfill_jobs SET status = 'running', updated_at = now() WHERE status = 'paused'")
        self._wake.set()
        return int(status.split()[-1])

    async def progress(self) -> list[Any]:
        rows: list[Any] = await self._db.pool.fetch(
            "SELECT chat_id, status, loaded, since, last_error FROM backfill_jobs ORDER BY chat_id")
        return rows

    # ---- loading ---------------------------------------------------------------------------------------
    async def _run(self) -> None:
        while True:
            try:
                worked = await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not is_connection_error(exc):
                    log.exception("backfill failed")
                worked = False
            if not worked:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), 1.0)
                except TimeoutError:
                    pass

    def _reader(self, chat_id: int) -> Any:
        preferred = self._scheduler.account_for(READ_DUTY, chat_id)
        members = [a for a in self._pool.membership.members(chat_id) if self._pool.state(a) is AccountState.ACTIVE]
        account = preferred if preferred in members else (members[0] if members else None)
        return self._pool.accounts.get(account) if account is not None else None

    async def _tick(self) -> bool:
        if not self._pool.api_ready.is_set():
            return False
        cfg = self._store.current.config.backfill
        backlog = await self._backlog() if self._backlog is not None else 0
        if backlog > cfg.max_backlog:
            self.throttled += 1
            return False  # the pipeline is busy: the history waits for the live flow
        # Only as many batches at once as fit into what is left below the threshold.
        fits = max(1, (cfg.max_backlog - backlog) // cfg.batch)
        now = time.monotonic()
        jobs = await self._db.pool.fetch("SELECT * FROM backfill_jobs WHERE status = 'running' ORDER BY updated_at")
        due = [j for j in jobs if self._next_at.get(j["chat_id"], 0.0) <= now][: min(cfg.chats_at_once, fits)]
        if not due:
            return False
        await asyncio.gather(*(self._batch(j) for j in due))
        return True

    async def _batch(self, job: Any) -> None:
        cfg = self._store.current.config.backfill
        chat_id = job["chat_id"]
        self._next_at[chat_id] = time.monotonic() + cfg.interval_s
        acc = self._reader(chat_id)
        if acc is None:
            return  # no active reader in the chat now: it waits (coverage alarms are elsewhere)
        try:
            events = await acc.limiter.call(BACKFILL, "getHistory", partial(
                acc.client.get_history, chat_id, job["offset_id"], job["since"], cfg.batch))
        except FloodWait as exc:
            self.flood_waits += 1
            self._next_at[chat_id] = time.monotonic() + exc.seconds
            return
        except TelegramError as exc:
            self._pool.apply_error(acc, exc.code, chat_id)
            await self._db.pool.execute("UPDATE backfill_jobs SET last_error = $2, updated_at = now() "
                                        "WHERE chat_id = $1", chat_id, exc.code)
            return
        # Forward in time, oldest first: the stages of one key come in the order they happened.
        fresh = sorted(events, key=lambda e: e.msg_id)
        await self._pool.deliver(acc, fresh)
        self.batches += 1
        finished = len(events) < cfg.batch  # reached the newest message; the rest comes live
        offset = max((e.msg_id for e in events), default=job["offset_id"])
        await self._db.pool.execute(
            "UPDATE backfill_jobs SET offset_id = $2, loaded = loaded + $3, updated_at = now(), "
            "status = CASE WHEN $4 THEN 'done' ELSE status END, done_at = CASE WHEN $4 THEN now() END "
            "WHERE chat_id = $1 AND status = 'running'", chat_id, offset, len(fresh), finished)
