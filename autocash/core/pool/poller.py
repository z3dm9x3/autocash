"""Safety poll and catch-up through getChannelDifference (FT-2.5..2.7).

Telegram may deliver fewer push updates for some supergroups, so every active account keeps a
short poll of the chats the scheduler gave it, at the interval the server names. Chats with a
gap (after a restart, a reconnect or a hole in pts) are caught up first, by the account that
holds their "read" duty. Every call goes through the account's limiter.
"""

from __future__ import annotations

import asyncio
import logging
import time
from functools import partial
from typing import Any

from autocash.core.config.loader import ConfigStore
from autocash.core.pool.limiter import READ
from autocash.core.pool.pool import Account, AccountPool
from autocash.core.pool.scheduler import POLL, READ_DUTY, Scheduler
from autocash.core.telegram.api import DifferenceTooLong, FloodWait, TelegramError

log = logging.getLogger(__name__)


class Poller:
    def __init__(self, store: ConfigStore, pool: AccountPool, scheduler: Scheduler) -> None:
        self._store = store
        self._pool = pool
        self._scheduler = scheduler
        self._task: asyncio.Task[None] | None = None
        self._busy: dict[int, asyncio.Task[Any]] = {}  # account -> the call in flight
        self._in_flight: set[int] = set()  # a chat is fetched by one account at a time
        self._next_poll: dict[int, float] = {}
        self.polls = 0
        self.catchups = 0
        self.tick_s = 0.05

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="poller")

    async def stop(self) -> None:
        tasks = [t for t in (self._task, *self._busy.values()) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None
        self._busy.clear()

    @property
    def busy(self) -> bool:
        """True while a catch-up is running or waiting; the safety poll alone does not count."""
        pool = self._pool
        return any(c in pool.pts and self._reader(c) is not None for c in pool.gaps)

    async def _run(self) -> None:
        await self._pool.api_ready.wait()
        while True:
            try:
                self._dispatch()
            except Exception:
                log.exception("poller: dispatch failed")
            await asyncio.sleep(self.tick_s)

    def _dispatch(self) -> None:
        now = time.monotonic()
        pool = self._pool
        for acc in pool.active():
            if acc.account_id in self._busy or not acc.dialogs_loaded:
                continue
            chat_id = self._next_chat(acc, now)
            if chat_id is None:
                continue
            task = asyncio.create_task(self._fetch(acc, chat_id), name=f"difference-{acc.account_id}")
            self._busy[acc.account_id] = task
            self._in_flight.add(chat_id)
            task.add_done_callback(partial(self._done, acc.account_id, chat_id))

    def _done(self, account_id: int, chat_id: int, _task: asyncio.Task[Any]) -> None:
        self._busy.pop(account_id, None)
        self._in_flight.discard(chat_id)

    def _next_chat(self, acc: Account, now: float) -> int | None:
        pool, sched = self._pool, self._scheduler
        if acc.limiter.paused_until("getChannelDifference") is not None:
            return None
        gaps = [c for c in pool.gaps
                if c in pool.pts and c not in self._in_flight and self._reader(c) == acc.account_id]
        if gaps:
            priority = self._store.current
            return min(gaps, key=lambda c: (priority.source(c).cfg.priority, c))
        due = [c for c in sched.chats_for(POLL, acc.account_id)
               if c in pool.pts and c not in self._in_flight and self._next_poll.get(c, 0.0) <= now]
        return min(due, key=lambda c: self._next_poll.get(c, 0.0)) if due else None

    def _reader(self, chat_id: int) -> int | None:
        """The account that catches the chat up: its read duty, or any active member."""
        duty = self._scheduler.account_for(READ_DUTY, chat_id)
        members = [a for a in self._pool.membership.members(chat_id) if self._pool.state(a).value == "active"]
        return self._pool.reader(members, duty)

    async def _fetch(self, acc: Account, chat_id: int) -> None:
        pool = self._pool
        cfg = self._store.current.config.poll
        catching_up = chat_id in pool.gaps
        while True:
            pts = pool.pts[chat_id]
            try:
                events, new_pts, final, timeout = await acc.limiter.call(
                    READ, "getChannelDifference",
                    partial(acc.client.get_channel_difference, chat_id, pts, cfg.difference_limit),
                )
            except DifferenceTooLong as exc:
                await pool.too_long(chat_id, exc.pts)
                self._next_poll[chat_id] = time.monotonic() + cfg.idle_interval_s
                return
            except FloodWait:
                return  # the limiter paused the method; the pool moved the duties
            except TelegramError as exc:
                if not pool.apply_error(acc, exc.code, chat_id):
                    log.error("account %s: difference of %s failed: %s", acc.account_id, chat_id, exc.code)
                self._next_poll[chat_id] = time.monotonic() + cfg.idle_interval_s
                return
            except (OSError, TimeoutError) as exc:
                log.warning("account %s: difference of %s failed: %r", acc.account_id, chat_id, exc)
                self._next_poll[chat_id] = time.monotonic() + cfg.idle_interval_s
                return
            self.polls += 1
            await pool.deliver(acc, events)  # committed to SQLite before the counter moves
            if final:
                pool.caught_up(chat_id, new_pts)
                if catching_up:
                    self.catchups += 1
                self._next_poll[chat_id] = time.monotonic() + (timeout if timeout > 0 else cfg.idle_interval_s)
                return
            pool._set_pts(chat_id, new_pts)
            if acc.state.value != "active":
                return
