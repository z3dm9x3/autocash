"""Per-account limiter of active Telegram API calls (FT-10.1..10.3).

Push updates cost nothing; every active call (send, poll, catch-up, dialogs, history) of one
account passes here: bounded concurrency, a fixed tempo, five priority classes, and pauses on
FloodWait. The library never sleeps a FloodWait away (`flood_sleep_threshold=0`): the pause is
set here, per method, and reported to the pool, which stores it and moves the duties.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from autocash.core.telegram.api import FloodWait

T = TypeVar("T")

# Budget classes (FT-10.2), the lower the earlier.
COMMAND_REPLY, CLIENT_REPLY, READ, NOTIFY, BACKFILL = range(5)

PauseHook = Callable[[str, float], None]  # method, wall-clock time the pause ends


class AccountLimiter:
    def __init__(
        self,
        *,
        concurrency: int = 1,
        rate_per_s: float = 1.0,
        backfill_share: float = 0.5,
        flood_slowdown: float = 2.0,
        flood_recovery_s: float = 600.0,
        on_pause: PauseHook | None = None,
    ) -> None:
        self.concurrency = concurrency
        self.rate_per_s = rate_per_s
        self.backfill_share = backfill_share
        self.flood_slowdown = flood_slowdown
        self.flood_recovery_s = flood_recovery_s
        self.on_pause = on_pause
        self.pauses: dict[str, float] = {}  # method -> wall-clock end; "" pauses the whole account
        self.calls = 0
        self.floods = 0
        self._waiters: list[tuple[int, int, str, asyncio.Future[None]]] = []
        self._seq = itertools.count()
        self._running = 0
        self._next_at = 0.0
        self._slow = 1.0
        self._slow_at = 0.0
        self._timer: asyncio.TimerHandle | None = None

    # ---- pauses --------------------------------------------------------------------------
    def paused_until(self, method: str | None = None) -> float | None:
        """Wall-clock end of the pause for `method` (or of any pause when None), if one is active."""
        now = time.time()
        for key in [k for k, until in self.pauses.items() if until <= now]:
            del self.pauses[key]
        if method is None:
            return max(self.pauses.values(), default=None)
        ends = [self.pauses[k] for k in ("", method) if k in self.pauses]
        return max(ends, default=None)

    def pause(self, method: str, seconds: float) -> None:
        """A FloodWait was received: pause the method and slow the tempo down."""
        until = time.time() + max(seconds, 0.0)
        self.pauses[method] = max(until, self.pauses.get(method, 0.0))
        self.floods += 1
        self._slow = min(self._slow * self.flood_slowdown, 16.0)
        self._slow_at = time.monotonic()
        if self.on_pause is not None:
            self.on_pause(method, self.pauses[method])
        self._kick()

    def restore(self, method: str, until: float) -> None:
        """A pause read from the database after a restart (FT-10.3)."""
        if until > time.time():
            self.pauses[method] = max(until, self.pauses.get(method, 0.0))

    # ---- budget --------------------------------------------------------------------------
    @property
    def load(self) -> int:
        return self._running + len(self._waiters)

    def budget_left(self) -> float:
        """0..1: share of the tempo that is not taken; used to spread duties evenly (FT-1.6)."""
        if self.paused_until() is not None:
            return 0.0
        return 1.0 / (self._slowdown() * (1 + self.load))

    def _slowdown(self) -> float:
        if self._slow > 1.0 and time.monotonic() - self._slow_at > self.flood_recovery_s:
            self._slow = 1.0
        return self._slow

    def _interval(self, priority: int) -> float:
        if self.rate_per_s <= 0:
            return 0.0
        interval = self._slowdown() / self.rate_per_s
        return interval / self.backfill_share if priority >= BACKFILL else interval

    # ---- calls ---------------------------------------------------------------------------
    async def call(self, priority: int, method: str, fn: Callable[[], Awaitable[T]]) -> T:
        grant: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (priority, next(self._seq), method, grant))
        self._kick()
        try:
            await grant
        except asyncio.CancelledError:
            if grant.done() and not grant.cancelled():
                self._running -= 1  # the slot was granted in the same tick as the cancellation
                self._kick()
            raise
        try:
            self.calls += 1
            return await fn()
        except FloodWait as exc:
            self.pause(method, exc.seconds)
            raise
        finally:
            self._running -= 1
            self._kick()

    def _kick(self) -> None:
        """Grant slots to waiters in priority order; never blocks."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        loop = asyncio.get_event_loop()
        wake: float | None = None
        while self._running < self.concurrency and self._waiters:
            now = loop.time()
            if now < self._next_at:
                wake = self._next_at - now
                break
            wall = time.time()
            skipped: list[tuple[int, int, str, asyncio.Future[None]]] = []
            granted = False
            while self._waiters:
                item = heapq.heappop(self._waiters)
                priority, _, method, grant = item
                if grant.done():
                    continue  # cancelled while waiting
                until = self.paused_until(method)
                if until is not None:
                    skipped.append(item)
                    wake = min(wake, until - wall) if wake is not None else until - wall
                    continue
                self._running += 1
                self._next_at = now + self._interval(priority)
                grant.set_result(None)
                granted = True
                break
            for item in skipped:
                heapq.heappush(self._waiters, item)
            if not granted:
                break
        if wake is not None and self._waiters:
            self._timer = loop.call_later(max(wake, 0.0) + 0.001, self._kick)
