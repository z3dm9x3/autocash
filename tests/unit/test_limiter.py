"""Account limiter: concurrency, tempo, priority classes, FloodWait pauses (FT-10.1..10.3)."""

from __future__ import annotations

import asyncio
import itertools
import time

import pytest

from autocash.core.pool.limiter import BACKFILL, CLIENT_REPLY, COMMAND_REPLY, NOTIFY, READ, AccountLimiter
from autocash.core.telegram.api import FloodWait


async def test_calls_of_one_account_never_overlap_beyond_the_concurrency() -> None:
    limiter = AccountLimiter(concurrency=2, rate_per_s=0)
    running = peak = 0

    async def call() -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1

    await asyncio.gather(*(limiter.call(READ, "m", call) for _ in range(8)))
    assert peak == 2 and limiter.calls == 8 and limiter.load == 0


async def test_waiters_are_served_by_budget_class() -> None:
    limiter = AccountLimiter(concurrency=1, rate_per_s=0)
    order: list[str] = []
    release = asyncio.Event()

    async def blocker() -> None:
        await release.wait()

    async def mark(name: str) -> None:
        order.append(name)

    first = asyncio.create_task(limiter.call(READ, "m", blocker))
    await asyncio.sleep(0)
    names = {BACKFILL: "backfill", NOTIFY: "notify", READ: "read", CLIENT_REPLY: "client", COMMAND_REPLY: "command"}
    tasks = [asyncio.create_task(limiter.call(prio, "m", lambda n=name: mark(n)))  # type: ignore[misc]
             for prio, name in names.items()]
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, *tasks)
    assert order == ["command", "client", "read", "notify", "backfill"]


async def test_tempo_spaces_calls_and_backfill_is_slower() -> None:
    limiter = AccountLimiter(concurrency=1, rate_per_s=20, backfill_share=0.5)
    stamps: list[float] = []

    async def call() -> None:
        stamps.append(time.perf_counter())

    # The loop clock on Windows ticks every ~16 ms, hence the tolerance.
    await asyncio.gather(*(limiter.call(READ, "m", call) for _ in range(5)))
    gaps = [b - a for a, b in itertools.pairwise(stamps)]
    assert all(gap >= 0.03 for gap in gaps), gaps  # 20 per second -> about 50 ms apart
    assert stamps[-1] - stamps[0] >= 0.18

    stamps.clear()
    await asyncio.gather(*(limiter.call(BACKFILL, "m", call) for _ in range(4)))
    assert stamps[-1] - stamps[0] >= 0.28  # backfill takes what is left: half the tempo, ~100 ms apart


async def test_flood_wait_pauses_the_method_slows_down_and_reports() -> None:
    reported: list[tuple[str, float]] = []
    limiter = AccountLimiter(concurrency=1, rate_per_s=0, on_pause=lambda m, until: reported.append((m, until)))

    async def flood() -> None:
        raise FloodWait(0.3)

    async def ok() -> str:
        return "ok"

    began = time.time()
    with pytest.raises(FloodWait):
        await limiter.call(READ, "getChannelDifference", flood)
    assert limiter.paused_until("getChannelDifference") is not None and limiter.paused_until("send") is None
    assert limiter.paused_until() is not None and limiter.budget_left() == 0.0
    assert reported and reported[0][0] == "getChannelDifference" and limiter.floods == 1

    # Another method is not held back; the paused one waits for the end of the pause.
    assert await limiter.call(NOTIFY, "send", ok) == "ok"
    assert time.time() - began < 0.25
    assert await limiter.call(READ, "getChannelDifference", ok) == "ok"
    assert time.time() - began >= 0.3
    assert limiter.paused_until() is None and 0 < limiter.budget_left() < 1.0  # the tempo stays lowered


async def test_account_wide_pause_and_restore_after_restart() -> None:
    limiter = AccountLimiter(concurrency=1, rate_per_s=0)
    limiter.restore("", time.time() + 0.2)
    limiter.restore("old", time.time() - 5)  # already over: ignored
    assert limiter.paused_until("anything") is not None and "old" not in limiter.pauses and limiter.floods == 0

    async def ok() -> int:
        return 1

    began = time.time()
    assert await limiter.call(COMMAND_REPLY, "send", ok) == 1
    assert time.time() - began >= 0.19


async def test_cancelled_waiter_frees_its_place() -> None:
    limiter = AccountLimiter(concurrency=1, rate_per_s=0)
    release = asyncio.Event()

    async def blocker() -> None:
        await release.wait()

    async def ok() -> str:
        return "ok"

    first = asyncio.create_task(limiter.call(READ, "m", blocker))
    await asyncio.sleep(0)
    doomed = asyncio.create_task(limiter.call(READ, "m", ok))
    await asyncio.sleep(0)
    doomed.cancel()
    await asyncio.gather(doomed, return_exceptions=True)
    release.set()
    await first
    assert await asyncio.wait_for(limiter.call(READ, "m", ok), 1) == "ok" and limiter.load == 0
