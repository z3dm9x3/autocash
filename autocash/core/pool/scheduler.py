"""Duty scheduler (FT-1.6, FT-1.7): which account polls, sends to and reads which chat.

Recomputed on every change of the pool (account added / removed / changed state, membership
changed). An assignment is kept while its account stays eligible, so a change moves only the
duties of the account that dropped out; the rest is spread evenly by current load.
"""

from __future__ import annotations

import asyncio
import time

import asyncpg

from autocash.core.config.loader import ConfigStore
from autocash.core.pool.pool import AccountPool
from autocash.core.storage.writer import BatchWriter

POLL, SEND, READ_DUTY = "poll", "send", "read"
DUTIES = (POLL, SEND, READ_DUTY)
SHARED_PTS = ("supergroup", "channel")


class Scheduler:
    def __init__(self, store: ConfigStore, pool: AccountPool, writer: BatchWriter) -> None:
        self._store = store
        self._pool = pool
        self._writer = writer
        self.assignments: dict[str, dict[int, int]] = {duty: {} for duty in DUTIES}
        self.rebalanced_at = 0.0
        self.rebalances = 0
        self._timer: asyncio.TimerHandle | None = None

    def request(self) -> None:
        """Called on every pool change; the new distribution is in force well within N seconds."""
        if self._timer is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        delay = min(0.05, self._store.current.config.pool.rebalance_s / 2)
        self._timer = loop.call_later(delay, self._fire)

    def _fire(self) -> None:
        self._timer = None
        self.rebalance()

    def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def account_for(self, duty: str, chat_id: int) -> int | None:
        return self.assignments[duty].get(chat_id)

    def chats_for(self, duty: str, account_id: int) -> list[int]:
        return [chat for chat, acc in self.assignments[duty].items() if acc == account_id]

    def rebalance(self) -> None:
        pool, rt = self._pool, self._store.current
        cfg = rt.config.poll
        active = {a.account_id: a for a in pool.active()}
        load: dict[int, int] = dict.fromkeys(active, 0)
        new: dict[str, dict[int, int]] = {duty: {} for duty in DUTIES}

        per_duty: dict[str, dict[int, int]] = {duty: dict.fromkeys(active, 0) for duty in DUTIES}

        def pick(duty: str, chat_id: int, eligible: list[int]) -> None:
            if not eligible:
                return
            mine = per_duty[duty]
            previous = self.assignments[duty].get(chat_id)
            lightest = min(eligible, key=lambda a: (mine[a], load[a], -active[a].limiter.budget_left(), a))
            # Keep the assignment unless that leaves this duty or the account's total load uneven
            # (an account came back or joined): duties spread evenly, not piled on the old holder.
            chosen = lightest
            if previous is not None and previous in eligible and (
                    (mine[previous], load[previous]) <= (mine[lightest], load[lightest])):
                chosen = previous
            new[duty][chat_id] = chosen
            mine[chosen] += 1
            load[chosen] += 1

        chats = sorted(pool.membership.chats)
        # Sticky assignments first, so their load is known before free chats are spread.
        for sticky in (True, False):
            for chat_id in chats:
                members = pool.membership.members(chat_id)
                readers = [a for a in members if a in active]
                senders = [a for a, can_send in members.items() if can_send and a in active]
                for duty, eligible in ((SEND, senders), (READ_DUTY, readers)):
                    if duty == SEND and chat_id > 0:
                        continue  # a private dialog is answered only by its own account (FT-9.4)
                    if chat_id in new[duty]:
                        continue
                    if (self.assignments[duty].get(chat_id) in eligible) == sticky:
                        pick(duty, chat_id, eligible)

        # Safety poll: up to N chats per active account, chosen by gap, silence and priority (FT-2.5).
        now = time.monotonic()

        def rank(chat_id: int) -> tuple[int, int, int, int]:
            silent = now - pool.last_event.get(chat_id, 0.0) > cfg.silence_norm_s
            return (0 if chat_id in pool.gaps else 1, 0 if silent else 1, rt.source(chat_id).cfg.priority, chat_id)

        candidates = sorted(
            (c for c in chats if pool.membership.kinds.get(c) in SHARED_PTS and rt.source(c).policy == "full"),
            key=rank,
        )
        polls: dict[int, int] = dict.fromkeys(active, 0)
        for chat_id in candidates:
            eligible = [a for a in pool.membership.members(chat_id)
                        if a in active and polls[a] < cfg.chats_per_account]
            if not eligible:
                continue
            previous = self.assignments[POLL].get(chat_id)
            lightest = min(eligible, key=lambda a: (polls[a], load[a], a))
            chosen = previous if previous in eligible and polls[previous] <= polls[lightest] else lightest
            new[POLL][chat_id] = chosen
            polls[chosen] += 1

        changed = new != self.assignments
        self.assignments = new
        self.rebalanced_at = time.monotonic()
        self.rebalances += 1
        if changed:
            self._store_assignments()

    def _store_assignments(self) -> None:
        rows = [(duty, chat, acc) for duty, chats in self.assignments.items() for chat, acc in chats.items()]

        async def write(conn: asyncpg.Connection) -> None:
            await conn.execute("DELETE FROM duty_assignments")
            if rows:
                await conn.executemany(
                    "INSERT INTO duty_assignments (duty, chat_id, account_id) VALUES ($1, $2, $3)", rows)

        self._writer.submit(write)

    def summary(self) -> dict[int, dict[str, int]]:
        """Duties per account; shown by /accounts in stage 1c."""
        out: dict[int, dict[str, int]] = {a.account_id: dict.fromkeys(DUTIES, 0) for a in self._pool.active()}
        for duty, chats in self.assignments.items():
            for account_id in chats.values():
                out.setdefault(account_id, dict.fromkeys(DUTIES, 0))[duty] += 1
        return out
