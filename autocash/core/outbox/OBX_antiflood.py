"""OBX_Antifloodwaiter: how fast one account may call Telegram (D-OBX-2, D-OBX-7).

Every account has its OWN limits, two separate budgets (`outbox.antiflood.<user|bot>`):
- writes (send, pin): `per_account_per_minute`, spread EVENLY (one write per 60/N s), plus
  `per_chat_interval_s` between two writes to one chat and `group_per_minute` to one group;
- reads (every other call: history, dialogs, catching up of groups, re-reads): `reads_per_minute`,
  spread evenly too. Per Telegram, a FloodWait may come on ANY method (errors.json, 420
  FLOOD_WAIT_%d: no method list), so every call counts.
A burst is never sent: when 500 tasks arrive at once they go out one by one.
`jitter_s`: a random extra pause, so the writes are not a metronome.
`factor` (the fuse, OBX_fuse): 0.5 after a FloodWait -> per-minute limits halved, intervals doubled.
The windows are kept in memory; the write window is rebuilt at start from `outbound_attempts`.
"""

from __future__ import annotations

import random
import time
from collections import deque

from autocash.core.config.schema import AntifloodCfg

WINDOW_S = 60.0


def _scaled(per_minute: int, factor: float) -> int:
    return max(1, int(per_minute * factor)) if per_minute > 0 else 0


class Antiflood:
    def __init__(self) -> None:
        self._writes: dict[int, deque[float]] = {}  # account -> monotonic times of writes in the window
        self._chat_last: dict[tuple[int, int], float] = {}
        self._group: dict[tuple[int, int], deque[float]] = {}
        self._next: dict[int, float] = {}  # account -> earliest time of its next write
        self._reads: dict[int, deque[float]] = {}  # account -> times of reads in the window
        self._next_read: dict[int, float] = {}  # account -> the next free read slot

    @staticmethod
    def _trim(window: deque[float], now: float) -> deque[float]:
        while window and now - window[0] >= WINDOW_S:
            window.popleft()
        return window

    # ---- writes -----------------------------------------------------------------------------------
    def recent(self, account_id: int) -> int:
        """Writes of the account in the last minute (OBX_Router balances by it)."""
        return len(self._trim(self._writes.get(account_id, deque()), time.monotonic()))

    def last_write(self, account_id: int) -> float:
        window = self._writes.get(account_id)
        return window[-1] if window else 0.0

    def delay(self, cfg: AntifloodCfg | None, account_id: int, chat_id: int, factor: float = 1.0) -> float:
        """Seconds to wait before this account may write to this chat."""
        if cfg is None:
            return 0.0
        now = time.monotonic()
        ready = self._next.get(account_id, 0.0)
        per_minute = _scaled(cfg.per_account_per_minute, factor)
        if per_minute > 0:
            window = self._trim(self._writes.get(account_id, deque()), now)
            if len(window) >= per_minute:
                ready = max(ready, window[0] + WINDOW_S)
        if cfg.per_chat_interval_s > 0:
            ready = max(ready, self._chat_last.get((account_id, chat_id), -1e9) + cfg.per_chat_interval_s / factor)
        group_limit = _scaled(cfg.group_per_minute, factor)
        if chat_id < 0 and group_limit > 0:
            group = self._trim(self._group.get((account_id, chat_id), deque()), now)
            if len(group) >= group_limit:
                ready = max(ready, group[0] + WINDOW_S)
        return max(0.0, ready - now)

    def wrote(self, cfg: AntifloodCfg | None, account_id: int, chat_id: int, at: float | None = None,
              factor: float = 1.0) -> None:
        """Record a write; the next write of the account is spread evenly over the minute."""
        now = time.monotonic() if at is None else at
        self._writes.setdefault(account_id, deque()).append(now)
        self._chat_last[(account_id, chat_id)] = now
        if chat_id < 0:
            self._group.setdefault((account_id, chat_id), deque()).append(now)
        per_minute = _scaled(cfg.per_account_per_minute, factor) if cfg is not None else 0
        if cfg is not None and per_minute > 0:
            jitter = random.uniform(0.0, cfg.jitter_s) if cfg.jitter_s > 0 else 0.0
            self._next[account_id] = now + WINDOW_S / per_minute + jitter
        if len(self._chat_last) > 5000:
            self._chat_last = {k: v for k, v in self._chat_last.items() if now - v < WINDOW_S}

    # ---- reads ------------------------------------------------------------------------------------
    def recent_reads(self, account_id: int) -> int:
        """Reads of the account in the last minute (the router of reads balances by it)."""
        return len(self._trim(self._reads.get(account_id, deque()), time.monotonic()))

    def reserve_read(self, cfg: AntifloodCfg | None, account_id: int, factor: float = 1.0) -> float:
        """Take the next read slot of the account; returns the seconds to wait for it.

        Slots are handed out in order and spaced evenly (60/N s), so reads queued at once go one by one.
        """
        now = time.monotonic()
        per_minute = _scaled(cfg.reads_per_minute, factor) if cfg is not None else 0
        slot = now
        if per_minute > 0:
            slot = max(now, self._next_read.get(account_id, 0.0))
            self._next_read[account_id] = slot + WINDOW_S / per_minute
        self._reads.setdefault(account_id, deque()).append(slot)
        self._trim(self._reads[account_id], now)
        return max(0.0, slot - now)
