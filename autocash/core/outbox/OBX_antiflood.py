"""OBX_Antifloodwaiter: how fast one account may write to Telegram (D-OBX-2).

Limits of one account, from the configuration (`outbox.antiflood.<user|bot>`):
- `per_account_per_minute` writes per minute, spread EVENLY: one write per 60/N seconds. A burst is
  never sent, even when 500 tasks arrive at once: they wait in the queue and go out one by one;
- `per_chat_interval_s` between two writes of the account to one chat;
- `group_per_minute` writes of the account to one group in any 60 seconds;
- `jitter_s`: a random extra pause, so the writes are not a metronome.
Every write counts: a send, a pin. A FloodWait from Telegram pauses the account on top of this
(the limiter of the pool). The window is kept in memory and rebuilt at start from
`outbound_attempts`, so a restart does not reset it.
"""

from __future__ import annotations

import random
import time
from collections import deque

from autocash.core.config.schema import AntifloodCfg

WINDOW_S = 60.0


class Antiflood:
    def __init__(self) -> None:
        self._writes: dict[int, deque[float]] = {}  # account -> monotonic times of writes in the window
        self._chat_last: dict[tuple[int, int], float] = {}
        self._group: dict[tuple[int, int], deque[float]] = {}
        self._next: dict[int, float] = {}  # account -> earliest time of its next write

    @staticmethod
    def _trim(window: deque[float], now: float) -> deque[float]:
        while window and now - window[0] >= WINDOW_S:
            window.popleft()
        return window

    def recent(self, account_id: int) -> int:
        """Writes of the account in the last minute (OBX_Router balances by it)."""
        return len(self._trim(self._writes.get(account_id, deque()), time.monotonic()))

    def last_write(self, account_id: int) -> float:
        window = self._writes.get(account_id)
        return window[-1] if window else 0.0

    def delay(self, cfg: AntifloodCfg | None, account_id: int, chat_id: int) -> float:
        """Seconds to wait before this account may write to this chat."""
        if cfg is None:
            return 0.0
        now = time.monotonic()
        ready = self._next.get(account_id, 0.0)
        if cfg.per_account_per_minute > 0:
            window = self._trim(self._writes.get(account_id, deque()), now)
            if len(window) >= cfg.per_account_per_minute:
                ready = max(ready, window[0] + WINDOW_S)
        if cfg.per_chat_interval_s > 0:
            ready = max(ready, self._chat_last.get((account_id, chat_id), -1e9) + cfg.per_chat_interval_s)
        if chat_id < 0 and cfg.group_per_minute > 0:
            group = self._trim(self._group.get((account_id, chat_id), deque()), now)
            if len(group) >= cfg.group_per_minute:
                ready = max(ready, group[0] + WINDOW_S)
        return max(0.0, ready - now)

    def wrote(self, cfg: AntifloodCfg | None, account_id: int, chat_id: int, at: float | None = None) -> None:
        """Record a write; the next write of the account is spread evenly over the minute."""
        now = time.monotonic() if at is None else at
        self._writes.setdefault(account_id, deque()).append(now)
        self._chat_last[(account_id, chat_id)] = now
        if chat_id < 0:
            self._group.setdefault((account_id, chat_id), deque()).append(now)
        if cfg is not None and cfg.per_account_per_minute > 0:
            spacing = WINDOW_S / cfg.per_account_per_minute
            jitter = random.uniform(0.0, cfg.jitter_s) if cfg.jitter_s > 0 else 0.0
            self._next[account_id] = now + spacing + jitter
        if len(self._chat_last) > 5000:
            self._chat_last = {k: v for k, v in self._chat_last.items() if now - v < WINDOW_S}
