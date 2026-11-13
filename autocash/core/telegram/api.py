"""Telegram client abstraction. Core code depends on this module, never on Telethon directly."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, TypeVar

ChatKind = Literal["supergroup", "channel", "group", "private", "unknown"]
EventKind = Literal["new", "edit", "delete"]
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RawEvent:
    kind: EventKind
    account_id: int
    msg_id: int
    chat_id: int | None = None  # None: deletion in a private chat / basic group
    chat_kind: ChatKind = "unknown"
    sender_id: int | None = None
    date: datetime | None = None
    edit_date: datetime | None = None
    text: str = ""  # may contain Telegram markdown
    reply_to: int | None = None
    media_type: str | None = None
    media_ref: str | None = None
    forwarded: bool = False
    anonymous: bool = False  # anonymous admin / posted on behalf of the chat
    chat_title: str | None = None
    source: str = "live"


EventHandler = Callable[[RawEvent], Awaitable[None]]


class TelegramError(Exception):
    """Any Telegram API error; `code` is stored as is when it is not in the dictionary."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


class FloodWait(TelegramError):
    def __init__(self, seconds: float, method: str = "") -> None:
        super().__init__("FLOOD_WAIT", f"FLOOD_WAIT_{int(seconds)}")
        self.seconds = seconds
        self.method = method


class RandomIdDuplicate(TelegramError):
    def __init__(self) -> None:
        super().__init__("RANDOM_ID_DUPLICATE")


class TelegramClient(Protocol):
    account_id: int

    async def start(self, handler: EventHandler) -> None: ...

    async def stop(self) -> None: ...

    async def send_message(self, chat_id: int, text: str, *, random_id: int, reply_to: int | None = None) -> int: ...


class Limiter(Protocol):
    """All active API calls of one account go through its limiter (FT-10.1)."""

    async def call(self, priority: int, method: str, fn: Callable[[], Awaitable[T]]) -> T: ...


class SimpleLimiter:
    """Stage 1a limiter: bounded concurrency, minimal interval, pause on FloodWait.

    Stage 1b replaces it with the full budget/priority limiter behind the same interface.
    """

    def __init__(self, concurrency: int = 1, min_interval_s: float = 0.0) -> None:
        self._sem = asyncio.Semaphore(concurrency)
        self._min_interval = min_interval_s
        self._next_at = 0.0
        self.paused_until = 0.0

    async def call(self, priority: int, method: str, fn: Callable[[], Awaitable[T]]) -> T:
        async with self._sem:
            delay = max(self._next_at, self.paused_until) - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_at = time.monotonic() + self._min_interval
            try:
                return await fn()
            except FloodWait as exc:
                self.paused_until = time.monotonic() + exc.seconds
                raise
