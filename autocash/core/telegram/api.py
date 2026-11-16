"""Telegram client abstraction. Core code depends on this module, never on Telethon directly."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, TypeVar

ChatKind = Literal["supergroup", "channel", "group", "private", "unknown"]
# "joined" / "left": the account itself entered or left the chat (left also covers a deleted chat).
EventKind = Literal["new", "edit", "delete", "joined", "left"]
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
    pts: int | None = None  # channel/supergroup update counter, when the update carries it


@dataclass(frozen=True, slots=True)
class DialogInfo:
    chat_id: int
    kind: ChatKind
    title: str | None = None
    pts: int | None = None  # channels and supergroups only
    can_send: bool = True


EventHandler = Callable[[RawEvent], Awaitable[None]]
Difference = tuple[list[RawEvent], int, bool, float]  # events, new pts, final, timeout (seconds)


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


class DifferenceTooLong(TelegramError):
    """The gap is too big for getChannelDifference; `pts` is the current counter if known (FT-2.7)."""

    def __init__(self, pts: int | None = None) -> None:
        super().__init__("DIFFERENCE_TOO_LONG")
        self.pts = pts


class TelegramClient(Protocol):
    account_id: int  # known after start() at the latest
    kind: str  # "user" | "bot"

    async def start(self, handler: EventHandler) -> None: ...

    async def stop(self) -> None: ...

    async def wait_disconnected(self) -> None:
        """Returns (or raises TelegramError) when the connection is gone."""
        ...

    async def send_message(self, chat_id: int, text: str, *, random_id: int, reply_to: int | None = None) -> int: ...

    async def get_dialogs(self) -> list[DialogInfo]: ...

    async def get_channel_difference(self, chat_id: int, pts: int, limit: int = 100) -> Difference: ...


class Limiter(Protocol):
    """All active API calls of one account go through its limiter (FT-10.1)."""

    async def call(self, priority: int, method: str, fn: Callable[[], Awaitable[T]]) -> T: ...
