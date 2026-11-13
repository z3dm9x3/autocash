"""In-memory fake Telegram for tests: push updates, getChannelDifference, error injection.

Real accounts are never used in automated tests (AGENTS.md, rule 2).
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from autocash.core.telegram.api import ChatKind, EventHandler, FloodWait, RandomIdDuplicate, RawEvent, TelegramError

SHARED = ("supergroup", "channel")


class DifferenceTooLong(TelegramError):
    def __init__(self) -> None:
        super().__init__("DIFFERENCE_TOO_LONG")


@dataclass
class FakeMessage:
    chat_id: int
    sender_id: int | None
    text: str
    date: datetime
    ids: dict[int, int]  # account_id -> message id (one shared id under key 0 for supergroups)
    edit_date: datetime | None = None
    reply_to: int | None = None
    deleted: bool = False
    anonymous: bool = False
    forwarded: bool = False
    media_type: str | None = None

    def id_for(self, account_id: int) -> int:
        return self.ids.get(0) or self.ids[account_id]


@dataclass
class FakeChat:
    chat_id: int
    kind: ChatKind
    title: str
    members: set[int] = field(default_factory=set)
    readonly: set[int] = field(default_factory=set)
    next_id: int = 1
    pts: int = 0
    log: list[tuple[int, str, FakeMessage]] = field(default_factory=list)  # (pts, kind, message)
    messages: list[FakeMessage] = field(default_factory=list)


class FakeTelegram:
    def __init__(self, *, max_calls_per_s: float | None = None, difference_limit: int = 1000) -> None:
        self.chats: dict[int, FakeChat] = {}
        self.clients: dict[int, FakeClient] = {}
        self.sent: list[tuple[int, int, str, int]] = []  # (account_id, chat_id, text, msg_id)
        self.random_ids: dict[tuple[int, int], int] = {}
        self.failures: dict[int, deque[Exception]] = defaultdict(deque)
        self.banned: set[int] = set()
        self.restricted: set[int] = set()
        self.flood_waits = 0
        self._max_calls = max_calls_per_s
        self._calls: dict[int, deque[float]] = defaultdict(deque)
        self._acct_seq: dict[int, int] = defaultdict(int)
        self._tasks: set[asyncio.Task[None]] = set()
        self._difference_limit = difference_limit

    # ---- setup -----------------------------------------------------------------------------
    def add_chat(self, chat_id: int, kind: ChatKind = "supergroup", title: str = "", members: tuple[int, ...] = ()) -> FakeChat:
        chat = FakeChat(chat_id, kind, title or f"chat {chat_id}", set(members))
        self.chats[chat_id] = chat
        return chat

    def client(self, account_id: int) -> FakeClient:
        if account_id not in self.clients:
            self.clients[account_id] = FakeClient(self, account_id)
        return self.clients[account_id]

    def fail_next(self, account_id: int, exc: Exception, times: int = 1) -> None:
        self.failures[account_id].extend([exc] * times)

    def kick(self, account_id: int, chat_id: int) -> None:
        self.chats[chat_id].members.discard(account_id)

    # ---- producing updates ------------------------------------------------------------------------
    def _event(self, kind: str, msg: FakeMessage, chat: FakeChat, account_id: int) -> RawEvent:
        return RawEvent(
            kind=kind,  # type: ignore[arg-type]
            account_id=account_id, msg_id=msg.id_for(account_id),
            chat_id=chat.chat_id if (kind != "delete" or chat.kind in SHARED) else None,
            chat_kind=chat.kind, sender_id=msg.sender_id, date=msg.date, edit_date=msg.edit_date, text=msg.text,
            reply_to=msg.reply_to, media_type=msg.media_type, forwarded=msg.forwarded, anonymous=msg.anonymous,
            chat_title=chat.title,
        )

    def _push(self, kind: str, msg: FakeMessage, chat: FakeChat) -> None:
        chat.pts += 1
        chat.log.append((chat.pts, kind, msg))
        for account_id in sorted(chat.members):
            client = self.clients.get(account_id)
            if client is None or client.handler is None or not client.online:
                continue
            task = asyncio.ensure_future(client.handler(self._event(kind, msg, chat, account_id)))
            self._tasks.add(task)  # type: ignore[arg-type]
            task.add_done_callback(self._tasks.discard)  # type: ignore[arg-type]

    def post(self, chat_id: int, sender_id: int | None, text: str, **kw: Any) -> FakeMessage:
        chat = self.chats[chat_id]
        if chat.kind in SHARED:
            ids = {0: chat.next_id}
            chat.next_id += 1
        else:
            ids = {}
            for account_id in chat.members:
                self._acct_seq[account_id] += 1
                ids[account_id] = self._acct_seq[account_id]
        msg = FakeMessage(chat_id, sender_id, text, kw.pop("date", None) or datetime.now(UTC), ids, **kw)
        chat.messages.append(msg)
        self._push("new", msg, chat)
        return msg

    def edit(self, msg: FakeMessage, text: str, edit_date: datetime | None = None) -> None:
        msg.text = text
        msg.edit_date = edit_date or datetime.now(UTC)
        self._push("edit", msg, self.chats[msg.chat_id])

    def delete(self, msg: FakeMessage) -> None:
        msg.deleted = True
        self._push("delete", msg, self.chats[msg.chat_id])

    async def drain(self) -> None:
        """Wait until every pushed update has been handled by the clients."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ---- API surface used by clients ---------------------------------------------------------------
    def _api_call(self, account_id: int) -> None:
        if account_id in self.banned:
            raise TelegramError("USER_DEACTIVATED_BAN")
        if self.failures[account_id]:
            raise self.failures[account_id].popleft()
        if self._max_calls is not None:
            now = time.monotonic()
            calls = self._calls[account_id]
            while calls and now - calls[0] > 1.0:
                calls.popleft()
            if len(calls) >= self._max_calls:
                self.flood_waits += 1
                raise FloodWait(1.0)
            calls.append(now)

    def send(self, account_id: int, chat_id: int, text: str, random_id: int, reply_to: int | None) -> int:
        self._api_call(account_id)
        if (account_id, random_id) in self.random_ids:
            raise RandomIdDuplicate()
        chat = self.chats.get(chat_id)
        if chat is None or account_id not in chat.members:
            raise TelegramError("CHANNEL_PRIVATE")
        if account_id in self.restricted:
            raise TelegramError("PEER_FLOOD")
        if account_id in chat.readonly:
            raise TelegramError("CHAT_WRITE_FORBIDDEN")
        msg = self.post(chat_id, account_id, text, reply_to=reply_to)
        msg_id = msg.id_for(account_id)
        self.random_ids[(account_id, random_id)] = msg_id
        self.sent.append((account_id, chat_id, text, msg_id))
        return msg_id

    def get_channel_difference(self, account_id: int, chat_id: int, pts: int, limit: int = 100) -> tuple[list[RawEvent], int, bool, float]:
        """Returns (events, new_pts, final, timeout) like updates.getChannelDifference."""
        self._api_call(account_id)
        chat = self.chats.get(chat_id)
        if chat is None or account_id not in chat.members:
            raise TelegramError("CHANNEL_PRIVATE")
        if chat.pts - pts > self._difference_limit:
            raise DifferenceTooLong()
        entries = [e for e in chat.log if e[0] > pts][:limit]
        events = [self._event(kind, msg, chat, account_id) for _, kind, msg in entries]
        new_pts = entries[-1][0] if entries else pts
        return events, new_pts, new_pts >= chat.pts, 1.0


class FakeClient:
    """Implements autocash.core.telegram.api.TelegramClient on top of FakeTelegram."""

    def __init__(self, server: FakeTelegram, account_id: int) -> None:
        self.server = server
        self.account_id = account_id
        self.handler: EventHandler | None = None
        self.online = False

    async def start(self, handler: EventHandler) -> None:
        self.handler = handler
        self.online = True

    async def stop(self) -> None:
        self.online = False

    async def send_message(self, chat_id: int, text: str, *, random_id: int, reply_to: int | None = None) -> int:
        await asyncio.sleep(0)
        return self.server.send(self.account_id, chat_id, text, random_id, reply_to)

    async def get_channel_difference(self, chat_id: int, pts: int, limit: int = 100) -> tuple[list[RawEvent], int, bool, float]:
        await asyncio.sleep(0)
        return self.server.get_channel_difference(self.account_id, chat_id, pts, limit)
