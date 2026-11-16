"""In-memory fake Telegram for tests: push updates, getChannelDifference, error injection.

Real accounts are never used in automated tests (AGENTS.md, rule 2).
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from autocash.core.telegram.api import (
    ChatKind,
    DialogInfo,
    Difference,
    DifferenceTooLong,
    EventHandler,
    FloodWait,
    RandomIdDuplicate,
    RawEvent,
    TelegramError,
)

SHARED = ("supergroup", "channel")


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
        # account -> queued failures: (method or None for any call, exception)
        self.failures: dict[int, deque[tuple[str | None, Exception]]] = defaultdict(deque)
        self.banned: set[int] = set()
        self.restricted: set[int] = set()
        self.flood_waits = 0
        self.calls: dict[str, int] = defaultdict(int)  # method -> number of API calls
        self._max_calls = max_calls_per_s
        self._calls: dict[int, deque[float]] = defaultdict(deque)
        self._acct_seq: dict[int, int] = defaultdict(int)
        self._tasks: set[asyncio.Future[None]] = set()
        self._difference_limit = difference_limit

    # ---- setup -----------------------------------------------------------------------------
    def add_chat(
        self, chat_id: int, kind: ChatKind = "supergroup", title: str = "", members: tuple[int, ...] = ()
    ) -> FakeChat:
        chat = FakeChat(chat_id, kind, title or f"chat {chat_id}", set(members))
        self.chats[chat_id] = chat
        return chat

    def client(self, account_id: int, kind: str = "user") -> FakeClient:
        if account_id not in self.clients:
            self.clients[account_id] = FakeClient(self, account_id, kind)
        return self.clients[account_id]

    def fail_next(self, account_id: int, exc: Exception, times: int = 1, method: str | None = None) -> None:
        self.failures[account_id].extend([(method, exc)] * times)

    # ---- changes of the world -----------------------------------------------------------------
    def _notify_member(self, kind: str, account_id: int, chat: FakeChat) -> None:
        client = self.clients.get(account_id)
        if client is None or client.handler is None or not client.online:
            return
        event = RawEvent(kind=kind, account_id=account_id, msg_id=0, chat_id=chat.chat_id,  # type: ignore[arg-type]
                         chat_kind=chat.kind, chat_title=chat.title, pts=chat.pts if chat.kind in SHARED else None)
        self._track(client.handler(event))

    def join(self, account_id: int, chat_id: int) -> None:
        chat = self.chats[chat_id]
        chat.members.add(account_id)
        self._notify_member("joined", account_id, chat)

    def kick(self, account_id: int, chat_id: int) -> None:
        chat = self.chats[chat_id]
        chat.members.discard(account_id)
        self._notify_member("left", account_id, chat)

    def delete_chat(self, chat_id: int) -> None:
        chat = self.chats.pop(chat_id)
        for account_id in sorted(chat.members):
            self._notify_member("left", account_id, chat)

    def ban(self, account_id: int) -> None:
        """The account is banned: its connection ends and every later call fails."""
        self.banned.add(account_id)
        client = self.clients.get(account_id)
        if client is not None:
            client.drop(TelegramError("USER_DEACTIVATED_BAN"))

    # ---- producing updates ------------------------------------------------------------------------
    def _track(self, coro: Any) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _event(self, kind: str, msg: FakeMessage, chat: FakeChat, account_id: int, pts: int | None = None) -> RawEvent:
        shared = chat.kind in SHARED
        return RawEvent(
            kind=kind,  # type: ignore[arg-type]
            account_id=account_id, msg_id=msg.id_for(account_id),
            chat_id=chat.chat_id if (kind != "delete" or shared) else None,
            chat_kind=chat.kind, sender_id=msg.sender_id, date=msg.date, edit_date=msg.edit_date, text=msg.text,
            reply_to=msg.reply_to, media_type=msg.media_type, forwarded=msg.forwarded, anonymous=msg.anonymous,
            chat_title=chat.title, pts=pts if shared else None,
        )

    def _push(self, kind: str, msg: FakeMessage, chat: FakeChat) -> None:
        chat.pts += 1
        chat.log.append((chat.pts, kind, msg))
        for account_id in sorted(chat.members):
            client = self.clients.get(account_id)
            if client is None or client.handler is None or not client.online or client.deaf:
                continue
            self._track(client.handler(self._event(kind, msg, chat, account_id, chat.pts)))

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
    def _api_call(self, account_id: int, method: str) -> None:
        self.calls[method] += 1
        if account_id in self.banned:
            raise TelegramError("USER_DEACTIVATED_BAN")
        queue = self.failures[account_id]
        for i, (wanted, exc) in enumerate(queue):
            if wanted is None or wanted == method:
                del queue[i]
                raise exc
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
        self._api_call(account_id, "send")
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

    def messages(self, account_id: int, chat_id: int, ids: list[int]) -> list[RawEvent]:
        self._api_call(account_id, "getMessages")
        chat = self.chats.get(chat_id)
        if chat is None or account_id not in chat.members:
            raise TelegramError("CHANNEL_PRIVATE")
        wanted = set(ids)
        return [self._event("edit" if m.edit_date else "new", m, chat, account_id) for m in chat.messages
                if not m.deleted and m.id_for(account_id) in wanted]

    def history(self, account_id: int, chat_id: int, after_id: int, since: datetime, limit: int) -> list[RawEvent]:
        self._api_call(account_id, "getHistory")
        chat = self.chats.get(chat_id)
        if chat is None or account_id not in chat.members:
            raise TelegramError("CHANNEL_PRIVATE")
        newer = [m for m in chat.messages
                 if not m.deleted and (0 in m.ids or account_id in m.ids)
                 and (m.id_for(account_id) > after_id if after_id else m.date >= since)]
        newer.sort(key=lambda m: m.id_for(account_id))
        return [replace(self._event("edit" if m.edit_date else "new", m, chat, account_id), source="backfill")
                for m in newer[:limit]]

    def dialogs(self, account_id: int) -> list[DialogInfo]:
        self._api_call(account_id, "getDialogs")
        return [
            DialogInfo(chat.chat_id, chat.kind, chat.title, chat.pts if chat.kind in SHARED else None,
                       account_id not in chat.readonly)
            for chat in self.chats.values() if account_id in chat.members
        ]

    def get_channel_difference(self, account_id: int, chat_id: int, pts: int, limit: int = 100) -> Difference:
        """Returns (events, new_pts, final, timeout) like updates.getChannelDifference."""
        self._api_call(account_id, "getChannelDifference")
        chat = self.chats.get(chat_id)
        if chat is None or account_id not in chat.members:
            raise TelegramError("CHANNEL_PRIVATE")
        if chat.pts - pts > self._difference_limit:
            raise DifferenceTooLong(chat.pts)
        entries = [e for e in chat.log if e[0] > pts][:limit]
        events = [self._event(kind, msg, chat, account_id) for _, kind, msg in entries]
        new_pts = entries[-1][0] if entries else pts
        return events, new_pts, new_pts >= chat.pts, 1.0


class FakeClient:
    """Implements autocash.core.telegram.api.TelegramClient on top of FakeTelegram."""

    def __init__(self, server: FakeTelegram, account_id: int, kind: str = "user") -> None:
        self.server = server
        self.account_id = account_id
        self.kind = kind
        self.handler: EventHandler | None = None
        self.online = False
        self.deaf = False  # connected, but Telegram delivers no push updates (FT-2.5)
        self.starts = 0
        self._gone: asyncio.Future[None] | None = None

    async def start(self, handler: EventHandler) -> None:
        if self.account_id in self.server.banned:
            raise TelegramError("USER_DEACTIVATED_BAN")
        self.handler = handler
        self.online = True
        self.starts += 1
        self._gone = asyncio.get_running_loop().create_future()

    async def stop(self) -> None:
        self.online = False
        if self._gone is not None and not self._gone.done():
            self._gone.set_result(None)

    def drop(self, error: Exception | None = None) -> None:
        """The connection is lost (network) or ended by Telegram (ban, revoked session)."""
        self.online = False
        if self._gone is not None and not self._gone.done():
            if error is None:
                self._gone.set_result(None)
            else:
                self._gone.set_exception(error)

    async def wait_disconnected(self) -> None:
        if self._gone is not None:
            await self._gone

    async def send_message(self, chat_id: int, text: str, *, random_id: int, reply_to: int | None = None) -> int:
        await asyncio.sleep(0)
        return self.server.send(self.account_id, chat_id, text, random_id, reply_to)

    async def get_dialogs(self) -> list[DialogInfo]:
        await asyncio.sleep(0)
        return self.server.dialogs(self.account_id)

    async def get_channel_difference(self, chat_id: int, pts: int, limit: int = 100) -> Difference:
        await asyncio.sleep(0)
        return self.server.get_channel_difference(self.account_id, chat_id, pts, limit)

    async def get_messages(self, chat_id: int, ids: list[int]) -> list[RawEvent]:
        await asyncio.sleep(0)
        return self.server.messages(self.account_id, chat_id, ids)

    async def get_history(self, chat_id: int, after_id: int, since: datetime, limit: int) -> list[RawEvent]:
        await asyncio.sleep(0)
        return self.server.history(self.account_id, chat_id, after_id, since, limit)
