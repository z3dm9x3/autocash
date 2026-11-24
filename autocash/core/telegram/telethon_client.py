"""Telethon implementation of `TelegramClient` (the only module of the core that imports Telethon).

Incoming updates are converted to `RawEvent` from the update itself and Telethon's entity
cache only - no API calls on the inbound path (A-8). `flood_sleep_threshold=0`: FloodWait is
never slept away inside the library, it reaches our limiter (invariant 6).

NOT verified against live Telegram: automated tests cover the pure conversions only.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import datetime
from typing import Any, TypeVar

from telethon import TelegramClient as _Telethon
from telethon import errors, events, functions, types, utils
from telethon.extensions import markdown
from telethon.sessions import StringSession

from autocash.core.telegram.api import (
    ChatKind,
    DialogInfo,
    Difference,
    DifferenceTooLong,
    EventHandler,
    EventKind,
    FloodWait,
    RandomIdDuplicate,
    RawEvent,
    TelegramError,
)

log = logging.getLogger(__name__)
T = TypeVar("T")
# Recent dialogs read when the access hash of a private peer is not cached (D-OBX-6): one request;
# a new private dialog is on top of the list.
PEER_DIALOGS = 20

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
# Base classes of Telethon errors: their names say nothing, the server text is in `.message`.
_GENERIC = {"RPCError", "BadRequestError", "ForbiddenError", "FloodError", "UnauthorizedError", "NotFoundError",
            "ServerError", "InvalidDCError", "AuthKeyError", "TimedOutError"}


def error_code(exc: BaseException) -> str:
    """Telegram error code of a Telethon exception: PeerFloodError -> PEER_FLOOD.

    An unknown code is returned as the server sent it (FT-11.5).
    """
    name = type(exc).__name__
    if name in _GENERIC or not name.endswith("Error"):
        return str(getattr(exc, "message", None) or name)
    return _CAMEL.sub("_", name[: -len("Error")]).upper()


def chat_kind(msg: Any) -> ChatKind:
    peer = msg.peer_id
    if isinstance(peer, types.PeerChannel):
        return "channel" if getattr(msg, "post", False) else "supergroup"
    if isinstance(peer, types.PeerChat):
        return "group"
    if isinstance(peer, types.PeerUser):
        return "private"
    return "unknown"


def _is_pdf(document: Any) -> bool:
    if str(getattr(document, "mime_type", "") or "").lower() == "application/pdf":
        return True
    return any(str(getattr(a, "file_name", "") or "").lower().endswith(".pdf")
               for a in getattr(document, "attributes", None) or [])


def media_info(msg: Any) -> tuple[str | None, str | None]:
    media = getattr(msg, "media", None)
    if media is None:
        return None, None
    kind = type(media).__name__.removeprefix("MessageMedia").lower() or "media"
    inner = getattr(media, "photo", None) or getattr(media, "document", None)
    if kind == "document" and inner is not None and _is_pdf(inner):
        kind = "pdf"  # a receipt in PDF is a fact modules use (D-AS-10); other documents stay "document"
    ref = f"{kind}:{inner.id}" if inner is not None and getattr(inner, "id", None) is not None else None
    return kind, ref


def message_text(msg: Any) -> str:
    """Text with Telegram markdown where Telethon can render it, the plain text otherwise."""
    try:
        text = msg.text
    except Exception:
        text = None
    return str(text if text is not None else (getattr(msg, "message", None) or ""))


def message_event(kind: EventKind, account_id: int, msg: Any, chat_title: str | None = None,
                  pts: int | None = None) -> RawEvent:
    """Build a RawEvent from a Telethon message. Pure: uses only the fields of the update."""
    ckind = chat_kind(msg)
    chat_id = utils.get_peer_id(msg.peer_id)
    from_id = getattr(msg, "from_id", None)
    # Anonymous admin / posted on behalf of the chat: there is no user to take a role from (FT-2.9).
    anonymous = ckind in ("supergroup", "group") and not isinstance(from_id, types.PeerUser)
    if ckind == "channel":
        sender_id = None
    elif isinstance(from_id, types.PeerUser):
        sender_id = from_id.user_id  # of a forwarded message: the one who forwarded it
    elif ckind == "private":
        sender_id = account_id if getattr(msg, "out", False) else chat_id
    else:
        sender_id = None
    reply = getattr(msg, "reply_to", None)
    media_type, media_ref = media_info(msg)
    return RawEvent(
        kind=kind, account_id=account_id, msg_id=msg.id, chat_id=chat_id, chat_kind=ckind, sender_id=sender_id,
        date=msg.date, edit_date=getattr(msg, "edit_date", None), text=message_text(msg),
        reply_to=getattr(reply, "reply_to_msg_id", None), media_type=media_type, media_ref=media_ref,
        forwarded=getattr(msg, "fwd_from", None) is not None, anonymous=anonymous, chat_title=chat_title,
        pts=pts if ckind in ("supergroup", "channel") else None,
    )


def draft_event(account_id: int, update: Any) -> RawEvent | None:
    """A RawEvent "draft" from UpdateDraftMessage: private dialogs only (not Saved Messages, not topics).
    An emptied draft comes with text "". Pure: uses only the fields of the update."""
    peer = getattr(update, "peer", None)
    if not isinstance(peer, types.PeerUser) or peer.user_id == account_id or getattr(update, "top_msg_id", None):
        return None
    draft = getattr(update, "draft", None)
    text = str(getattr(draft, "message", "") or "") if isinstance(draft, types.DraftMessage) else ""
    reply = getattr(draft, "reply_to", None)
    return RawEvent(kind="draft", account_id=account_id, msg_id=0, chat_id=int(peer.user_id), chat_kind="private",
                    sender_id=account_id, date=getattr(draft, "date", None), text=text,
                    reply_to=getattr(reply, "reply_to_msg_id", None),
                    media_type="draft_media" if getattr(draft, "media", None) is not None else None)


def sent_message_id(result: Any, random_id: int) -> int:
    if isinstance(result, types.UpdateShortSentMessage):
        return int(result.id)
    for update in getattr(result, "updates", []):
        if isinstance(update, types.UpdateMessageID) and update.random_id == random_id:
            return int(update.id)
    for update in getattr(result, "updates", []):
        message_id = getattr(getattr(update, "message", None), "id", None)
        if message_id is not None:
            return int(message_id)
    raise TelegramError("NO_MESSAGE_ID", "the send result carries no message id")


def dialog_info(dialog: Any) -> DialogInfo | None:
    """One entry of the dialog list -> DialogInfo; None for chats the account has left."""
    entity = dialog.entity
    if getattr(entity, "left", False) or getattr(entity, "deactivated", False):
        return None
    banned = getattr(entity, "default_banned_rights", None)
    admin = bool(getattr(entity, "creator", False) or getattr(entity, "admin_rights", None))
    members_may_write = not getattr(banned, "send_messages", False)
    if isinstance(entity, types.Channel):
        if entity.megagroup:
            kind: ChatKind = "supergroup"
            can_send = admin or members_may_write
        else:
            kind, can_send = "channel", admin
        pts = getattr(dialog.dialog, "pts", None)
    elif isinstance(entity, types.Chat):
        kind, can_send, pts = "group", admin or members_may_write, None
    elif isinstance(entity, types.User):
        kind, can_send, pts = "private", True, None
    else:
        return None
    return DialogInfo(int(dialog.id), kind, getattr(entity, "title", None), pts, can_send)


def difference_events(account_id: int, chat_id: int, result: Any) -> list[RawEvent]:
    """New messages, edits and deletions of a getChannelDifference answer, in their order."""
    out: list[RawEvent] = []
    for msg in getattr(result, "new_messages", []):
        if isinstance(msg, types.Message):
            out.append(message_event("new", account_id, msg))
    for update in getattr(result, "other_updates", []):
        if isinstance(update, types.UpdateEditChannelMessage) and isinstance(update.message, types.Message):
            out.append(message_event("edit", account_id, update.message))
        elif isinstance(update, types.UpdateDeleteChannelMessages):
            out.extend(RawEvent(kind="delete", account_id=account_id, msg_id=msg_id, chat_id=chat_id,
                                chat_kind="supergroup") for msg_id in update.messages)
    return out


class TelethonClient:
    def __init__(self, session: str | StringSession, api_id: int, api_hash: str, *,
                 bot_token: str | None = None) -> None:
        self._bot_token = bot_token
        self._client = _Telethon(session, api_id, api_hash, flood_sleep_threshold=0)
        self._handler: EventHandler | None = None
        self.account_id = 0  # known after start()
        # Set by the pool (D-OBX-7): a Telegram call the adapter makes by itself waits for the read budget.
        self.extra_call: Callable[[str], Awaitable[None]] | None = None
        self.kind = "bot" if bot_token else "user"

    @classmethod
    def from_string(cls, session: str, api_id: int, api_hash: str, bot_token: str = "") -> TelethonClient:
        """A client over a session string taken from an encrypted session file (FT-1.9)."""
        return cls(StringSession(session), api_id, api_hash, bot_token=bot_token or None)

    async def start(self, handler: EventHandler) -> None:
        self._handler = handler
        client = self._client
        try:
            await client.connect()
            if not await client.is_user_authorized():
                if not self._bot_token:
                    await client.disconnect()
                    raise TelegramError("AUTH_KEY_UNREGISTERED", "the session is not logged in")
                await client.start(bot_token=self._bot_token)
            me = await client.get_me()
        except errors.RPCError as exc:
            raise TelegramError(error_code(exc), str(exc)) from exc
        if me is None:
            await client.disconnect()
            raise TelegramError("AUTH_KEY_UNREGISTERED", "Telegram did not return the account of this session")
        self.account_id = int(me.id)
        self.kind = "bot" if getattr(me, "bot", False) else "user"
        if not client.list_event_handlers():
            client.add_event_handler(self._on_new, events.NewMessage())
            client.add_event_handler(self._on_edit, events.MessageEdited())
            client.add_event_handler(self._on_delete, events.MessageDeleted())
            client.add_event_handler(self._on_action, events.ChatAction())
            client.add_event_handler(self._on_draft, events.Raw(types.UpdateDraftMessage))

    async def stop(self) -> None:
        await self._client.disconnect()

    async def wait_disconnected(self) -> None:
        # Telethon reconnects by itself; this future ends only when it gave up or was stopped.
        try:
            await self._client.disconnected
        except errors.RPCError as exc:
            raise TelegramError(error_code(exc), str(exc)) from exc

    # ---- incoming ------------------------------------------------------------------------
    async def _emit(self, event: RawEvent) -> None:
        if self._handler is None:
            return
        try:
            await self._handler(event)
        except Exception:
            log.exception("update handler failed (chat %s, message %s)", event.chat_id, event.msg_id)

    @staticmethod
    def _title(event: Any) -> str | None:
        chat = event.chat  # entity cache only, never an API call
        return getattr(chat, "title", None) if chat is not None else None

    @staticmethod
    def _pts(event: Any) -> int | None:
        return getattr(getattr(event, "original_update", None), "pts", None)

    async def _on_new(self, event: Any) -> None:
        await self._emit(message_event("new", self.account_id, event.message, self._title(event), self._pts(event)))

    async def _on_edit(self, event: Any) -> None:
        await self._emit(message_event("edit", self.account_id, event.message, self._title(event), self._pts(event)))

    async def _on_delete(self, event: Any) -> None:
        # Telegram reports the chat only for channels and supergroups.
        chat_id = event.chat_id
        for msg_id in event.deleted_ids:
            await self._emit(RawEvent(
                kind="delete", account_id=self.account_id, msg_id=msg_id, chat_id=chat_id,
                chat_kind="supergroup" if chat_id is not None else "unknown",
                pts=self._pts(event) if chat_id is not None else None,
            ))

    async def _on_action(self, event: Any) -> None:
        """The account itself joined or left a chat: the membership map follows (FT-1.4)."""
        if event.chat_id is None or self.account_id not in (event.user_ids or []):
            return
        if event.user_joined or event.user_added:
            kind: EventKind = "joined"
        elif event.user_left or event.user_kicked:
            kind = "left"
        else:
            return
        await self._emit(RawEvent(kind=kind, account_id=self.account_id, msg_id=0, chat_id=event.chat_id,
                                  chat_title=self._title(event)))

    async def _on_draft(self, update: Any) -> None:
        """The cloud draft of a private dialog changed (QuickCast2, D-QK2-1); other peers are skipped."""
        event = draft_event(self.account_id, update)
        if event is not None:
            await self._emit(event)

    # ---- active calls --------------------------------------------------------------------
    async def _call(self, method: str, awaitable: Awaitable[T]) -> T:
        try:
            return await awaitable
        except errors.RandomIdDuplicateError as exc:
            raise RandomIdDuplicate() from exc
        except errors.FloodWaitError as exc:
            raise FloodWait(float(exc.seconds), method) from exc
        except errors.RPCError as exc:
            raise TelegramError(error_code(exc), str(exc)) from exc
        except ValueError as exc:
            # Telethon could not resolve the peer from its cache: the account does not know this chat.
            raise TelegramError("PEER_ID_INVALID", str(exc)) from exc

    async def _peer(self, chat_id: int) -> Any:
        """The input peer of a chat. Telethon keeps access hashes in memory only, and a new private
        dialog may arrive without the user's hash (a short update), and none survive a restart
        (D-OBX-6). Then the recent dialogs of the account are read once: they carry the hash."""
        client = self._client
        try:
            return await client.get_input_entity(chat_id)
        except ValueError:
            if chat_id < 0 or self.kind == "bot":
                raise
        log.info("peer %s is not in the cache of account %s: reading the recent dialogs", chat_id, self.account_id)
        if self.extra_call is not None:
            await self.extra_call("getDialogs")
        async for _ in client.iter_dialogs(limit=PEER_DIALOGS):
            pass
        return await client.get_input_entity(chat_id)

    async def send_message(self, chat_id: int, text: str, *, random_id: int, reply_to: int | None = None,
                           clear_draft: bool = False) -> int:
        result = await self._call("messages.sendMessage", self._send(chat_id, text, random_id, reply_to, clear_draft))
        return sent_message_id(result, random_id)

    async def _send(self, chat_id: int, text: str, random_id: int, reply_to: int | None, clear_draft: bool) -> Any:
        client = self._client
        peer = await self._peer(chat_id)
        plain, entities = markdown.parse(text)
        return await client(functions.messages.SendMessageRequest(
            peer=peer, message=plain, entities=entities or None, random_id=random_id, no_webpage=True,
            reply_to=types.InputReplyToMessage(reply_to_msg_id=reply_to) if reply_to else None,
            clear_draft=clear_draft or None,
        ))

    async def pin_message(self, chat_id: int, msg_id: int) -> None:
        await self._call("messages.updatePinnedMessage", self._pin(chat_id, msg_id))

    async def _pin(self, chat_id: int, msg_id: int) -> Any:
        return await self._client.pin_message(await self._peer(chat_id), msg_id, notify=False, pm_oneside=False)

    async def delete_messages(self, chat_id: int, msg_ids: list[int]) -> int:
        result = await self._call("messages.deleteMessages", self._delete(chat_id, msg_ids))
        return sum(int(getattr(r, "pts_count", 0) or 0) for r in (result or []))

    async def _delete(self, chat_id: int, msg_ids: list[int]) -> Any:
        return await self._client.delete_messages(await self._peer(chat_id), msg_ids, revoke=True)

    async def get_dialogs(self) -> list[DialogInfo]:
        if self.kind == "bot":
            return []  # Telegram gives bots no dialog list
        return await self._call("messages.getDialogs", self._dialogs())

    async def _dialogs(self) -> list[DialogInfo]:
        out = []
        async for dialog in self._client.iter_dialogs():
            info = dialog_info(dialog)
            if info is not None:
                out.append(info)
        return out

    async def get_history(self, chat_id: int, after_id: int, since: datetime, limit: int) -> list[RawEvent]:
        # reverse=True reads from old to new: from `since` at first, then after the last loaded id.
        kwargs: dict[str, Any] = {"min_id": after_id} if after_id else {"offset_date": since}
        msgs = await self._call("messages.getHistory", self._messages(chat_id, limit=limit, reverse=True, **kwargs))
        return [replace(message_event("edit" if getattr(m, "edit_date", None) else "new", self.account_id, m),
                        source="backfill")
                for m in msgs if m is not None and not isinstance(m, (types.MessageEmpty, types.MessageService))]

    async def get_messages(self, chat_id: int, ids: list[int]) -> list[RawEvent]:
        msgs = await self._call("channels.getMessages", self._messages(chat_id, ids=ids))
        return [message_event("edit" if getattr(m, "edit_date", None) else "new", self.account_id, m)
                for m in msgs if m is not None and not isinstance(m, types.MessageEmpty)]

    async def _messages(self, chat_id: int, **kwargs: Any) -> Any:
        return await self._client.get_messages(await self._peer(chat_id), **kwargs)

    async def get_channel_difference(self, chat_id: int, pts: int, limit: int = 100) -> Difference:
        result = await self._call("updates.getChannelDifference", self._difference(chat_id, pts, limit))
        if isinstance(result, types.updates.ChannelDifferenceTooLong):
            raise DifferenceTooLong(getattr(result.dialog, "pts", None))
        timeout = float(getattr(result, "timeout", 0) or 0)
        return difference_events(self.account_id, chat_id, result), int(result.pts), bool(result.final), timeout

    async def _difference(self, chat_id: int, pts: int, limit: int) -> Any:
        client = self._client
        channel = await client.get_input_entity(chat_id)
        result = await client(functions.updates.GetChannelDifferenceRequest(
            channel=channel, filter=types.ChannelMessagesFilterEmpty(), pts=pts, limit=limit, force=True))
        for msg in getattr(result, "new_messages", []):
            finish = getattr(msg, "_finish_init", None)
            if finish is not None:
                try:
                    finish(client, {}, None)  # lets Telethon render markdown of the text
                except Exception:
                    log.debug("could not finish a message of the difference", exc_info=True)
        return result
