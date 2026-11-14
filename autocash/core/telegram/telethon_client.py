"""Telethon implementation of `TelegramClient` (the only module of the core that imports Telethon).

Incoming updates are converted to `RawEvent` from the update itself and Telethon's entity
cache only - no API calls on the inbound path (A-8). `flood_sleep_threshold=0`: FloodWait is
never slept away inside the library, it reaches our limiter (invariant 6).
"""

from __future__ import annotations

import logging
import re
from typing import Any

from telethon import TelegramClient as _Telethon
from telethon import errors, events, functions, types, utils
from telethon.extensions import markdown

from autocash.core.telegram.api import (
    ChatKind,
    EventHandler,
    EventKind,
    FloodWait,
    RandomIdDuplicate,
    RawEvent,
    TelegramError,
)

log = logging.getLogger(__name__)

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


def media_info(msg: Any) -> tuple[str | None, str | None]:
    media = getattr(msg, "media", None)
    if media is None:
        return None, None
    kind = type(media).__name__.removeprefix("MessageMedia").lower() or "media"
    inner = getattr(media, "photo", None) or getattr(media, "document", None)
    ref = f"{kind}:{inner.id}" if inner is not None and getattr(inner, "id", None) is not None else None
    return kind, ref


def message_event(kind: EventKind, account_id: int, msg: Any, chat_title: str | None = None) -> RawEvent:
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
        date=msg.date, edit_date=getattr(msg, "edit_date", None), text=msg.text or "",
        reply_to=getattr(reply, "reply_to_msg_id", None), media_type=media_type, media_ref=media_ref,
        forwarded=getattr(msg, "fwd_from", None) is not None, anonymous=anonymous, chat_title=chat_title,
    )


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


class TelethonClient:
    def __init__(self, session: str, api_id: int, api_hash: str, *, bot_token: str | None = None) -> None:
        self._bot_token = bot_token
        self._client = _Telethon(session, api_id, api_hash, flood_sleep_threshold=0)
        self._handler: EventHandler | None = None
        self.account_id = 0  # known after start()

    async def start(self, handler: EventHandler) -> None:
        self._handler = handler
        client = self._client
        await client.connect()
        if not await client.is_user_authorized():
            if not self._bot_token:
                await client.disconnect()
                raise RuntimeError("the session is not logged in: run accounts/login.py for this account")
            await client.start(bot_token=self._bot_token)
        me = await client.get_me()
        if me is None:
            await client.disconnect()
            raise RuntimeError("Telegram did not return the account of this session")
        self.account_id = int(me.id)
        client.add_event_handler(self._on_new, events.NewMessage())
        client.add_event_handler(self._on_edit, events.MessageEdited())
        client.add_event_handler(self._on_delete, events.MessageDeleted())

    async def stop(self) -> None:
        await self._client.disconnect()

    async def run_until_disconnected(self) -> None:
        await self._client.run_until_disconnected()

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

    async def _on_new(self, event: Any) -> None:
        await self._emit(message_event("new", self.account_id, event.message, self._title(event)))

    async def _on_edit(self, event: Any) -> None:
        await self._emit(message_event("edit", self.account_id, event.message, self._title(event)))

    async def _on_delete(self, event: Any) -> None:
        # Telegram reports the chat only for channels and supergroups.
        chat_id = event.chat_id
        for msg_id in event.deleted_ids:
            await self._emit(RawEvent(
                kind="delete", account_id=self.account_id, msg_id=msg_id, chat_id=chat_id,
                chat_kind="supergroup" if chat_id is not None else "unknown",
            ))

    # ---- outgoing ------------------------------------------------------------------------
    async def send_message(self, chat_id: int, text: str, *, random_id: int, reply_to: int | None = None) -> int:
        client = self._client
        try:
            peer = await client.get_input_entity(chat_id)
            plain, entities = markdown.parse(text)
            result = await client(functions.messages.SendMessageRequest(
                peer=peer, message=plain, entities=entities or None, random_id=random_id, no_webpage=True,
                reply_to=types.InputReplyToMessage(reply_to_msg_id=reply_to) if reply_to else None,
            ))
        except errors.RandomIdDuplicateError as exc:
            raise RandomIdDuplicate() from exc
        except errors.FloodWaitError as exc:
            raise FloodWait(float(exc.seconds), "messages.sendMessage") from exc
        except errors.RPCError as exc:
            raise TelegramError(error_code(exc), str(exc)) from exc
        except ValueError as exc:
            # Telethon could not resolve the peer from its cache: the account does not know this chat.
            raise TelegramError("PEER_NOT_RESOLVED", str(exc)) from exc
        return sent_message_id(result, random_id)
