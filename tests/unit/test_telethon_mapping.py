"""Telethon update -> RawEvent mapping and error codes. No network, no accounts (rule 2)."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from telethon import errors, types

from autocash.core.telegram.telethon_client import draft_event, error_code, message_event, sent_message_id

ME = 111
DATE = datetime(2026, 11, 14, 10, 0, tzinfo=UTC)


def msg(peer: Any, **kw: Any) -> Any:
    base = {"id": 7, "peer_id": peer, "from_id": None, "date": DATE, "edit_date": None, "text": "hi", "out": False,
            "post": False, "reply_to": None, "media": None, "fwd_from": None}
    return SimpleNamespace(**{**base, **kw})


def test_supergroup_message() -> None:
    ev = message_event("new", ME, msg(types.PeerChannel(1000000001), from_id=types.PeerUser(5001),
                                      reply_to=SimpleNamespace(reply_to_msg_id=3)), "Касса 1")
    assert (ev.chat_id, ev.chat_kind, ev.sender_id, ev.msg_id) == (-1001000000001, "supergroup", 5001, 7)
    assert ev.reply_to == 3 and not ev.anonymous and not ev.forwarded and ev.chat_title == "Касса 1"
    assert ev.date == DATE and ev.account_id == ME and ev.kind == "new"


def test_anonymous_admin_has_no_sender() -> None:
    for from_id in (None, types.PeerChannel(1000000001)):
        ev = message_event("new", ME, msg(types.PeerChannel(1000000001), from_id=from_id))
        assert ev.anonymous and ev.sender_id is None


def test_channel_post_and_basic_group() -> None:
    post = message_event("new", ME, msg(types.PeerChannel(1000000002), post=True))
    assert post.chat_kind == "channel" and post.sender_id is None and not post.anonymous
    group = message_event("new", ME, msg(types.PeerChat(55), from_id=types.PeerUser(9)))
    assert (group.chat_id, group.chat_kind, group.sender_id) == (-55, "group", 9)


def test_private_dialog_incoming_and_outgoing() -> None:
    incoming = message_event("new", ME, msg(types.PeerUser(7001)))
    assert (incoming.chat_id, incoming.chat_kind, incoming.sender_id) == (7001, "private", 7001)
    outgoing = message_event("new", ME, msg(types.PeerUser(7001), out=True))
    assert outgoing.sender_id == ME


def test_forwarded_message_belongs_to_the_forwarder_and_edit_keeps_dates() -> None:
    edited = datetime(2026, 11, 14, 10, 5, tzinfo=UTC)
    ev = message_event("edit", ME, msg(types.PeerChannel(1000000001), from_id=types.PeerUser(6001),
                                       fwd_from=SimpleNamespace(from_id=types.PeerUser(42)), edit_date=edited,
                                       text=None))
    assert ev.forwarded and ev.sender_id == 6001 and ev.edit_date == edited and ev.text == "" and ev.kind == "edit"


def test_media_type_and_reference() -> None:
    photo = types.MessageMediaPhoto(photo=SimpleNamespace(id=900))
    ev = message_event("new", ME, msg(types.PeerUser(7001), media=photo))
    assert (ev.media_type, ev.media_ref) == ("photo", "photo:900")
    assert message_event("new", ME, msg(types.PeerUser(7001))).media_type is None


def test_a_pdf_document_is_pdf_other_documents_stay_document() -> None:
    def doc(mime: str, name: str) -> Any:
        attrs = [types.DocumentAttributeFilename(file_name=name)]
        return types.MessageMediaDocument(document=SimpleNamespace(id=901, mime_type=mime, attributes=attrs))

    def media(m: Any) -> str | None:
        return message_event("new", ME, msg(types.PeerUser(7001), media=m)).media_type

    assert media(doc("application/pdf", "receipt.pdf")) == "pdf"
    assert media(doc("application/octet-stream", "Чек.PDF")) == "pdf"  # by the name when the MIME is generic
    assert media(doc("video/mp4", "IMG_1786.MP4")) == "document"


def test_error_codes() -> None:
    assert error_code(errors.PeerFloodError(request=None)) == "PEER_FLOOD"
    assert error_code(errors.ChatWriteForbiddenError(request=None)) == "CHAT_WRITE_FORBIDDEN"
    assert error_code(errors.UserBannedInChannelError(request=None)) == "USER_BANNED_IN_CHANNEL"
    assert error_code(errors.ChannelPrivateError(request=None)) == "CHANNEL_PRIVATE"
    assert error_code(errors.UserIsBlockedError(request=None)) == "USER_IS_BLOCKED"
    assert error_code(errors.UserDeactivatedBanError(request=None)) == "USER_DEACTIVATED_BAN"
    assert error_code(errors.AuthKeyUnregisteredError(request=None)) == "AUTH_KEY_UNREGISTERED"
    # A code Telethon has no class for is kept as the server sent it (FT-11.5).
    assert error_code(errors.BadRequestError(request=None, message="SOME_NEW_CODE")) == "SOME_NEW_CODE"


def test_sent_message_id() -> None:
    assert sent_message_id(types.UpdateShortSentMessage(id=15, pts=1, pts_count=1, date=DATE, out=True), 99) == 15
    updates = SimpleNamespace(
        updates=[types.UpdateMessageID(id=1, random_id=5), types.UpdateMessageID(id=16, random_id=99)])
    assert sent_message_id(updates, 99) == 16


def test_draft_of_a_private_dialog() -> None:
    """QuickCast2 (D-QK2-1): UpdateDraftMessage of a private dialog -> RawEvent "draft"; others skipped."""
    draft = types.DraftMessage(message="зд\\", date=DATE, reply_to=types.InputReplyToMessage(reply_to_msg_id=5))
    ev = draft_event(ME, types.UpdateDraftMessage(peer=types.PeerUser(5001), draft=draft))
    assert ev is not None
    assert (ev.kind, ev.chat_id, ev.chat_kind, ev.sender_id, ev.text, ev.reply_to) == (
        "draft", 5001, "private", ME, "зд\\", 5)
    empty = draft_event(ME, types.UpdateDraftMessage(peer=types.PeerUser(5001), draft=types.DraftMessageEmpty()))
    assert empty is not None and empty.text == ""
    assert draft_event(ME, types.UpdateDraftMessage(peer=types.PeerUser(ME), draft=draft)) is None  # Saved
    assert draft_event(ME, types.UpdateDraftMessage(peer=types.PeerChannel(1000000001), draft=draft)) is None
