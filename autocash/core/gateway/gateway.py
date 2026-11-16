"""Gateway: receives push updates of pool accounts and turns them into buffered records.

No Telegram API calls happen on this path (A-8): only the update itself and config are used.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from autocash.core.buffer.sqlite_buffer import SqliteBuffer
from autocash.core.config.loader import ConfigStore
from autocash.core.extractor.extractor import extract
from autocash.core.extractor.normalize import clean_markup, text_hash
from autocash.core.telegram.api import RawEvent

log = logging.getLogger(__name__)

SHARED_ID_KINDS = ("supergroup", "channel")


class OwnMessages:
    """Texts recently sent through Output: lets the Gateway tell origin=system from human."""

    def __init__(self, ttl_s: float = 120.0) -> None:
        self._ttl = ttl_s
        self._sent: dict[tuple[int, str], float] = {}
        self._delivered: OrderedDict[tuple[int, int], None] = OrderedDict()

    def register(self, chat_id: int, text: str) -> None:
        """Called right before a send: the push update may come before the answer to the send."""
        now = time.monotonic()
        if len(self._sent) > 2000:
            self._sent = {k: t for k, t in self._sent.items() if now - t < self._ttl}
        self._sent[(chat_id, text_hash(clean_markup(text)[0]))] = now

    def delivered(self, chat_id: int, msg_id: int) -> None:
        """Called with the id of a delivered message. Exact only where ids are shared by all
        accounts (supergroups, channels): there it also marks later edits of our message."""
        if chat_id <= -1_000_000_000_000:
            self._delivered[(chat_id, msg_id)] = None
            if len(self._delivered) > 20_000:
                self._delivered.popitem(last=False)

    def is_own(self, chat_id: int, hash_: str, msg_id: int | None = None) -> bool:
        if msg_id is not None and (chat_id, msg_id) in self._delivered:
            return True
        ts = self._sent.get((chat_id, hash_))
        return ts is not None and time.monotonic() - ts < self._ttl


class Gateway:
    def __init__(
        self,
        store: ConfigStore,
        buffer: SqliteBuffer,
        own: OwnMessages,
        notify_logs: Callable[[str], None],
    ) -> None:
        self._store = store
        self._buffer = buffer
        self._own = own
        self._notify_logs = notify_logs
        self.pool_user_ids: set[int] = set()
        self.presence: dict[int, dict[str, Any]] = {}
        self._announced: set[int] = set()
        self._last_hash: OrderedDict[tuple[int, int, int], tuple[str, int]] = OrderedDict()
        self.accepted = 0
        self.duplicates = 0
        self.rejected = 0
        self.ignored = 0

    def _remember(self, key: tuple[int, int, int], value: tuple[str, int]) -> None:
        self._last_hash[key] = value
        self._last_hash.move_to_end(key)
        if len(self._last_hash) > 50_000:
            self._last_hash.popitem(last=False)

    def _touch(self, ev: RawEvent, policy: str, known: bool, approx: bool) -> None:
        assert ev.chat_id is not None
        self.presence[ev.chat_id] = {
            "kind": ev.chat_kind, "title": ev.chat_title, "policy": policy, "known": known,
            "dedup_approx": approx, "last_seen": time.time(), "dirty": True,
        }

    async def on_event(self, ev: RawEvent) -> None:
        """Entry point for every update. Must not await before the record is enqueued."""
        received = time.time()
        rt = self._store.current
        trace_id = str(uuid.uuid4())

        if ev.kind in ("joined", "left"):
            return  # membership events are handled by the pool
        if ev.kind == "delete":
            scope = 0 if ev.chat_id is not None and ev.chat_kind in SHARED_ID_KINDS else ev.account_id
            if ev.chat_id is not None and rt.source(ev.chat_id).policy in ("ignore", "presence"):
                return
            rec: dict[str, Any] = {
                "kind": "delete", "ukey": f"d:{scope}:{ev.chat_id or 0}:{ev.msg_id}", "trace_id": trace_id,
                "scope_id": scope, "chat_id": ev.chat_id or 0, "msg_id": ev.msg_id, "account_id": ev.account_id,
                "received_at": received, "extracted_at": received, "config_version": rt.version,
                "src_priority": rt.source(ev.chat_id).cfg.priority if ev.chat_id is not None else 5,
            }
            await self._store_record(rec)
            return

        if ev.chat_id is None or ev.date is None:
            return
        src = rt.source(ev.chat_id)
        exact = ev.chat_kind in SHARED_ID_KINDS
        if src.policy == "ignore":
            self.ignored += 1
            return
        self._touch(ev, src.policy, src.known, approx=ev.chat_kind == "group")
        if not src.known and ev.chat_id not in self._announced:
            self._announced.add(ev.chat_id)
            self._notify_logs(
                f"Новый чат без роли: {ev.chat_title or ''} ({ev.chat_id}), политика приёма: {src.policy}"
            )
        if src.policy == "presence":
            return

        scope = 0 if exact else ev.account_id
        version = 0 if ev.kind == "new" else int((ev.edit_date or ev.date).timestamp())
        own_sender = ev.sender_id is not None and ev.sender_id in self.pool_user_ids
        full = src.policy == "full"
        text, hash_, features, is_command = "", "", [], False
        manifest_version = None
        if full:
            ex = extract(ev.text, rt.manifest)
            text, hash_, features = ex.text, ex.text_hash, ex.features
            manifest_version = rt.manifest.version
            key = (scope, ev.chat_id, ev.msg_id)
            last = self._last_hash.get(key)
            if ev.kind == "edit" and last is not None and last[0] == hash_ and last[1] != version:
                return  # a later edit that did not change the text: no new version (FT-5.2)
            # The same version arriving on another pool account passes: it gets a "seen" mark (FT-2.2).
            self._remember(key, (hash_, version))
            is_command = ex.command is not None and not ev.anonymous
        extracted = time.time()
        origin = "external"
        if own_sender:
            origin = "system" if self._own.is_own(ev.chat_id, hash_, ev.msg_id) else "human"
        date_ts = ev.date.timestamp()
        if exact or ev.chat_kind == "private":
            dedup_key, dedup_exact = f"{ev.chat_id}:{ev.msg_id}", True
        else:
            dedup_key, dedup_exact = f"{ev.sender_id}:{int(date_ts)}:{version}:{hash_[:16]}", False
        rec = {
            "kind": "message", "ukey": f"m:{ev.account_id}:{ev.chat_id}:{ev.msg_id}:{version}",
            "trace_id": trace_id, "scope_id": scope, "chat_id": ev.chat_id, "msg_id": ev.msg_id,
            "version": version, "msg_date": date_ts, "account_id": ev.account_id, "event": ev.kind,
            "source": ev.source, "policy": src.policy, "origin": origin, "sender_id": ev.sender_id,
            "sender_role": rt.sender_role(src, ev.sender_id, ev.anonymous), "can_command": not ev.anonymous,
            "source_roles": sorted(src.roles), "src_priority": src.cfg.priority,
            "edit_date": ev.edit_date.timestamp() if ev.edit_date else None, "received_at": received,
            "extracted_at": extracted, "reply_to": ev.reply_to, "media_type": ev.media_type,
            "media_ref": ev.media_ref, "text": text if full else None, "text_hash": hash_ if full else None,
            "features": features if full else None, "is_command": is_command,
            "manifest_version": manifest_version, "config_version": rt.version,
            "dedup_key": dedup_key, "dedup_exact": dedup_exact,
        }
        await self._store_record(rec)

    async def _store_record(self, rec: dict[str, Any]) -> None:
        result = await self._buffer.append(rec)
        if result > 0:
            self.accepted += 1
        elif result == 0:
            self.duplicates += 1  # the same update came again (push and getChannelDifference)
        else:
            self.rejected += 1
