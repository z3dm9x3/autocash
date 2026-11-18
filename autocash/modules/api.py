"""Module API (FT-8.1..8.3, 8.11).

A module declares what it subscribes to and handles events. Its writes and external actions
are collected in the Context and committed by the core in ONE transaction together with the
processing mark, so a repeated run of the same event changes nothing (FT-8.2).
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.core.record import MessageRecord
from autocash.core.storage.pg import Database
from autocash.core.telegram.api import RawEvent


@dataclass(frozen=True, slots=True)
class EntityDecl:
    type: str
    storage: str = "entities"  # shared entity store or a module-owned table


@dataclass(frozen=True, slots=True)
class Event:
    record: MessageRecord

    @property
    def trace_id(self) -> str:
        return self.record.trace_id

    @property
    def msg_type(self) -> str:
        return self.record.msg_type or ""

    @property
    def fields(self) -> dict[str, Any]:
        return self.record.fields

    @property
    def chat_id(self) -> int:
        return self.record.chat_id

    @property
    def external(self) -> bool:
        """Reactions fire only for senders outside the pool (FT-2.10)."""
        return self.record.origin == "external"


class Context:
    def __init__(self, event: Event, module: str, db: Database, config: dict[str, Any], config_version: str) -> None:
        self.event = event
        self.module = module
        self.config = config
        self.config_version = config_version
        self._db = db
        self.ops: list[tuple[str, tuple[Any, ...]]] = []
        self.drafts: list[OutboundDraft] = []
        self.final: tuple[str, str] | None = None

    def finish(self, status: str, reason: str) -> None:
        """End the task with a status other than "processed" (e.g. cancelled + ACCESS_DENIED)."""
        self.final = (str(status), str(reason))

    # ---- deferred writes: applied atomically with the processing mark -------------------
    def execute(self, sql: str, *args: Any) -> None:
        self.ops.append((sql, args))

    def upsert_entity(self, entity_type: str, key: str, *, state: str | None = None,
                      attrs: dict[str, Any] | None = None) -> None:
        self.execute(
            "INSERT INTO entities (entity_type, entity_key, state, attrs) VALUES ($1, $2, $3, $4::jsonb) "
            "ON CONFLICT (entity_type, entity_key) DO UPDATE SET state = coalesce(EXCLUDED.state, entities.state), "
            "attrs = entities.attrs || EXCLUDED.attrs, updated_at = now()",
            entity_type, key, state, json.dumps(attrs or {}, ensure_ascii=False),
        )

    # ---- external actions: become outbound tasks ----------------------------------------
    def send(self, chat_id: int, text: str, *, rule: str, kind: str = "notify", reply_to: int | None = None,
             rule_version: str | None = None, via_account: int | None = None) -> OutboundDraft:
        """`via_account`: the account of a private dialog; groups are routed by the Output."""
        draft = OutboundDraft(chat_id=chat_id, text=text, rule=rule, kind=kind, reply_to=reply_to,
                              rule_version=rule_version, via_account=via_account)
        self.drafts.append(draft)
        return draft

    def reply(self, text: str, *, rule: str, kind: str = "client_reply",
              rule_version: str | None = None) -> OutboundDraft:
        rec = self.event.record
        # In a private dialog the player is answered by the account they wrote to (FT-9.4).
        via = rec.account_id if rec.chat_id > 0 else None
        return self.send(rec.chat_id, text, rule=rule, kind=kind, reply_to=rec.msg_id, rule_version=rule_version,
                         via_account=via)

    # ---- reads ---------------------------------------------------------------------------
    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        rows: list[Any] = await self._db.pool.fetch(sql, *args)
        return rows

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        return await self._db.pool.fetchrow(sql, *args)


@dataclass(slots=True)
class Services:
    """What the core gives a module outside of event handling (bound by the App)."""

    db: Database
    reread: Callable[[int, list[int]], Awaitable[bool]]  # chat, message ids -> re-read through the pool
    notify: Callable[[str], None]  # a message to the staff group `logs`
    runtime: Callable[[], Any]  # the current configuration Runtime
    enqueue: Callable[[list[int]], None] = lambda ids: None  # hand outbound tasks written outside an event
    account_state: Callable[[int], str] = lambda account_id: "active"
    # (account, private chat, message id) -> had the dialog messages before this one? None: unknown.
    # One light getHistory through the limiter of that account (D-RM-11).
    dialog_history: Callable[[int, int, int], Awaitable[bool | None]] | None = None
    wake_protocols: Callable[[], None] = lambda: None  # after autocash.core.protocols.emit (D-RM-12)
    # A task for the Outbox outside event handling (protocols, background): (draft, module) -> task id.
    # The dedup key of the draft makes it once only (D-OBX-1).
    outbox: Callable[[OutboundDraft, str], Awaitable[int | None]] | None = None
    # (account, chat, since, limit) -> messages of a dialog, oldest first; a background read (D-AS-3).
    read_dialog: Callable[[int, int, datetime, int], Awaitable[list[RawEvent]]] | None = None


TraceResolver = Callable[[str], Awaitable[list[str]]]


class Module(ABC):
    name: ClassVar[str]
    subscriptions: ClassVar[list[str]] = []  # message types, masks allowed: "deposit.*", "*"
    entities: ClassVar[list[EntityDecl]] = []
    commands: ClassVar[list[str]] = []
    # Commands also accepted from groups that are not read (policy presence/meta), e.g. /rm ? (D-RM-4).
    commands_any_chat: ClassVar[list[str]] = []
    # Every message of private dialogs, whatever its type (also untyped): Role Manager (D-RM-11).
    private_dialogs: ClassVar[bool] = False
    # Protocols this module takes part in (D-RM-12); it gets them in `on_protocol`.
    protocols: ClassVar[list[str]] = []
    config_model: ClassVar[type[BaseModel] | None] = None
    concurrency: ClassVar[int] = 16
    timeout_s: ClassVar[float] = 5.0
    periodic_s: ClassVar[float | None] = None  # run `periodic()` this often, if set
    migrations: ClassVar[Path | None] = None  # the module's own SQL migrations (P-1: not in the core)
    services: Services | None = None

    def bind(self, services: Services) -> None:
        self.services = services

    async def on_protocol(self, protocol: str, subject: int, data: dict[str, Any]) -> None:  # noqa: B027
        """A protocol run (D-RM-12); must be idempotent: it may come again after a crash."""

    async def start(self) -> None:  # noqa: B027 - optional hook, a no-op unless the module needs it
        """Called once at start, after migrations and before any event reaches the module."""

    async def periodic(self) -> None:  # noqa: B027 - optional hook, a no-op unless the module needs it
        """Periodic work of the module (sweeps, reconciliation); runs outside event handling."""

    def wants(self, record: MessageRecord) -> bool:
        """Last filter of delivery; for "unknown" a module gets the message only if it wants it."""
        return record.msg_type != "unknown"

    def trace_resolver(self) -> TraceResolver | None:
        """Turns a module key (e.g. an entity id) into trace ids for /trace."""
        return None

    @abstractmethod
    async def handle(self, event: Event, ctx: Context) -> None:
        """Process one event. Must be idempotent; all effects go through `ctx`."""

    async def reconcile(self, chat_id: int) -> bool:
        """Hook: re-read open entities of a source from history after a gap (FT-2.7).

        Modules implement it in stage 2. Return True when the source was reconciled.
        """
        return False
