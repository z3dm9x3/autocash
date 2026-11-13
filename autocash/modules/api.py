"""Module API (FT-8.1..8.3, 8.11).

A module declares what it subscribes to and handles events. Its writes and external actions
are collected in the Context and committed by the core in ONE transaction together with the
processing mark, so a repeated run of the same event changes nothing (FT-8.2).
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.actions.send import OutboundDraft
from autocash.core.record import MessageRecord
from autocash.core.storage.pg import Database


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
             rule_version: str | None = None) -> OutboundDraft:
        draft = OutboundDraft(chat_id=chat_id, text=text, rule=rule, kind=kind, reply_to=reply_to,
                              rule_version=rule_version)
        self.drafts.append(draft)
        return draft

    def reply(self, text: str, *, rule: str, kind: str = "client_reply",
              rule_version: str | None = None) -> OutboundDraft:
        rec = self.event.record
        return self.send(rec.chat_id, text, rule=rule, kind=kind, reply_to=rec.msg_id, rule_version=rule_version)

    # ---- reads ---------------------------------------------------------------------------
    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        rows: list[Any] = await self._db.pool.fetch(sql, *args)
        return rows

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        return await self._db.pool.fetchrow(sql, *args)


class Module(ABC):
    name: ClassVar[str]
    subscriptions: ClassVar[list[str]] = []  # message types, masks allowed: "deposit.*", "*"
    entities: ClassVar[list[EntityDecl]] = []
    commands: ClassVar[list[str]] = []
    config_model: ClassVar[type[BaseModel] | None] = None
    concurrency: ClassVar[int] = 16
    timeout_s: ClassVar[float] = 5.0

    @abstractmethod
    async def handle(self, event: Event, ctx: Context) -> None:
        """Process one event. Must be idempotent; all effects go through `ctx`."""
