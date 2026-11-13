"""The message record that travels through the pipeline after the buffer."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MessageRecord:
    trace_id: str
    ingest_id: int
    scope_id: int
    chat_id: int
    msg_id: int
    version: int
    msg_date: float
    account_id: int
    event: str
    source: str
    policy: str
    origin: str
    sender_id: int | None
    sender_role: str
    can_command: bool
    source_roles: list[str]
    src_priority: int
    reply_to: int | None
    text: str
    features: list[dict[str, Any]]
    config_version: str
    received_at: float
    # filled by the sorter
    msg_type: str | None = None
    fields: dict[str, Any] = field(default_factory=dict)
    order_key: str = ""
    priority: int = 5
    lane: int = 1

    @classmethod
    def from_payload(cls, p: dict[str, Any], ingest_id: int) -> MessageRecord:
        return cls(
            trace_id=p["trace_id"], ingest_id=ingest_id, scope_id=p["scope_id"], chat_id=p["chat_id"],
            msg_id=p["msg_id"], version=p["version"], msg_date=p["msg_date"], account_id=p["account_id"],
            event=p["event"], source=p["source"], policy=p["policy"], origin=p["origin"],
            sender_id=p["sender_id"], sender_role=p["sender_role"], can_command=p["can_command"],
            source_roles=p["source_roles"], src_priority=p["src_priority"], reply_to=p["reply_to"],
            text=p["text"] or "", features=p["features"] or [], config_version=p["config_version"],
            received_at=p["received_at"],
        )

    @classmethod
    def from_row(cls, r: Any) -> MessageRecord:
        feats = r["features"]
        fields = r["fields"]
        return cls(
            trace_id=str(r["trace_id"]), ingest_id=r["ingest_id"], scope_id=r["scope_id"], chat_id=r["chat_id"],
            msg_id=r["msg_id"], version=r["version"], msg_date=r["msg_date"].timestamp(),
            account_id=r["account_id"], event=r["event"], source=r["source"], policy=r["policy"],
            origin=r["origin"], sender_id=r["sender_id"], sender_role=r["sender_role"] or "user",
            can_command=r["can_command"], source_roles=list(r["source_roles"]), src_priority=r["src_priority"],
            reply_to=r["reply_to"], text=r["text"] or "",
            features=json.loads(feats) if isinstance(feats, str) else (feats or []),
            config_version=r["config_version"] or "", received_at=r["received_at"].timestamp(),
            msg_type=r["msg_type"],
            fields=json.loads(fields) if isinstance(fields, str) else (fields or {}),
            order_key=r["order_key"] or "", priority=r["priority"] if r["priority"] is not None else 5,
            lane=r["lane"] if r["lane"] is not None else 1,
        )
