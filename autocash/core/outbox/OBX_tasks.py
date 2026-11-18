"""Outbox tasks: how a module asks the Outbox to write to Telegram (FT-8.4, D-OBX-1).

A module never writes to Telegram itself: it describes a task (`OutboundDraft`) and the Outbox
sends it. Inside event handling the task is inserted in the same transaction as the module's
processing mark, with the unique key "event + rule" (FT-8.3); outside it (protocols, background
work) through `Services.outbox`, with a dedup key of its own.
"""

from __future__ import annotations

import secrets
import uuid
from dataclasses import dataclass, field

import asyncpg

# Priority of a task: lower goes first. For now every task has the middle priority 5
# (customer, 2026-11-18); the kinds stay for the lifetime of a task (ttl_s) and the API budget.
KINDS = ("command_reply", "client_reply", "notify", "bulk")
DEFAULT_PRIORITY = 5
KIND_PRIORITY = dict.fromkeys(KINDS, DEFAULT_PRIORITY)
ACTIONS = ("send", "pin")

_INSERT = """
INSERT INTO outbound_tasks (trace_id, parent_trace_id, dedup_key, module, rule, rule_version, kind, priority,
    chat_id, reply_to, text, random_id, queued_at, expires_at, config_version, dialog_account_id,
    action, pin_msg_id, pin_after_send, target_type, binding)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(),
        now() + make_interval(secs => $13::float8), $14, $15, $16, $17, $18, $19, $20)
ON CONFLICT (dedup_key) DO NOTHING
RETURNING id
"""


@dataclass(slots=True)
class OutboundDraft:
    chat_id: int
    text: str
    rule: str
    kind: str = "notify"
    reply_to: int | None = None
    rule_version: str | None = None
    dedup_key: str | None = None
    # The account that must send the task. In a private dialog: the account of that dialog
    # (only it may answer there, FT-9.4). Elsewhere: an account the module names explicitly.
    via_account: int | None = None
    action: str = "send"  # send / pin
    pin_msg_id: int | None = None  # action pin: the message to pin
    pin_after_send: bool = False  # send, then pin the sent message (a follow-up pin task)
    priority: int = DEFAULT_PRIORITY
    # Generated before sending and stored with the task (invariant 3).
    random_id: int = field(default_factory=lambda: secrets.randbits(63) or 1)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def binding(self) -> str:
        if self.via_account is None:
            return "none"
        return "dialog" if self.chat_id > 0 else "explicit"


async def insert_outbound(
    conn: asyncpg.Connection,
    draft: OutboundDraft,
    *,
    parent_trace_id: str | None,
    module: str | None,
    ttl_s: float,
    config_version: str,
) -> int | None:
    """Insert the task; returns its id, or None when the same task (dedup key) exists."""
    if draft.kind not in KINDS:
        raise ValueError(f"unknown outbound kind {draft.kind!r}")
    if draft.action not in ACTIONS:
        raise ValueError(f"unknown outbound action {draft.action!r}")
    if draft.action == "pin" and draft.pin_msg_id is None:
        raise ValueError("an outbound pin needs pin_msg_id")
    dedup = draft.dedup_key or f"{parent_trace_id}:{module}:{draft.rule}"
    task_id: int | None = await conn.fetchval(
        _INSERT, draft.trace_id, parent_trace_id, dedup, module, draft.rule, draft.rule_version, draft.kind,
        draft.priority, draft.chat_id, draft.reply_to, draft.text, draft.random_id, ttl_s,
        config_version, draft.via_account, draft.action, draft.pin_msg_id, draft.pin_after_send,
        "user" if draft.chat_id > 0 else "group", draft.binding,
    )
    return task_id
