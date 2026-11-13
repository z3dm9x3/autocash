"""Fundamental actions `send` / `reply`: create an outbound task for the Output (FT-8.4).

The task is only described here; it is inserted in the same transaction as the module's
processing mark, with the unique key "event + rule" (FT-8.3).
"""

from __future__ import annotations

import secrets
import uuid
from dataclasses import dataclass, field

import asyncpg

# Order of sending (FT-9.8): command replies, client replies, notifications, bulk.
KIND_PRIORITY = {"command_reply": 0, "client_reply": 1, "notify": 2, "bulk": 3}

_INSERT = """
INSERT INTO outbound_tasks (trace_id, parent_trace_id, dedup_key, module, rule, rule_version, kind, priority,
    chat_id, reply_to, text, random_id, queued_at, expires_at, config_version)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(),
        now() + make_interval(secs => $13::float8), $14)
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
    # Generated before sending and stored with the task (invariant 3).
    random_id: int = field(default_factory=lambda: secrets.randbits(63) or 1)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))


async def insert_outbound(
    conn: asyncpg.Connection,
    draft: OutboundDraft,
    *,
    parent_trace_id: str | None,
    module: str | None,
    ttl_s: float,
    config_version: str,
) -> int | None:
    """Insert the task; returns its id, or None when the same "event + rule" task exists."""
    if draft.kind not in KIND_PRIORITY:
        raise ValueError(f"unknown outbound kind {draft.kind!r}")
    dedup = draft.dedup_key or f"{parent_trace_id}:{module}:{draft.rule}"
    task_id: int | None = await conn.fetchval(
        _INSERT, draft.trace_id, parent_trace_id, dedup, module, draft.rule, draft.rule_version, draft.kind,
        KIND_PRIORITY[draft.kind], draft.chat_id, draft.reply_to, draft.text, draft.random_id, ttl_s,
        config_version,
    )
    return task_id
