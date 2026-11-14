"""Uploader: SQLite buffer -> PostgreSQL in batches, one transaction per batch (A-1, FT-4.2..4.4).

After the commit the batch is handed to the sorter in memory (A-2). A batch containing a
command is sent immediately (A-5). While PostgreSQL is down, intake continues and pending
events are marked as waiting with DB_UNAVAILABLE (item 3.3).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg

from autocash.core import crashpoints
from autocash.core.buffer.sqlite_buffer import BufferRow, SqliteBuffer
from autocash.core.config.loader import ConfigStore
from autocash.core.record import MessageRecord
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.trace.codes import Reason

log = logging.getLogger(__name__)

_INSERT_MESSAGES = """
INSERT INTO messages (
    scope_id, chat_id, msg_id, version, msg_date, trace_id, account_id, event, source, policy, origin,
    sender_id, sender_role, can_command, source_roles, src_priority, edit_date, received_at, reply_to,
    media_type, media_ref, text, text_hash, features, is_command, manifest_version, config_version,
    dedup_key, dedup_exact, sorted_at)
SELECT (e->>'scope_id')::bigint, (e->>'chat_id')::bigint, (e->>'msg_id')::bigint, (e->>'version')::bigint,
       to_timestamp((e->>'msg_date')::float8), (e->>'trace_id')::uuid, (e->>'account_id')::bigint,
       e->>'event', e->>'source', e->>'policy', e->>'origin', (e->>'sender_id')::bigint, e->>'sender_role',
       (e->>'can_command')::boolean,
       ARRAY(SELECT jsonb_array_elements_text(e->'source_roles')), (e->>'src_priority')::int,
       to_timestamp((e->>'edit_date')::float8), to_timestamp((e->>'received_at')::float8),
       (e->>'reply_to')::bigint, e->>'media_type', e->>'media_ref', e->>'text', e->>'text_hash',
       NULLIF(e->'features', 'null'::jsonb), (e->>'is_command')::boolean, e->>'manifest_version',
       e->>'config_version', e->>'dedup_key', (e->>'dedup_exact')::boolean,
       CASE WHEN e->>'policy' = 'full' THEN NULL ELSE now() END
FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS t(e, ord)
ORDER BY ord
ON CONFLICT (scope_id, chat_id, msg_id, version, msg_date) DO NOTHING
RETURNING trace_id::text, ingest_id
"""

_INSERT_SEEN = """
INSERT INTO message_seen (scope_id, chat_id, msg_id, version, account_id, seen_at)
SELECT (e->>'scope_id')::bigint, (e->>'chat_id')::bigint, (e->>'msg_id')::bigint, (e->>'version')::bigint,
       (e->>'account_id')::bigint, to_timestamp((e->>'received_at')::float8)
FROM jsonb_array_elements($1::jsonb) AS t(e)
ON CONFLICT DO NOTHING
"""

_INSERT_TRACE = """
INSERT INTO trace_inbound (trace_id, ingest_id, account_id, chat_id, msg_id, version, event, accepted_at,
    extracted_at, buffered_at, in_pg_at, status, finished_at, waited_reason, config_version)
SELECT (e->>'trace_id')::uuid, (e->>'ingest_id')::bigint, (e->>'account_id')::bigint, (e->>'chat_id')::bigint,
       (e->>'msg_id')::bigint, (e->>'version')::bigint, e->>'event', to_timestamp((e->>'received_at')::float8),
       to_timestamp((e->>'extracted_at')::float8), to_timestamp((e->>'buffered_at')::float8), now(),
       e->>'status', CASE WHEN e->>'status' = 'in_progress' THEN NULL ELSE now() END,
       e->>'waited_reason', e->>'config_version'
FROM jsonb_array_elements($1::jsonb) AS t(e)
ON CONFLICT (trace_id) DO NOTHING
"""

SortSink = Callable[[list[MessageRecord]], Awaitable[None]]
RecoverFn = Callable[[int], Awaitable[None]]


class Uploader:
    def __init__(
        self, store: ConfigStore, buffer: SqliteBuffer, db: Database, sink: SortSink, recover: RecoverFn
    ) -> None:
        self._store = store
        self._buffer = buffer
        self._db = db
        self._sink = sink
        self._recover = recover
        self._task: asyncio.Task[None] | None = None
        self.uploaded = 0
        self.batches = 0
        self.outage = False
        self.busy = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="uploader")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        buf = self._buffer
        buf.pending_event.set()  # upload leftovers of the previous run first
        while True:
            await buf.pending_event.wait()
            cfg = self._store.current.config.uploader
            if not buf.urgent_event.is_set():
                # Collect a batch: by timer or by count, whichever comes first (A-1).
                try:
                    await asyncio.wait_for(buf.urgent_event.wait(), cfg.batch_interval_ms / 1000)
                except TimeoutError:
                    pass
            buf.urgent_event.clear()
            buf.pending_event.clear()
            while True:
                self.busy = True
                try:
                    rows = await buf.fetch_pending(cfg.batch_max)
                    if not rows:
                        break
                    await self._upload_with_retry(rows)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("uploader: unexpected failure")
                    await asyncio.sleep(0.5)
                    continue
                finally:
                    self.busy = False
                if len(rows) < cfg.batch_max:
                    break

    async def _upload_with_retry(self, rows: list[BufferRow]) -> None:
        had_outage = False
        while True:
            try:
                records = await self._upload(rows)
                break
            except Exception as exc:
                if not is_connection_error(exc):
                    log.exception("uploader: batch failed, retrying")
                    await asyncio.sleep(1.0)
                    continue
                self._db.mark_down(exc)
                self.outage = had_outage = True
                await self._buffer.set_wait_reason(Reason.DB_UNAVAILABLE)
                retry_max = self._store.current.config.uploader.retry_max_s
                waiter = asyncio.create_task(self._db.wait_available(max_retry_s=retry_max))
                # Keep marking newly accepted events as waiting while the database is down.
                while not waiter.done():
                    await asyncio.wait({waiter}, timeout=0.5)
                    if not waiter.done():
                        await self._buffer.set_wait_reason(Reason.DB_UNAVAILABLE)
                await waiter
                self._db.forget_partitions()
                rows = await self._refetch(rows)
        self.outage = False
        crashpoints.hit("uploader_after_pg_commit")
        await self._buffer.mark_uploaded([r.seq for r in rows])
        self.uploaded += len(rows)
        self.batches += 1
        if records:
            await self._sink(records)
        if had_outage:
            # A commit whose outcome was lost may have stored rows that were never handed over:
            # everything up to the current maximum is either handed already or must be re-read.
            bound = await self._db.pool.fetchval("SELECT coalesce(max(ingest_id), 0) FROM messages")
            await self._recover(int(bound))

    async def _refetch(self, rows: list[BufferRow]) -> list[BufferRow]:
        """Re-read the same rows to pick up wait_reason set during the outage."""
        fresh = await self._buffer.fetch_pending(len(rows))
        by_seq = {r.seq: r for r in fresh}
        return [by_seq.get(r.seq, r) for r in rows]

    async def _upload(self, rows: list[BufferRow]) -> list[MessageRecord]:
        messages = [r for r in rows if r.payload["kind"] == "message"]
        deletions = [r for r in rows if r.payload["kind"] == "delete"]
        records: list[MessageRecord] = []
        async with self._db.pool.acquire() as conn, conn.transaction():
            if messages:
                records = await self._upload_messages(conn, messages)
            for row in deletions:
                await self._upload_deletion(conn, row)
            crashpoints.hit("uploader_before_pg_commit")
        return records

    async def _upload_messages(self, conn: asyncpg.Connection, rows: list[BufferRow]) -> list[MessageRecord]:
        await self._db.ensure_partitions(conn, {r.payload["msg_date"] for r in rows})
        payloads: list[dict[str, Any]] = []
        seen_only: list[dict[str, Any]] = []
        for row in rows:
            p = row.payload
            p["buffered_at"] = row.buffered_at
            p["waited_reason"] = row.wait_reason
            if not p["dedup_exact"]:
                # Basic groups: message ids differ per account -> approximate dedup (FT-2.11).
                orig = await conn.fetchrow(
                    "SELECT scope_id, msg_id, version FROM messages "
                    "WHERE chat_id = $1 AND dedup_key = $2 AND NOT dedup_exact AND scope_id <> $3 LIMIT 1",
                    p["chat_id"], p["dedup_key"], p["scope_id"],
                )
                if orig is not None:
                    seen_only.append({**p, "scope_id": orig["scope_id"], "msg_id": orig["msg_id"],
                                      "version": orig["version"]})
                    continue
            payloads.append(p)
        inserted: dict[str, int] = {}
        if payloads:
            for rec in await conn.fetch(_INSERT_MESSAGES, json.dumps(payloads, ensure_ascii=False)):
                inserted[rec["trace_id"]] = rec["ingest_id"]
        # Only the key columns travel here: the full payload (text, features) is sent once.
        seen = [{k: p[k] for k in _SEEN_FIELDS} for p in (*payloads, *seen_only)]
        await conn.execute(_INSERT_SEEN, json.dumps(seen))
        traces = []
        records: list[MessageRecord] = []
        for p in payloads:
            ingest_id = inserted.get(p["trace_id"])
            if ingest_id is None:
                continue  # already stored: by another account or by a previous upload of this batch
            full = p["policy"] == "full"
            traces.append({**_trace_fields(p), "ingest_id": ingest_id,
                           "status": "in_progress" if full else "processed"})
            if full:
                records.append(MessageRecord.from_payload(p, ingest_id))
        if traces:
            await conn.execute(_INSERT_TRACE, json.dumps(traces))
        return records

    async def _upload_deletion(self, conn: asyncpg.Connection, row: BufferRow) -> None:
        p = row.payload
        fresh = await conn.fetchval(
            "INSERT INTO message_deletions (scope_id, chat_id, msg_id, account_id, trace_id, deleted_at) "
            "VALUES ($1, $2, $3, $4, $5::uuid, to_timestamp($6::float8)) ON CONFLICT DO NOTHING RETURNING 1",
            p["scope_id"], p["chat_id"], p["msg_id"], p["account_id"], p["trace_id"], p["received_at"],
        )
        if not fresh:
            return
        await conn.execute(
            "UPDATE messages SET deleted_at = to_timestamp($4::float8) "
            "WHERE scope_id = $1 AND msg_id = $2 AND ($3::bigint = 0 OR chat_id = $3::bigint) AND deleted_at IS NULL",
            p["scope_id"], p["msg_id"], p["chat_id"], p["received_at"],
        )
        trace = {**_trace_fields(p), "version": None, "event": "delete", "ingest_id": None,
                 "buffered_at": row.buffered_at, "waited_reason": row.wait_reason, "status": "processed"}
        await conn.execute(_INSERT_TRACE, json.dumps([trace]))


_SEEN_FIELDS = ("scope_id", "chat_id", "msg_id", "version", "account_id", "received_at")


def _trace_fields(p: dict[str, Any]) -> dict[str, Any]:
    return {k: p.get(k) for k in ("trace_id", "account_id", "chat_id", "msg_id", "version", "event", "received_at",
                                  "extracted_at", "buffered_at", "waited_reason", "config_version")}
