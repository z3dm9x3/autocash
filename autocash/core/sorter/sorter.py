"""Sorter service: types committed batches in memory (A-2), reads PostgreSQL only to recover."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

import asyncpg

from autocash.core.config.loader import ConfigStore
from autocash.core.record import MessageRecord
from autocash.core.sorter.rules import UNKNOWN, build_view, classify
from autocash.core.storage.pg import Database
from autocash.core.storage.writer import BatchWriter
from autocash.core.trace.codes import Reason, Status

log = logging.getLogger(__name__)

_UPDATE_MESSAGES = """
UPDATE messages m SET msg_type = x.msg_type, fields = x.fields, order_key = x.order_key,
       priority = x.priority, lane = x.lane, rule_id = x.rule_id, sorted_at = now(),
       text_expires_at = now() + make_interval(days => x.ttl_days)
FROM jsonb_to_recordset($1::jsonb) AS x(ingest_id bigint, msg_type text, fields jsonb, order_key text,
                                        priority int, lane int, rule_id text, ttl_days int)
WHERE m.ingest_id = x.ingest_id
"""

_UPDATE_TRACE = """
UPDATE trace_inbound t SET sorted_at = now(), msg_type = x.msg_type, status = x.status,
       reason_code = x.reason, subscribers = x.subs,
       finished_at = CASE WHEN x.status = 'in_progress' THEN NULL ELSE now() END
FROM jsonb_to_recordset($1::jsonb) AS x(trace_id uuid, msg_type text, status text, reason text, subs int)
WHERE t.trace_id = x.trace_id
"""

_UNKNOWN_COUNT = """
INSERT INTO unknown_counters (chat_id, day, count) VALUES ($1, (now() AT TIME ZONE 'UTC')::date, $2)
ON CONFLICT (chat_id, day) DO UPDATE SET count = unknown_counters.count + EXCLUDED.count
"""

_RECOVER = """
SELECT * FROM messages
WHERE sorted_at IS NULL AND policy = 'full' AND ingest_id > $1 AND ($2::bigint IS NULL OR ingest_id <= $2)
ORDER BY ingest_id LIMIT $3
"""


@dataclass(slots=True)
class _Recover:
    bound: int | None
    done: asyncio.Future[int]


class Sorter:
    def __init__(
        self,
        store: ConfigStore,
        db: Database,
        writer: BatchWriter,
        dispatch: Callable[[MessageRecord], None],
        subscribers: Callable[[str, int], int],
        notify_debug: Callable[[str], None],
    ) -> None:
        self._store = store
        self._db = db
        self._writer = writer
        self._dispatch = dispatch
        self._subscribers = subscribers
        self._notify_debug = notify_debug
        self._queue: asyncio.Queue[list[MessageRecord] | _Recover] = asyncio.Queue(
            maxsize=store.current.config.sorter.queue_batches
        )
        self._tasks: list[asyncio.Task[None]] = []
        self.unknown: Counter[int] = Counter()
        self._unknown_reported: Counter[int] = Counter()
        self.sorted = 0
        self.busy = False

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._run(), name="sorter"),
            asyncio.create_task(self._summary_loop(), name="sorter-unknown-summary"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def submit(self, records: list[MessageRecord]) -> None:
        await self._queue.put(records)

    async def recover(self, bound: int | None = None) -> int:
        """Sort messages stored in PostgreSQL but never sorted (restart, lost hand-over)."""
        fut: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        await self._queue.put(_Recover(bound, fut))
        return await fut

    async def recover_nowait(self, bound: int) -> None:
        fut: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        await self._queue.put(_Recover(bound, fut))

    async def _run(self) -> None:
        while True:
            item = await self._queue.get()
            self.busy = True
            try:
                if isinstance(item, _Recover):
                    count = await self._recover(item.bound)
                    if not item.done.done():
                        item.done.set_result(count)
                else:
                    await self._sort(item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("sorter: batch failed")
                if isinstance(item, _Recover) and not item.done.done():
                    item.done.set_exception(exc)
            finally:
                self.busy = False

    async def _recover(self, bound: int | None) -> int:
        total, last = 0, 0
        while True:
            await self._db.wait_available()
            rows = await self._db.pool.fetch(_RECOVER, last, bound, 500)
            if not rows:
                return total
            records = [MessageRecord.from_row(r) for r in rows]
            last = records[-1].ingest_id
            await self._sort(records)
            total += len(records)

    async def _sort(self, records: list[MessageRecord]) -> None:
        rt = self._store.current
        default_ttl = rt.config.retention.default_text_ttl_days
        updates, traces = [], []
        unknown: Counter[int] = Counter()
        for rec in records:
            src = rt.source(rec.chat_id)
            view = build_view(
                text=rec.text, features=rec.features, event=rec.event, origin=rec.origin,
                source_roles=frozenset(rec.source_roles), sender_role=rec.sender_role,
                can_command=rec.can_command,
            )
            res = classify(src.rules, view, chat_id=rec.chat_id, source=rec.source,
                           source_priority=rec.src_priority)
            rec.msg_type, rec.fields, rec.order_key = res.msg_type, res.fields, res.order_key
            rec.priority, rec.lane = res.priority, res.lane
            updates.append({
                "ingest_id": rec.ingest_id, "msg_type": res.msg_type, "fields": res.fields,
                "order_key": res.order_key, "priority": res.priority, "lane": res.lane, "rule_id": res.rule_id,
                "ttl_days": res.text_ttl_days if res.text_ttl_days is not None else default_ttl,
            })
            subs = self._subscribers(res.msg_type, rec.chat_id)
            if res.msg_type == UNKNOWN:
                unknown[rec.chat_id] += 1
                status, reason = Status.SKIPPED, Reason.UNKNOWN_TYPE
            elif subs == 0:
                status, reason = Status.PROCESSED, None
            else:
                status, reason = Status.IN_PROGRESS, None
            traces.append({"trace_id": rec.trace_id, "msg_type": res.msg_type, "status": status,
                           "reason": reason, "subs": subs})

        async def write(conn: asyncpg.Connection) -> None:
            await conn.execute(_UPDATE_MESSAGES, json.dumps(updates, ensure_ascii=False))
            await conn.execute(_UPDATE_TRACE, json.dumps(traces))
            for chat_id, count in unknown.items():
                await conn.execute(_UNKNOWN_COUNT, chat_id, count)

        # The result is committed before modules see the message (FT-6.5), so a module queue
        # can always be rebuilt from PostgreSQL.
        await self._writer.submit_sync(write)
        self.unknown.update(unknown)
        self.sorted += len(records)
        for rec in records:
            self._dispatch(rec)

    def unknown_summary(self) -> str | None:
        fresh = self.unknown - self._unknown_reported
        if not fresh:
            return None
        self._unknown_reported = Counter(self.unknown)
        lines = [f"{chat_id}: {count}" for chat_id, count in fresh.most_common(30)]
        return "Сообщения без подходящего правила (unknown) по источникам:\n" + "\n".join(lines)

    async def _summary_loop(self) -> None:
        while True:
            await asyncio.sleep(self._store.current.config.sorter.unknown_summary_interval_s)
            text = self.unknown_summary()
            if text:
                self._notify_debug(text)
