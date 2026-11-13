"""Queue manager (FT-7.1..7.7, A-3): per-module in-memory queues over PostgreSQL.

Each module has its own bounded queue, concurrency limit and workers, so a slow module only
delays itself (P-4). A queue is a window over PostgreSQL: when it is full, new messages are
not held in memory and not blocked on - they are re-read from PostgreSQL later (refill).
"""

from __future__ import annotations

import asyncio
import heapq
import json
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any

import asyncpg

from autocash.actions.send import OutboundDraft, insert_outbound
from autocash.core import crashpoints
from autocash.core.config.loader import ConfigStore
from autocash.core.record import MessageRecord
from autocash.core.sorter.rules import UNKNOWN
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.storage.writer import Aggregator, BatchWriter
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, Event, Module

log = logging.getLogger(__name__)

LANE_COMMANDS, LANE_LIVE, LANE_BACKFILL = 0, 1, 2

_REFILL = """
SELECT m.* FROM messages m
WHERE m.ingest_id > $1 AND m.sorted_at IS NOT NULL AND m.policy = 'full' AND m.msg_type <> 'unknown'
  AND NOT EXISTS (SELECT 1 FROM trace_module t
                  WHERE t.trace_id = m.trace_id AND t.module = $2 AND t.finished_at IS NOT NULL)
ORDER BY m.ingest_id LIMIT $3
"""

_QUEUED = """
INSERT INTO trace_module (trace_id, module, ingest_id, queued_at)
SELECT x.trace_id, x.module, x.ingest_id, to_timestamp(x.queued_at)
FROM jsonb_to_recordset($1::jsonb) AS x(trace_id uuid, module text, ingest_id bigint, queued_at float8)
ON CONFLICT (trace_id, module) DO NOTHING
"""

_DONE = """
INSERT INTO trace_module (trace_id, module, ingest_id, queued_at, started_at, finished_at, status)
SELECT x.trace_id, x.module, x.ingest_id, to_timestamp(x.queued_at), to_timestamp(x.started_at), now(), 'processed'
FROM jsonb_to_recordset($1::jsonb)
     AS x(trace_id uuid, module text, ingest_id bigint, queued_at float8, started_at float8)
ON CONFLICT (trace_id, module) DO UPDATE
    SET started_at = EXCLUDED.started_at, finished_at = now(), status = 'processed', reason_code = NULL, error = NULL
    WHERE trace_module.finished_at IS NULL
RETURNING trace_id::text AS trace_id, module
"""

_FAILED = """
INSERT INTO trace_module (trace_id, module, ingest_id, queued_at, started_at, finished_at, status, reason_code, error)
VALUES ($1::uuid, $2, $3, to_timestamp($4::float8), to_timestamp($5::float8), now(), 'waiting', $6, $7)
ON CONFLICT (trace_id, module) DO UPDATE
    SET started_at = EXCLUDED.started_at, finished_at = now(), status = 'waiting',
        reason_code = EXCLUDED.reason_code, error = EXCLUDED.error
    WHERE trace_module.finished_at IS NULL
"""

_INBOUND_DONE = """
UPDATE trace_inbound t SET status = 'processed', finished_at = now()
WHERE t.trace_id = ANY($1::uuid[]) AND t.status = 'in_progress'
  AND (SELECT count(*) FROM trace_module m
       WHERE m.trace_id = t.trace_id AND m.status = 'processed') >= coalesce(t.subscribers, 1)
"""


class ModuleQueue:
    """Three lanes, per-key sequential order. Order by key outranks the lanes (FT-7.5)."""

    def __init__(self, name: str, maxsize: int, live_threshold: int, backfill_rate: float) -> None:
        self.name = name
        self.maxsize = maxsize
        self.live_threshold = live_threshold
        self.backfill_rate = backfill_rate
        self._keys: dict[str, deque[MessageRecord]] = {}
        self._klanes: dict[str, list[int]] = {}
        self._running: set[str] = set()
        self._heap: list[tuple[int, int, int, str]] = []
        self.size = 0
        self.lane_counts = [0, 0, 0]
        self.pending: dict[int, float] = {}  # ingest_id -> enqueue time (queued or running)
        self.wake = asyncio.Event()
        # Window over PostgreSQL: starts in refill mode to rebuild after a restart (FT-7.7).
        self.overflow = True
        self.cursor = 0
        self.dropped_low: int | None = None
        self.max_seen = 0
        self._tokens = 1.0
        self._tokens_at = time.monotonic()
        self.next_retry = 0.0

    def _push(self, key: str) -> None:
        if key in self._running:
            return
        items = self._keys.get(key)
        if not items:
            return
        lanes = self._klanes[key]
        eff = 0 if lanes[0] else (1 if lanes[1] else 2)
        heapq.heappush(self._heap, (eff, items[0].priority, items[0].ingest_id, key))

    def _enqueue(self, rec: MessageRecord) -> None:
        key = rec.order_key
        self._keys.setdefault(key, deque()).append(rec)
        self._klanes.setdefault(key, [0, 0, 0])[rec.lane] += 1
        self.size += 1
        self.lane_counts[rec.lane] += 1
        self.pending[rec.ingest_id] = time.monotonic()
        self._push(key)
        self.wake.set()

    def note_seen(self, ingest_id: int) -> None:
        if ingest_id > self.max_seen:
            self.max_seen = ingest_id

    def offer(self, rec: MessageRecord) -> bool:
        """Called by the sorter. Never blocks: a full queue drops to PostgreSQL."""
        if rec.ingest_id in self.pending:
            return True
        if self.overflow or self.size >= self.maxsize:
            self.overflow = True
            if self.dropped_low is None or rec.ingest_id < self.dropped_low:
                self.dropped_low = rec.ingest_id
            return False
        self.note_seen(rec.ingest_id)
        self._enqueue(rec)
        return True

    def force(self, rec: MessageRecord) -> None:
        """Used by refill: enqueue a record read from PostgreSQL."""
        if rec.ingest_id not in self.pending:
            self._enqueue(rec)

    def _backfill_allowed(self) -> bool:
        if self.lane_counts[LANE_LIVE] >= self.live_threshold:
            self.next_retry = time.monotonic() + 0.05
            return False
        now = time.monotonic()
        self._tokens = min(max(self.backfill_rate, 1.0), self._tokens + (now - self._tokens_at) * self.backfill_rate)
        self._tokens_at = now
        if self._tokens < 1.0:
            self.next_retry = now + (1.0 - self._tokens) / self.backfill_rate
            return False
        self._tokens -= 1.0
        return True

    def take(self) -> MessageRecord | None:
        while self._heap:
            eff, prio, seq, key = self._heap[0]
            items = self._keys.get(key)
            lanes = self._klanes.get(key)
            if key in self._running or not items or lanes is None:
                heapq.heappop(self._heap)
                continue
            cur_eff = 0 if lanes[0] else (1 if lanes[1] else 2)
            if (cur_eff, items[0].priority, items[0].ingest_id) != (eff, prio, seq):
                heapq.heappop(self._heap)  # stale entry: the key was promoted or its head changed
                continue
            if eff == LANE_BACKFILL and not self._backfill_allowed():
                return None
            heapq.heappop(self._heap)
            rec = items.popleft()
            lanes[rec.lane] -= 1
            self.size -= 1
            self.lane_counts[rec.lane] -= 1
            self._running.add(key)
            return rec
        return None

    def release(self, rec: MessageRecord) -> None:
        key = rec.order_key
        self._running.discard(key)
        self.pending.pop(rec.ingest_id, None)
        if self._keys.get(key):
            self._push(key)
        else:
            self._keys.pop(key, None)
            self._klanes.pop(key, None)
        self.wake.set()

    def checkpoint(self) -> int:
        """Every message with ingest_id <= the returned value is finished for this module."""
        high = self.cursor if self.overflow else self.max_seen
        if self.dropped_low is not None:
            high = min(high, self.dropped_low - 1)
        if self.pending:
            high = min(high, min(self.pending) - 1)
        return max(high, 0)

    def oldest_age(self) -> float:
        return time.monotonic() - min(self.pending.values()) if self.pending else 0.0


@dataclass(slots=True)
class _Commit:
    trace_id: str
    module: str
    ingest_id: int
    queued_at: float
    started_at: float
    ops: list[tuple[str, tuple[Any, ...]]]
    drafts: list[OutboundDraft]
    ttl: dict[str, float]
    config_version: str


class _Runner:
    def __init__(self, module: Module, mq: ModuleQueue, concurrency: int, timeout_s: float) -> None:
        self.module = module
        self.mq = mq
        self.concurrency = concurrency
        self.timeout_s = timeout_s
        self.processed = 0
        self.failed = 0
        self.saved_mark = -1
        self.warned_at = 0.0


class QueueManager:
    def __init__(
        self,
        store: ConfigStore,
        db: Database,
        writer: BatchWriter,
        on_outbound: Callable[[list[int]], None],
        notify_logs: Callable[[str], None],
    ) -> None:
        self._store = store
        self._db = db
        self._writer = writer
        self._on_outbound = on_outbound
        self._notify_logs = notify_logs
        self._runners: dict[str, _Runner] = {}
        self._match_cache: dict[str, list[_Runner]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        # Marks, module writes and trace rows of many events share one round of statements.
        self._commits: Aggregator[_Commit] = Aggregator(writer, self._write_commits, urgent=True)
        self._queued: Aggregator[dict[str, Any]] = Aggregator(writer, self._write_queued, urgent=False)

    def register(self, module: Module) -> None:
        cfg = self._store.current.config
        mcfg = cfg.modules.get(module.name)
        if mcfg is not None and not mcfg.enabled:
            return
        mq = ModuleQueue(
            module.name,
            maxsize=(mcfg.maxsize if mcfg and mcfg.maxsize else cfg.queue.maxsize),
            live_threshold=cfg.queue.live_threshold,
            backfill_rate=cfg.queue.backfill_rate_per_s,
        )
        self._runners[module.name] = _Runner(
            module, mq,
            concurrency=(mcfg.concurrency if mcfg and mcfg.concurrency else module.concurrency),
            timeout_s=(mcfg.timeout_s if mcfg and mcfg.timeout_s else module.timeout_s),
        )
        self._match_cache.clear()

    def queue(self, name: str) -> ModuleQueue:
        return self._runners[name].mq

    def _matching(self, msg_type: str) -> list[_Runner]:
        hit = self._match_cache.get(msg_type)
        if hit is None:
            hit = [] if msg_type == UNKNOWN else [
                r for r in self._runners.values()
                if any(fnmatchcase(msg_type, mask) for mask in r.module.subscriptions)
            ]
            if len(self._match_cache) > 10_000:
                self._match_cache.clear()
            self._match_cache[msg_type] = hit
        return hit

    def subscribers(self, msg_type: str) -> int:
        return len(self._matching(msg_type))

    def dispatch(self, rec: MessageRecord) -> None:
        """Deliver a sorted message to every subscribed module (FT-7.1)."""
        targets = self._matching(rec.msg_type or UNKNOWN)
        for runner in self._runners.values():
            if runner in targets:
                if runner.mq.offer(rec):
                    self._trace_queued(runner, rec)
            else:
                runner.mq.note_seen(rec.ingest_id)

    def _trace_queued(self, runner: _Runner, rec: MessageRecord) -> None:
        self._queued.add({"trace_id": rec.trace_id, "module": runner.module.name, "ingest_id": rec.ingest_id,
                          "queued_at": time.time()})

    @staticmethod
    async def _write_queued(conn: asyncpg.Connection, entries: list[dict[str, Any]]) -> list[Any]:
        await conn.execute(_QUEUED, json.dumps(entries))
        return [None] * len(entries)

    async def _write_commits(self, conn: asyncpg.Connection, entries: list[_Commit]) -> list[Any]:
        try:
            async with conn.transaction():
                return await self._commit_group(conn, entries)
        except Exception as exc:
            if is_connection_error(exc):
                raise
        # One of the modules produced a failing write: isolate it, commit the others.
        results: list[Any] = []
        for entry in entries:
            try:
                async with conn.transaction():
                    results.extend(await self._commit_group(conn, [entry]))
            except Exception as exc:
                if is_connection_error(exc):
                    raise
                results.append(exc)
        return results

    @staticmethod
    async def _commit_group(conn: asyncpg.Connection, entries: list[_Commit]) -> list[Any]:
        """The mark, the module's writes and its outbound tasks in one transaction (FT-8.3)."""
        marks = [{"trace_id": e.trace_id, "module": e.module, "ingest_id": e.ingest_id,
                  "queued_at": e.queued_at, "started_at": e.started_at} for e in entries]
        rows = await conn.fetch(_DONE, json.dumps(marks))
        fresh_keys = {(r["trace_id"], r["module"]) for r in rows}
        # An entry that is not "fresh" was already processed before a crash: nothing is repeated.
        fresh = [e for e in entries if (e.trace_id, e.module) in fresh_keys]
        for pos in range(max((len(e.ops) for e in fresh), default=0)):
            grouped: dict[str, list[tuple[Any, ...]]] = {}
            for e in fresh:
                if pos < len(e.ops):
                    grouped.setdefault(e.ops[pos][0], []).append(e.ops[pos][1])
            for sql, args_list in grouped.items():
                if len(args_list) == 1:
                    await conn.execute(sql, *args_list[0])
                else:
                    await conn.executemany(sql, args_list)
        ids: dict[tuple[str, str], list[int]] = {}
        for e in fresh:
            for draft in e.drafts:
                task_id = await insert_outbound(
                    conn, draft, parent_trace_id=e.trace_id, module=e.module,
                    ttl_s=e.ttl.get(draft.kind, 3600.0), config_version=e.config_version,
                )
                if task_id is not None:
                    ids.setdefault((e.trace_id, e.module), []).append(task_id)
        if fresh:
            await conn.execute(_INBOUND_DONE, [e.trace_id for e in fresh])
        return [ids.get((e.trace_id, e.module), []) for e in entries]

    async def start(self) -> None:
        await self._db.wait_available()
        rows = await self._db.pool.fetch("SELECT module, last_ingest_id FROM module_checkpoints")
        marks = {r["module"]: r["last_ingest_id"] for r in rows}
        for name, runner in self._runners.items():
            runner.mq.cursor = marks.get(name, 0)
            runner.saved_mark = marks.get(name, -1)
            for i in range(runner.concurrency):
                self._tasks.append(asyncio.create_task(self._worker(runner), name=f"module-{name}-{i}"))
            self._tasks.append(asyncio.create_task(self._refill_loop(runner), name=f"refill-{name}"))
        self._tasks.append(asyncio.create_task(self._housekeeping(), name="queue-housekeeping"))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # ---- workers ---------------------------------------------------------------------------
    async def _worker(self, runner: _Runner) -> None:
        mq = runner.mq
        while True:
            rec = mq.take()
            if rec is None:
                mq.wake.clear()
                timeout = max(mq.next_retry - time.monotonic(), 0.01) if mq.lane_counts[LANE_BACKFILL] else None
                try:
                    await asyncio.wait_for(mq.wake.wait(), timeout)
                except TimeoutError:
                    pass
                continue
            try:
                await self._process(runner, rec)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("module %s: failed to record the result of %s", runner.module.name, rec.trace_id)
            finally:
                mq.release(rec)

    async def _process(self, runner: _Runner, rec: MessageRecord) -> None:
        module = runner.module
        rt = self._store.current
        mcfg = rt.config.modules.get(module.name)
        ctx = Context(Event(rec), module.name, self._db, mcfg.config if mcfg else {}, rt.version)
        queued_at = time.time() - (time.monotonic() - runner.mq.pending.get(rec.ingest_id, time.monotonic()))
        started = time.time()
        failure: tuple[Reason, str] | None = None
        try:
            await asyncio.wait_for(module.handle(ctx.event, ctx), runner.timeout_s)
        except TimeoutError:
            failure = (Reason.MODULE_TIMEOUT, f"timeout {runner.timeout_s}s")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("module %s: error on %s", module.name, rec.trace_id)
            failure = (Reason.MODULE_ERROR, repr(exc)[:500])
        if failure is None:
            entry = _Commit(rec.trace_id, module.name, rec.ingest_id, queued_at, started, ctx.ops, ctx.drafts,
                            rt.config.output.ttl_s, rt.version)
            crashpoints.hit("module_before_commit")
            try:
                ids: list[int] = await self._commits.add_sync(entry)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("module %s: writes of %s failed: %r", module.name, rec.trace_id, exc)
                failure = (Reason.MODULE_ERROR, repr(exc)[:500])
            else:
                crashpoints.hit("module_after_commit")
                runner.processed += 1
                if ids:
                    self._on_outbound(ids)
                return

        reason, error = failure
        runner.failed += 1

        async def fail(conn: asyncpg.Connection) -> None:
            await conn.execute(_FAILED, rec.trace_id, module.name, rec.ingest_id, queued_at, started, str(reason),
                               error)
            await conn.execute(
                "UPDATE trace_inbound SET status = $2, reason_code = $3 "
                "WHERE trace_id = $1::uuid AND status = 'in_progress'",
                rec.trace_id, str(Status.WAITING), str(reason),
            )

        await self._writer.submit_sync(fail)

    # ---- refill from PostgreSQL --------------------------------------------------------------
    async def _refill_loop(self, runner: _Runner) -> None:
        mq = runner.mq
        while True:
            try:
                if mq.overflow and mq.size <= mq.maxsize // 2:
                    await self._refill(runner)
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("module %s: refill failed", runner.module.name)
            await asyncio.sleep(0.1)

    async def _refill(self, runner: _Runner) -> None:
        mq = runner.mq
        await self._db.wait_available()
        if mq.dropped_low is not None:
            mq.cursor = min(mq.cursor, mq.dropped_low - 1)
            mq.dropped_low = None
        limit = max(mq.maxsize - mq.size, 1)
        rows = await self._db.pool.fetch(_REFILL, mq.cursor, runner.module.name, limit)
        matching_cache: dict[str, bool] = {}
        for row in rows:
            rec = MessageRecord.from_row(row)
            ok = matching_cache.get(rec.msg_type or "")
            if ok is None:
                ok = runner in self._matching(rec.msg_type or UNKNOWN)
                matching_cache[rec.msg_type or ""] = ok
            if ok and rec.ingest_id not in mq.pending:
                mq.force(rec)
                self._trace_queued(runner, rec)
            mq.cursor = rec.ingest_id
        if len(rows) < limit and mq.dropped_low is None:
            # Caught up with PostgreSQL and nothing was dropped meanwhile: back to in-memory hand-over.
            mq.max_seen = max(mq.max_seen, mq.cursor)
            mq.overflow = False

    # ---- checkpoints and warnings --------------------------------------------------------------
    async def _housekeeping(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            cfg = self._store.current.config.queue
            for name, runner in self._runners.items():
                mark = runner.mq.checkpoint()
                if mark > runner.saved_mark:
                    runner.saved_mark = mark
                    self._save_mark(name, mark)
                age = runner.mq.oldest_age()
                if (runner.mq.size > cfg.warn_len or age > cfg.warn_lag_s) and time.monotonic() - runner.warned_at > 60:
                    runner.warned_at = time.monotonic()
                    self._notify_logs(
                        f"Модуль {name}: очередь {runner.mq.size}, задержка {age:.1f} с "
                        f"(пороги {cfg.warn_len} / {cfg.warn_lag_s} с)"
                    )

    def _save_mark(self, name: str, mark: int) -> None:
        async def write(conn: asyncpg.Connection) -> None:
            await conn.execute(
                "INSERT INTO module_checkpoints (module, last_ingest_id) VALUES ($1, $2) "
                "ON CONFLICT (module) DO UPDATE SET last_ingest_id = EXCLUDED.last_ingest_id, updated_at = now() "
                "WHERE module_checkpoints.last_ingest_id < EXCLUDED.last_ingest_id",
                name, mark,
            )

        self._writer.submit(write)

    def stats(self) -> dict[str, Any]:
        return {
            name: {"size": r.mq.size, "lanes": list(r.mq.lane_counts), "running": len(r.mq.pending) - r.mq.size,
                   "processed": r.processed, "failed": r.failed, "overflow": r.mq.overflow,
                   "mark": r.mq.checkpoint()}
            for name, r in self._runners.items()
        }
