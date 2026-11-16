"""Stuck tasks, stage metrics and trace retention (FT-11.7, 11.10, 11.11).

Stage names (keys of `stage_norms_s`) are the stage a task is WAITING to reach; the time on
a stage is counted from the last stage it reached:

    in_pg          accepted, still only in the SQLite buffer        since buffered
    sorted         stored in PostgreSQL, not typed yet              since in_pg_at
    module_done    queued to a module (or failed in it)             since queued_at
    out_assigned   outbound task created, no account yet            since queued_at
    out_sending    account assigned, not sent yet                   since assigned_at
    out_delivered  being sent, outcome not recorded yet             since sending_at

A task over the norm gets the mark "stuck" (`stuck_at`, `stuck_stage`) and is listed by
/stuck; many stuck tasks on one stage raise an alarm naming the stage.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from autocash.core.config.loader import ConfigStore
from autocash.core.storage.pg import Database, is_connection_error

log = logging.getLogger(__name__)

STAGE_TITLES = {
    "in_pg": "буфер SQLite → PostgreSQL",
    "sorted": "PostgreSQL → сортировка",
    "module_done": "обработка модулем",
    "out_assigned": "исходящая: назначение аккаунта",
    "out_sending": "исходящая: отправка",
    "out_delivered": "исходящая: подтверждение доставки",
}

# One SELECT over the open rows of all three trace tables (partial indexes on open rows).
_OPEN = """
SELECT 'inbound' AS kind, trace_id::text AS trace_id, NULL::bigint AS task_id, 'sorted' AS stage,
       extract(epoch FROM now() - in_pg_at) AS age, coalesce(reason_code, waited_reason) AS reason,
       NULL::text AS module, account_id, chat_id
FROM trace_inbound WHERE finished_at IS NULL AND sorted_at IS NULL
UNION ALL
SELECT 'module', m.trace_id::text, NULL, 'module_done', extract(epoch FROM now() - m.queued_at),
       m.reason_code, m.module, t.account_id, t.chat_id
FROM trace_module m LEFT JOIN trace_inbound t USING (trace_id)
WHERE (m.finished_at IS NULL OR m.status = 'waiting') AND m.queued_at IS NOT NULL
UNION ALL
SELECT 'outbound', trace_id::text, id,
       CASE stage WHEN 'assigned' THEN 'out_sending' WHEN 'sending' THEN 'out_delivered' ELSE 'out_assigned' END,
       extract(epoch FROM now() - CASE stage WHEN 'assigned' THEN assigned_at WHEN 'sending' THEN sending_at
                                      ELSE coalesce(queued_at, created_at) END),
       reason_code, module, account_id, chat_id
FROM outbound_tasks WHERE finished_at IS NULL
"""

_MARK_INBOUND = """
UPDATE trace_inbound SET stuck_at = now(), stuck_stage = $2
WHERE trace_id = ANY($1::uuid[]) AND stuck_at IS NULL
"""
_MARK_MODULE = """
UPDATE trace_module SET stuck_at = now(), stuck_stage = 'module_done'
WHERE (trace_id, module) IN (SELECT * FROM unnest($1::uuid[], $2::text[])) AND stuck_at IS NULL
"""
_MARK_OUTBOUND = """
UPDATE outbound_tasks SET stuck_at = now(), stuck_stage = x.stage
FROM unnest($1::bigint[], $2::text[]) AS x(id, stage)
WHERE outbound_tasks.id = x.id AND (outbound_tasks.stuck_stage IS DISTINCT FROM x.stage)
"""

# ---- hourly aggregates, kept forever (FT-11.11) --------------------------------------------
_DURATIONS = """
WITH d AS (
    SELECT 'inbound' AS kind, s.stage, s.secs, t.chat_id::text AS source, t.account_id::text AS account,
           NULL::text AS module, date_trunc('hour', t.accepted_at) AS hour
    FROM trace_inbound t,
         LATERAL (VALUES ('buffered', extract(epoch FROM t.buffered_at - t.accepted_at)),
                         ('in_pg', extract(epoch FROM t.in_pg_at - t.buffered_at)),
                         ('sorted', extract(epoch FROM t.sorted_at - t.in_pg_at))) AS s(stage, secs)
    WHERE t.accepted_at >= $1 AND t.accepted_at < $2 AND s.secs IS NOT NULL
  UNION ALL
    SELECT 'module', 'module_done', extract(epoch FROM m.finished_at - m.queued_at), t.chat_id::text,
           t.account_id::text, m.module, date_trunc('hour', m.queued_at)
    FROM trace_module m JOIN trace_inbound t USING (trace_id)
    WHERE m.queued_at >= $1 AND m.queued_at < $2 AND m.status = 'processed'
  UNION ALL
    SELECT 'outbound', s.stage, s.secs, o.chat_id::text, o.account_id::text, o.module,
           date_trunc('hour', o.created_at)
    FROM outbound_tasks o,
         LATERAL (VALUES ('out_assigned', extract(epoch FROM o.assigned_at - coalesce(o.queued_at, o.created_at))),
                         ('out_sending', extract(epoch FROM o.sending_at - o.assigned_at)),
                         ('out_delivered', extract(epoch FROM o.finished_at - o.sending_at))) AS s(stage, secs)
    WHERE o.created_at >= $1 AND o.created_at < $2 AND s.secs IS NOT NULL
      AND (s.stage <> 'out_delivered' OR o.status = 'delivered')
), dims AS (
    SELECT kind, stage, secs, hour, 'all' AS dim, '*' AS value FROM d
    UNION ALL SELECT kind, stage, secs, hour, 'source', source FROM d WHERE source IS NOT NULL
    UNION ALL SELECT kind, stage, secs, hour, 'account', account FROM d WHERE account IS NOT NULL
    UNION ALL SELECT kind, stage, secs, hour, 'module', module FROM d WHERE module IS NOT NULL
)
INSERT INTO trace_metrics_hourly (hour, task_kind, stage, dim, dim_value, count, p50, p95, max)
SELECT hour, kind, stage, dim, value, count(*),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY secs), percentile_cont(0.95) WITHIN GROUP (ORDER BY secs),
       max(secs)
FROM dims GROUP BY hour, kind, stage, dim, value
ON CONFLICT (hour, task_kind, stage, dim, dim_value) DO UPDATE
    SET count = EXCLUDED.count, p50 = EXCLUDED.p50, p95 = EXCLUDED.p95, max = EXCLUDED.max
"""

_FINALS = """
WITH f AS (
    SELECT 'inbound' AS kind, status, coalesce(reason_code, '') AS reason, chat_id::text AS source,
           account_id::text AS account, NULL::text AS module, date_trunc('hour', accepted_at) AS hour
    FROM trace_inbound WHERE accepted_at >= $1 AND accepted_at < $2 AND finished_at IS NOT NULL
  UNION ALL
    SELECT 'module', m.status, coalesce(m.reason_code, ''), t.chat_id::text, t.account_id::text, m.module,
           date_trunc('hour', m.queued_at)
    FROM trace_module m JOIN trace_inbound t USING (trace_id)
    WHERE m.queued_at >= $1 AND m.queued_at < $2 AND m.finished_at IS NOT NULL
  UNION ALL
    SELECT 'outbound', status, coalesce(reason_code, ''), chat_id::text, account_id::text, module,
           date_trunc('hour', created_at)
    FROM outbound_tasks WHERE created_at >= $1 AND created_at < $2 AND finished_at IS NOT NULL
), dims AS (
    SELECT kind, status, reason, hour, 'all' AS dim, '*' AS value FROM f
    UNION ALL SELECT kind, status, reason, hour, 'source', source FROM f WHERE source IS NOT NULL
    UNION ALL SELECT kind, status, reason, hour, 'account', account FROM f WHERE account IS NOT NULL
    UNION ALL SELECT kind, status, reason, hour, 'module', module FROM f WHERE module IS NOT NULL
)
INSERT INTO trace_finals_hourly (hour, task_kind, status, reason_code, dim, dim_value, count)
SELECT hour, kind, status, reason, dim, value, count(*) FROM dims
GROUP BY hour, kind, status, reason, dim, value
ON CONFLICT (hour, task_kind, status, reason_code, dim, dim_value) DO UPDATE SET count = EXCLUDED.count
"""


@dataclass(slots=True)
class StuckTask:
    kind: str
    trace_id: str
    task_id: int | None
    stage: str
    age: float
    reason: str | None
    module: str | None
    account_id: int | None
    chat_id: int | None


@dataclass(slots=True)
class StuckSnapshot:
    at: float = 0.0
    tasks: list[StuckTask] = field(default_factory=list)
    buffer_stuck: int = 0  # accepted events still only in SQLite longer than the norm
    buffer_reason: str | None = None

    def by_stage(self) -> dict[str, dict[str, int]]:
        """stage -> reason ("" = in progress) -> count."""
        out: dict[str, dict[str, int]] = {}
        for task in self.tasks:
            bucket = out.setdefault(task.stage, {})
            bucket[task.reason or ""] = bucket.get(task.reason or "", 0) + 1
        if self.buffer_stuck:
            out.setdefault("in_pg", {})[self.buffer_reason or ""] = self.buffer_stuck
        return out

    def counts(self) -> dict[str, int]:
        return {stage: sum(reasons.values()) for stage, reasons in self.by_stage().items()}


class TraceMonitor:
    def __init__(
        self,
        store: ConfigStore,
        db: Database,
        buffer_backlog: Callable[[], Awaitable[tuple[int, float, str | None]]],
        alarm: Callable[[str], None],
    ) -> None:
        self._store = store
        self._db = db
        self._buffer_backlog = buffer_backlog  # -> (pending count, oldest age s, wait reason)
        self._alarm = alarm
        self.snapshot = StuckSnapshot()
        self._alarmed: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._rolled_at = 0.0
        self._purged_at = 0.0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="trace-monitor")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        while True:
            cfg = self._store.current.config.trace
            try:
                await self.check()
                now = time.monotonic()
                if now - self._rolled_at >= cfg.rollup_interval_s:
                    self._rolled_at = now
                    await self.rollup()
                if now - self._purged_at >= cfg.purge_interval_s:
                    self._purged_at = now
                    await self.purge()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_connection_error(exc):
                    # Nothing else may be writing right now: the monitor notices the outage itself,
                    # so /status and the emergency channel reflect it (NFT-18).
                    self._db.mark_down(exc)
                else:
                    log.exception("trace monitor failed")
            await asyncio.sleep(cfg.stuck_check_s)

    # ---- stuck tasks ---------------------------------------------------------------------------
    def norm(self, stage: str) -> float:
        return float(self._store.current.config.stage_norms_s.get(stage, float("inf")))

    async def check(self) -> StuckSnapshot:
        snap = StuckSnapshot(at=time.time())
        if not self._db.available:
            # Probe the database: when it is back and nothing is pending, no writer would notice.
            try:
                await asyncio.wait_for(self._db.connect(), 3.0)
            except Exception as exc:
                if not is_connection_error(exc):
                    raise
        pending, oldest, reason = await self._buffer_backlog()
        if pending and oldest > self.norm("in_pg"):
            snap.buffer_stuck, snap.buffer_reason = pending, reason
        if self._db.available:
            rows = await self._db.pool.fetch(_OPEN)
            for r in rows:
                age = float(r["age"] or 0.0)
                if age <= self.norm(r["stage"]):
                    continue
                snap.tasks.append(StuckTask(r["kind"], r["trace_id"], r["task_id"], r["stage"], age, r["reason"],
                                            r["module"], r["account_id"], r["chat_id"]))
            await self._mark(snap.tasks)
        elif self.snapshot.tasks:
            snap.tasks = self.snapshot.tasks  # keep the last known picture while PostgreSQL is down
        self.snapshot = snap
        self._mass_alarm(snap)
        return snap

    async def _mark(self, tasks: list[StuckTask]) -> None:
        inbound = [t for t in tasks if t.kind == "inbound"]
        modules = [t for t in tasks if t.kind == "module"]
        outbound = [t for t in tasks if t.kind == "outbound" and t.task_id is not None]
        if not (inbound or modules or outbound):
            return
        async with self._db.pool.acquire() as conn, conn.transaction():
            if inbound:
                await conn.execute(_MARK_INBOUND, [t.trace_id for t in inbound], "sorted")
            if modules:
                await conn.execute(_MARK_MODULE, [t.trace_id for t in modules], [t.module or "" for t in modules])
            if outbound:
                await conn.execute(_MARK_OUTBOUND, [t.task_id for t in outbound], [t.stage for t in outbound])

    def _mass_alarm(self, snap: StuckSnapshot) -> None:
        threshold = self._store.current.config.trace.mass_stuck_threshold
        counts = snap.counts()
        for stage, count in counts.items():
            if count >= threshold and stage not in self._alarmed:
                self._alarmed.add(stage)
                reasons = ", ".join(f"{r or 'в работе'}: {n}" for r, n in snap.by_stage()[stage].items())
                self._alarm(
                    f"Массовое зависание на этапе «{stage}» ({STAGE_TITLES.get(stage, stage)}): "
                    f"{count} задач дольше норматива {self.norm(stage):g} с ({reasons})"
                )
        # The alarm repeats only after the stage has recovered (below half of the threshold).
        self._alarmed = {s for s in self._alarmed if counts.get(s, 0) >= max(threshold // 2, 1)}

    # ---- metrics -----------------------------------------------------------------------------------
    async def rollup(self, hours: int = 2) -> None:
        """Recompute hourly aggregates of the current and previous hours (upsert)."""
        now = datetime.now(UTC)
        start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours - 1)
        end = start + timedelta(hours=hours)
        async with self._db.pool.acquire() as conn, conn.transaction():
            await conn.execute(_DURATIONS, start, end)
            await conn.execute(_FINALS, start, end)

    async def purge(self) -> int:
        """Detailed traces live N days; hourly aggregates stay forever (FT-11.11)."""
        days = self._store.current.config.retention.trace_days
        before = datetime.now(UTC) - timedelta(days=days)
        # Aggregates of the hours being purged are made final first.
        async with self._db.pool.acquire() as conn, conn.transaction():
            oldest = await conn.fetchval("SELECT min(accepted_at) FROM trace_inbound WHERE accepted_at < $1", before)
            if oldest is not None:
                await conn.execute(_DURATIONS, oldest - timedelta(hours=1), before)
                await conn.execute(_FINALS, oldest - timedelta(hours=1), before)
            removed = 0
            removed += _count(await conn.execute(
                "DELETE FROM trace_module WHERE queued_at < $1 AND finished_at IS NOT NULL", before))
            removed += _count(await conn.execute(
                "DELETE FROM trace_inbound WHERE accepted_at < $1 AND finished_at IS NOT NULL", before))
            removed += _count(await conn.execute(
                "DELETE FROM outbound_tasks WHERE created_at < $1 AND finished_at IS NOT NULL", before))
        return removed

    async def metrics(self, hours: float, dim: str = "all") -> list[Any]:
        """Stage metrics over the last `hours` from the hourly aggregates (reports pool)."""
        since = datetime.now(UTC) - timedelta(hours=hours)
        rows: list[Any] = await self._db.reports.fetch(
            "SELECT task_kind, stage, dim_value, sum(count) AS count, "
            "sum(p50 * count) / nullif(sum(count), 0) AS p50, max(p95) AS p95 "
            "FROM trace_metrics_hourly WHERE hour >= date_trunc('hour', $1::timestamptz) AND dim = $2 "
            "GROUP BY task_kind, stage, dim_value ORDER BY task_kind, stage, dim_value",
            since, dim,
        )
        return rows

    async def finals(self, hours: float, dim: str = "all") -> list[Any]:
        since = datetime.now(UTC) - timedelta(hours=hours)
        rows: list[Any] = await self._db.reports.fetch(
            "SELECT task_kind, status, reason_code, dim_value, sum(count) AS count FROM trace_finals_hourly "
            "WHERE hour >= date_trunc('hour', $1::timestamptz) AND dim = $2 "
            "GROUP BY task_kind, status, reason_code, dim_value ORDER BY task_kind, count DESC",
            since, dim,
        )
        return rows


def _count(status: str) -> int:
    try:
        return int(status.split()[-1])
    except (ValueError, IndexError):
        return 0
