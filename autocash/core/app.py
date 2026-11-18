"""Application wiring: Gateway -> Extractor -> SQLite -> Uploader -> PostgreSQL -> Sorter ->
Queue manager -> Modules -> Output (item 3.1 of the TZ)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

import asyncpg

from autocash.actions.entities import expire_states, shutdown_executor
from autocash.core.alerts import Alerts
from autocash.core.backfill import Backfill
from autocash.core.buffer.sqlite_buffer import BufferPragmaError, SqliteBuffer
from autocash.core.commands.service import SystemCommands
from autocash.core.config.loader import ConfigStore, Runtime
from autocash.core.extractor.extractor import CompiledManifest, extract
from autocash.core.gateway.gateway import Gateway, OwnMessages
from autocash.core.outbox.OBX_outbox import Outbox
from autocash.core.outbox.OBX_tasks import OutboundDraft, insert_outbound
from autocash.core.pool.limiter import BACKFILL, READ
from autocash.core.pool.poller import Poller
from autocash.core.pool.pool import AccountPool
from autocash.core.pool.scheduler import Scheduler
from autocash.core.pool.sessions import ClientFactory, SessionError, SessionWatcher, load_key
from autocash.core.protocols import Protocols
from autocash.core.queue.manager import QueueManager
from autocash.core.sorter.sorter import Sorter
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import RawEvent, TelegramClient
from autocash.core.trace.monitor import TraceMonitor
from autocash.core.uploader.uploader import Uploader
from autocash.core.watchdog import SystemdNotifier
from autocash.modules.api import Module, Services
from autocash.web.server import WebAdmin

log = logging.getLogger(__name__)


async def reextract(db: Database, manifest: CompiledManifest, *, batch: int = 200) -> int:
    """Recompute features of stored texts after a manifest change (FT-3.9)."""
    total = 0
    while True:
        rows = await db.pool.fetch(
            "SELECT ingest_id, text FROM messages WHERE text IS NOT NULL AND policy = 'full' "
            "AND manifest_version IS DISTINCT FROM $1 ORDER BY ingest_id LIMIT $2",
            manifest.version, batch,
        )
        if not rows:
            return total
        updates = [
            {"ingest_id": r["ingest_id"], "features": extract(r["text"], manifest).features} for r in rows
        ]
        await db.pool.execute(
            "UPDATE messages m SET features = x.features, manifest_version = $2 "
            "FROM jsonb_to_recordset($1::jsonb) AS x(ingest_id bigint, features jsonb) "
            "WHERE m.ingest_id = x.ingest_id",
            json.dumps(updates, ensure_ascii=False), manifest.version,
        )
        total += len(rows)
        await asyncio.sleep(0.05)  # low priority: yield to the live flow


class App:
    def __init__(
        self,
        *,
        config_path: Path,
        sqlite_path: Path,
        pg_dsn: str,
        modules: list[Module],
        client: TelegramClient | None = None,
        clients: list[TelegramClient] | None = None,
        client_factory: ClientFactory | None = None,
        maintenance_interval_s: float = 60.0,
    ) -> None:
        self.store = ConfigStore(config_path)
        cfg = self.store.current.config
        # Users, groups and rights (D-RM-7): the local snapshot first, so the gateway and the rights
        # work before PostgreSQL is reachable; Role Manager refreshes it from rm_* at start (the only
        # source of truth: the snapshot is a cache, never edited).
        self.store.directory.load(Path(sqlite_path).parent / "directory.json")
        # Accounts connected at start; more join and leave at run time through `self.pool`
        # or through the sessions directory (FT-1.2).
        self._clients: list[TelegramClient] = [*([client] if client is not None else []), *(clients or [])]
        self._client_factory = client_factory
        self.sessions: SessionWatcher | None = None
        self.buffer = SqliteBuffer(
            sqlite_path, journal_mode=cfg.buffer.journal_mode, synchronous=cfg.buffer.synchronous,
            soft_limit_mb=cfg.buffer.soft_limit_mb, hard_limit_mb=cfg.buffer.hard_limit_mb,
            top_priority=cfg.buffer.top_priority,
        )
        self.db = Database(pg_dsn, reports_timeout_ms=cfg.reports.statement_timeout_ms,
                           reports_size=cfg.reports.pool_size)
        self.writer = BatchWriter(self.db, flush_interval_ms=cfg.writer.flush_interval_ms,
                                  batch_max=cfg.writer.batch_max)
        self.own = OwnMessages()
        self.pool = AccountPool(self.store, self.db, self.writer)
        self.scheduler = Scheduler(self.store, self.pool, self.writer)
        self.poller = Poller(self.store, self.pool, self.scheduler)
        self.outbox = Outbox(self.store, self.db, self.writer, self.own, self.pool, self.scheduler)
        self.alerts = Alerts(self.store, self.db, self.writer, self.outbox)
        self.protocols = Protocols(self.db, lambda: self.modules, self.alerts.logs,  # D-RM-12
                                   self._module_enabled)
        self.outbox.on_failure = self.alerts.logs  # OBX_Finalized: final failures (D-OBX-4)
        self.gateway = Gateway(self.store, self.buffer, self.own, self.alerts.logs)
        self.gateway.pool_user_ids = self.pool.user_ids
        self.pool.bind(self.gateway.on_event, self.alerts.alarm, self.alerts.logs)
        self.pool.subscribe(self.scheduler.request)
        self.pool.subscribe(self.outbox.on_pool_change)
        self.queues = QueueManager(self.store, self.db, self.writer, self.outbox.enqueue, self.alerts.logs)
        # Debug and control commands are a core module (stage 1c).
        self.commands = SystemCommands(self)
        self.modules: list[Module] = []
        for module in [*modules, self.commands]:
            self.add_module(module)
        self.gateway.on_command = self.commands.emergency
        self.monitor = TraceMonitor(self.store, self.db, self._buffer_backlog, self.alerts.alarm)
        self.backfill = Backfill(self.store, self.db, self.pool, self.scheduler, self._pipeline_backlog)
        self.notifier = SystemdNotifier()
        self.web = WebAdmin(self)  # stage 5: started when `web.enabled`
        self.sorter = Sorter(self.store, self.db, self.writer, self.queues.dispatch, self.queues.subscribers,
                             self.alerts.debug)
        self.uploader = Uploader(self.store, self.buffer, self.db, self.sorter.submit, self.sorter.recover_nowait)
        self._maintenance_interval = maintenance_interval_s
        self._tasks: list[asyncio.Task[Any]] = []
        self._bg: set[asyncio.Task[Any]] = set()
        self.started_at = 0.0
        self.ready_at: float | None = None  # NFT-17: time to full readiness
        self.ready = asyncio.Event()

        self.store.on_error = self.alerts.logs
        self.store.subscribe(self._on_config_change)
        self.db.on_change = self._on_db_change
        self.buffer.on_level = self._on_buffer_level

    # ---- lifecycle -----------------------------------------------------------------------
    async def start(self) -> None:
        self.started_at = time.time()
        try:
            await self.buffer.start()
        except BufferPragmaError as exc:
            # FT-4.5: alarm and stop.
            log.critical("%s", exc)
            self.pool.intake = False  # nothing may be accepted without the buffer
            for client in self._clients:
                await self.pool.add(client)
            self.alerts.alarm(f"Остановка: {exc}")
            await asyncio.sleep(0)
            await asyncio.gather(*self.alerts._tasks, return_exceptions=True)
            await self.pool.stop()
            raise
        # The watchdog heartbeat comes from the main loop: a hung loop stops it (NFT-16).
        self._tasks.append(asyncio.create_task(self.notifier.heartbeat(), name="watchdog"))
        # Intake into SQLite starts immediately and does not depend on PostgreSQL (NFT-17).
        for client in self._clients:
            await self.pool.add(client)
        await self._start_sessions()
        task = asyncio.create_task(self._start_pipeline(), name="pipeline-start")
        task.add_done_callback(self._pipeline_started)
        self._tasks.append(task)

    def _pipeline_started(self, task: asyncio.Task[Any]) -> None:
        """A failure of the pipeline start must not be silent: intake goes on, processing does not."""
        if task.cancelled() or task.exception() is None:
            return
        exc = task.exception()
        log.critical("pipeline start failed: %r", exc, exc_info=exc)
        self.alerts.alarm(f"Конвейер не запустился: {exc!r}. Приём в SQLite продолжается, обработка стоит.")

    async def _start_sessions(self) -> None:
        cfg = self.store.current.config.pool
        if not cfg.sessions_dir or self._client_factory is None:
            return
        try:
            key = load_key()
        except SessionError as exc:
            self.alerts.alarm(f"Каталог сессий не используется: {exc}")
            return
        self.sessions = SessionWatcher(Path(cfg.sessions_dir), key, self._client_factory, self.pool,
                                       cfg.scan_interval_s)
        await self.sessions.start()

    async def _start_pipeline(self) -> None:
        await self.db.wait_available()
        await self.db.migrate({m.name: m.migrations for m in self.modules if m.migrations is not None})
        for module in self.modules:
            await module.start()  # e.g. Role Manager loads the directory before any event is handled
        self.protocols.start()
        self.writer.start()
        self._record_config(self.store.current)
        # Stored FloodWait pauses are applied before any active call is allowed (FT-10.3).
        await self.pool.load_state()
        await self.queues.start()
        self.sorter.start()
        await self.sorter.recover()
        await self.outbox.start()
        self.uploader.start()
        self.poller.start()
        self.monitor.start()
        self.backfill.start()
        for module in self.modules:
            if module.periodic_s:
                self._tasks.append(asyncio.create_task(self._periodic(module, module.periodic_s),
                                                       name=f"periodic-{module.name}"))
        self._tasks.append(asyncio.create_task(self._maintenance(), name="maintenance"))
        if self.store.current.config.web.enabled:
            try:
                await self.web.start()
            except OSError as exc:
                self.alerts.alarm(f"Веб-админка не запустилась: {exc}")
        self.ready_at = time.time()
        self.ready.set()
        self.notifier.ready(f"ready in {self.ready_at - self.started_at:.1f} s")
        log.info("pipeline ready in %.2f s", self.ready_at - self.started_at)

    async def stop(self) -> None:
        self.notifier.stopping()
        await self.web.stop()
        await self.protocols.stop()
        await self.monitor.stop()
        await self.backfill.stop()
        if self.sessions is not None:
            await self.sessions.stop()
        await self.poller.stop()
        self.scheduler.stop()
        await self.pool.stop()
        for task in [*self._tasks, *self._bg]:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._bg, return_exceptions=True)
        await self.uploader.stop()
        await self.sorter.stop()
        await self.queues.stop()
        await self.outbox.stop()
        if self.ready.is_set() and self.db.available:
            # Update counters of chats and the last state changes: a restart continues from them.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.writer.flush(), 3.0)
        await self.writer.stop(drain=False)
        await self.buffer.stop()
        await self.db.close()
        shutdown_executor()

    # ---- reactions to state changes -----------------------------------------------------------
    def reload_config(self) -> tuple[bool, str]:
        """Hot reload (FT-13.3). An invalid config is not applied; the error goes to `logs`."""
        return self.store.reload()

    def add_module(self, module: Module) -> None:
        """Register a module before start: queue, reconcile hook, services, /trace resolver."""
        self.queues.register(module)
        self.pool.reconcilers.append(module.reconcile)
        module.bind(Services(self.db, self._reread, self.alerts.logs, lambda: self.store.current,
                             self.outbox.enqueue, lambda account_id: str(self.pool.state(account_id)),
                             self._dialog_history, self.protocols.wake, self._create_outbound,
                             self._read_dialog))
        self.gateway.any_chat_commands.update(module.commands_any_chat)
        resolver = module.trace_resolver()
        if resolver is not None:
            self.commands.resolvers.append(resolver)
        self.modules.append(module)

    def _module_enabled(self, name: str) -> bool:
        mcfg = self.store.current.config.modules.get(name)
        return mcfg is None or mcfg.enabled

    async def _create_outbound(self, draft: OutboundDraft, module: str) -> int | None:
        """A task for the Outbox outside event handling (protocols, background work, D-OBX-1)."""
        rt = self.store.current
        async with self.db.pool.acquire() as conn:
            task_id = await insert_outbound(conn, draft, parent_trace_id=None, module=module,
                                            ttl_s=rt.config.outbox.ttl_s.get(draft.kind, 3600.0),
                                            config_version=rt.version)
        if task_id:
            self.outbox.enqueue([task_id])
        return task_id

    async def _read_dialog(self, account_id: int, chat_id: int, since: datetime, limit: int) -> list[RawEvent]:
        """Messages of a dialog from `since`, oldest first, pages of 100 through the limiter of the
        account at the background priority (a read, not a write: the antiflood of sends does not apply)."""
        acc = self.pool.accounts.get(account_id)
        if acc is None:
            return []
        out: list[RawEvent] = []
        after = 0
        while len(out) < limit:
            page = await acc.limiter.call(BACKFILL, "getHistory", partial(
                acc.client.get_history, chat_id, after, since, min(100, limit - len(out))))
            if not page:
                break
            out += page
            after = page[-1].msg_id
        return out

    async def _dialog_history(self, account_id: int, chat_id: int, before_msg_id: int) -> bool | None:
        """Did the private dialog have messages before this one? One getHistory (limit 1, the oldest
        message) through the limiter of the account; None when it can not be told (D-RM-11)."""
        acc = self.pool.accounts.get(account_id)
        if acc is None:
            return None
        try:
            oldest = await acc.limiter.call(READ, "getHistory", partial(
                acc.client.get_history, chat_id, 0, datetime(2013, 8, 1, tzinfo=UTC), 1))
        except Exception:
            log.warning("history of dialog %s via %s is not known", chat_id, account_id, exc_info=True)
            return None
        return bool(oldest) and oldest[0].msg_id < before_msg_id

    async def _reread(self, chat_id: int, msg_ids: list[int]) -> bool:
        return await self.pool.reread(chat_id, msg_ids, self.scheduler.account_for("read", chat_id))

    async def _periodic(self, module: Module, every: float) -> None:
        while True:
            await asyncio.sleep(every)
            try:
                await module.periodic()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not is_connection_error(exc):
                    log.exception("module %s: periodic work failed", module.name)

    async def _pipeline_backlog(self) -> int:
        """Events accepted but not yet handed to the modules, plus what waits in module queues."""
        stats = await self.buffer.stats()
        queued = sum(q["size"] for q in self.queues.stats().values())
        return int(stats["pending"]) + self.sorter._queue.qsize() + queued

    async def _buffer_backlog(self) -> tuple[int, float, str | None]:
        stats = await self.buffer.stats()
        reason = next(iter(stats["waiting"]), None)
        return int(stats["pending"]), float(stats["oldest_age"]), reason

    def _spawn(self, coro: Any, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    def _on_config_change(self, old: Runtime, new: Runtime) -> None:
        cfg = new.config
        self.buffer.set_limits(cfg.buffer.soft_limit_mb, cfg.buffer.hard_limit_mb, cfg.buffer.top_priority)
        self.writer.flush_interval = cfg.writer.flush_interval_ms / 1000
        self.writer.batch_max = cfg.writer.batch_max
        self._record_config(new)
        self.alerts.logs(f"Конфигурация перезагружена: версия {new.version}")
        if old.manifest.version != new.manifest.version and self.ready.is_set():
            self._spawn(self._reextract(new.manifest), "reextract")

    async def _reextract(self, manifest: CompiledManifest) -> None:
        try:
            count = await reextract(self.db, manifest)
            log.info("manifest %s: features recomputed for %d messages", manifest.version, count)
        except Exception:
            log.exception("reextract failed")

    def _record_config(self, rt: Runtime) -> None:
        content = json.dumps(rt.config.model_dump(mode="json"), ensure_ascii=False)

        async def write(conn: asyncpg.Connection) -> None:
            await conn.execute(
                "INSERT INTO config_versions (version, content) VALUES ($1, $2::jsonb) ON CONFLICT DO NOTHING",
                rt.version, content,
            )

        self.writer.submit(write)

    def _on_db_change(self, available: bool, detail: str) -> None:
        if available:
            if self.ready.is_set():
                self.alerts.logs("PostgreSQL снова доступен, выгружаю накопленное")
        else:
            self.alerts.alarm("PostgreSQL недоступен: приём продолжается в SQLite, обработка ждёт (DB_UNAVAILABLE)")

    def _on_buffer_level(self, level: str, size: int) -> None:
        mb = size // (1024 * 1024)
        if level == "soft":
            self.alerts.alarm(f"Буфер SQLite приближается к лимиту: {mb} МБ невыгруженного остатка")
        elif level == "hard":
            self.alerts.alarm(f"Буфер SQLite достиг лимита ({mb} МБ): принимаются только источники высшего приоритета")
        else:
            self.alerts.logs("Буфер SQLite вернулся в норму")

    # ---- periodic maintenance ------------------------------------------------------------------
    async def _maintenance(self) -> None:
        while True:
            try:
                await self.run_maintenance()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not is_connection_error(exc):
                    log.exception("maintenance failed")
            await asyncio.sleep(self._maintenance_interval)

    async def run_maintenance(self) -> None:
        cfg = self.store.current.config
        await self.buffer.purge(cfg.buffer.retention_hours * 3600)
        self._flush_presence()
        # FT-5.3: the text is removed when its term expires; extracted fields stay forever.
        await self.db.pool.execute(
            "UPDATE messages SET text = NULL WHERE text IS NOT NULL AND text_expires_at < now()"
        )
        # set_state with a timeout (FT-8.4): passed timeouts move entities to their timeout state.
        async with self.db.pool.acquire() as conn:
            await expire_states(conn)

    def _flush_presence(self) -> None:
        dirty = [(chat_id, p) for chat_id, p in self.gateway.presence.items() if p.get("dirty")]
        if not dirty:
            return
        rt = self.store.current
        rows = []
        for chat_id, p in dirty:
            p["dirty"] = False
            rows.append((chat_id, p["kind"], p["title"], sorted(rt.source(chat_id).roles), p["policy"],
                         p["dedup_approx"], p["known"], p["last_seen"]))

        async def write(conn: asyncpg.Connection) -> None:
            await conn.executemany(
                "INSERT INTO sources (chat_id, kind, title, roles, policy, dedup_approx, known, last_seen) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8::float8)) "
                "ON CONFLICT (chat_id) DO UPDATE SET kind = EXCLUDED.kind, "
                "title = coalesce(EXCLUDED.title, sources.title), roles = EXCLUDED.roles, policy = EXCLUDED.policy, "
                "dedup_approx = EXCLUDED.dedup_approx, known = EXCLUDED.known, last_seen = EXCLUDED.last_seen",
                rows,
            )

        self.writer.submit(write)

    # ---- introspection (used by tests now, by /status in stage 1c) ---------------------------------
    async def idle(self, timeout: float = 30.0) -> None:
        """Wait until everything accepted so far is uploaded, sorted, processed and sent."""
        deadline = time.monotonic() + timeout
        stable = 0
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            if not self.ready.is_set():
                continue
            stats = await self.buffer.stats()
            queues = self.queues.stats()
            busy = (
                stats["pending"] or len(self.writer) or self.sorter._queue.qsize()
                or self.uploader.busy or self.sorter.busy or self.outbox.busy or self.poller.busy
                or any(q["size"] or q["running"] or q["overflow"] for q in queues.values())
            )
            stable = 0 if busy else stable + 1
            if stable >= 3:
                await self.writer.flush()
                return
        raise TimeoutError("pipeline did not become idle")
