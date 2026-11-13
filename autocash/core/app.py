"""Application wiring: Gateway -> Extractor -> SQLite -> Uploader -> PostgreSQL -> Sorter ->
Queue manager -> Modules -> Output (item 3.1 of the TZ)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import asyncpg

from autocash.core.alerts import Alerts
from autocash.core.buffer.sqlite_buffer import BufferPragmaError, SqliteBuffer
from autocash.core.config.loader import ConfigStore, Runtime
from autocash.core.extractor.extractor import CompiledManifest, extract
from autocash.core.gateway.gateway import Gateway, OwnMessages
from autocash.core.output.output import Output
from autocash.core.queue.manager import QueueManager
from autocash.core.sorter.sorter import Sorter
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import Limiter, SimpleLimiter, TelegramClient
from autocash.core.uploader.uploader import Uploader
from autocash.modules.api import Module

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
        client: TelegramClient,
        modules: list[Module],
        limiter: Limiter | None = None,
        maintenance_interval_s: float = 60.0,
    ) -> None:
        self.store = ConfigStore(config_path)
        cfg = self.store.current.config
        self.client = client
        self.limiter: Limiter = limiter or SimpleLimiter(
            cfg.output.concurrency, cfg.output.min_interval_ms / 1000
        )
        self.buffer = SqliteBuffer(
            sqlite_path, journal_mode=cfg.buffer.journal_mode, synchronous=cfg.buffer.synchronous,
            soft_limit_mb=cfg.buffer.soft_limit_mb, hard_limit_mb=cfg.buffer.hard_limit_mb,
            top_priority=cfg.buffer.top_priority,
        )
        self.db = Database(pg_dsn)
        self.writer = BatchWriter(self.db, flush_interval_ms=cfg.writer.flush_interval_ms,
                                  batch_max=cfg.writer.batch_max)
        self.own = OwnMessages()
        self.output = Output(self.store, self.db, self.writer, self.own)
        self.output.set_account(client, self.limiter)
        self.alerts = Alerts(self.store, self.db, self.writer, self.output)
        self.gateway = Gateway(self.store, self.buffer, self.own, self.alerts.logs)
        self.queues = QueueManager(self.store, self.db, self.writer, self.output.enqueue, self.alerts.logs)
        for module in modules:
            self.queues.register(module)
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
            await self.client.start(_ignore)
            self.alerts.alarm(f"Остановка: {exc}")
            await asyncio.sleep(0)
            await asyncio.gather(*self.alerts._tasks, return_exceptions=True)
            await self.client.stop()
            raise
        # Intake into SQLite starts immediately and does not depend on PostgreSQL (NFT-17).
        self.gateway.pool_user_ids.add(self.client.account_id)
        await self.client.start(self.gateway.on_event)
        self.gateway.pool_user_ids.add(self.client.account_id)
        self._tasks.append(asyncio.create_task(self._start_pipeline(), name="pipeline-start"))

    async def _start_pipeline(self) -> None:
        await self.db.wait_available()
        await self.db.migrate()
        self.writer.start()
        self._record_config(self.store.current)
        await self.queues.start()
        self.sorter.start()
        await self.sorter.recover()
        await self.output.start()
        self.uploader.start()
        self._tasks.append(asyncio.create_task(self._maintenance(), name="maintenance"))
        self.ready_at = time.time()
        self.ready.set()
        log.info("pipeline ready in %.2f s", self.ready_at - self.started_at)

    async def stop(self) -> None:
        await self.client.stop()
        for task in [*self._tasks, *self._bg]:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._bg, return_exceptions=True)
        await self.uploader.stop()
        await self.sorter.stop()
        await self.queues.stop()
        await self.output.stop()
        await self.writer.stop(drain=False)
        await self.buffer.stop()
        await self.db.close()

    # ---- reactions to state changes -----------------------------------------------------------
    def reload_config(self) -> tuple[bool, str]:
        """Hot reload (FT-13.3). An invalid config is not applied; the error goes to `logs`."""
        return self.store.reload()

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
                or self.uploader.busy or self.sorter.busy or self.output.busy or self.output._timers
                or any(q["size"] or q["running"] or q["overflow"] for q in queues.values())
                or self.output._queue.qsize() or self.output._queued
            )
            stable = 0 if busy else stable + 1
            if stable >= 3:
                await self.writer.flush()
                return
        raise TimeoutError("pipeline did not become idle")


async def _ignore(event: Any) -> None:
    return None
