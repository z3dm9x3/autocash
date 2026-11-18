"""Protocols: instructions between modules (D-RM-12).

A module that learns a fact (Role Manager: "a new user wrote to an account for the first time")
writes a protocol run with `emit` in the SAME transaction as the fact: a run exists if and only if
the fact was committed, and once per (protocol, subject). The dispatcher then calls the handlers
of the modules that declared the protocol (`Module.protocols`, `Module.on_protocol`), in the order
of the modules, each until it succeeds; a handler that keeps failing marks the run failed and
raises an alarm. Handlers must be idempotent: after a crash a handler may run again.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Sequence
from typing import Any

import asyncpg

from autocash.core.storage.pg import Database
from autocash.modules.api import Module

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
RETRY_S = 30.0


async def emit(conn: asyncpg.Connection, protocol: str, subject: int, data: dict[str, Any]) -> bool:
    """Write a run inside the caller's transaction; False if this protocol already ran for the subject."""
    row = await conn.fetchrow(
        "INSERT INTO protocol_runs (protocol, subject, data) VALUES ($1, $2, $3::jsonb) "
        "ON CONFLICT (protocol, subject) DO NOTHING RETURNING id",
        protocol, subject, json.dumps(data, ensure_ascii=False))
    return row is not None


class Protocols:
    def __init__(self, db: Database, modules: Callable[[], Sequence[Module]], notify: Callable[[str], None]) -> None:
        self._db = db
        self._modules = modules
        self._notify = notify
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.done = 0
        self.failed = 0

    def start(self) -> None:
        if self._task is None:
            self._wake.set()  # runs left by a previous start
            self._task = asyncio.create_task(self._run(), name="protocols")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def wake(self) -> None:
        self._wake.set()

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=RETRY_S)
            except TimeoutError:
                pass
            self._wake.clear()
            try:
                await self.run_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("protocols: dispatch failed, retrying later")

    async def run_pending(self) -> int:
        rows = await self._db.pool.fetch(
            "SELECT id, protocol, subject, data, done_modules, attempts FROM protocol_runs "
            "WHERE status = 'pending' AND next_at <= now() ORDER BY id LIMIT 100")
        for row in rows:
            await self._run_one(row)
        return len(rows)

    async def _run_one(self, row: Any) -> None:
        protocol, subject = row["protocol"], int(row["subject"])
        data = json.loads(row["data"]) if isinstance(row["data"], str) else dict(row["data"])
        done = list(row["done_modules"])
        for module in self._modules():
            if protocol not in module.protocols or module.name in done:
                continue
            try:
                await asyncio.wait_for(module.on_protocol(protocol, subject, data), module.timeout_s)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempts = int(row["attempts"]) + 1
                failed = attempts >= MAX_ATTEMPTS
                await self._db.pool.execute(
                    "UPDATE protocol_runs SET attempts = $2, last_error = $3, done_modules = $4, "
                    "status = CASE WHEN $5 THEN 'failed' ELSE 'pending' END, "
                    "next_at = now() + make_interval(secs => $6) WHERE id = $1",
                    row["id"], attempts, f"{module.name}: {exc!r}"[:500], done, failed, RETRY_S * attempts)
                log.exception("protocol %s for %s: module %s failed", protocol, subject, module.name)
                if failed:
                    self.failed += 1
                    self._notify(f"Протокол {protocol} для {subject}: модуль {module.name} не выполнил его "
                                 f"за {attempts} попыток ({exc!r})")
                return
            done.append(module.name)
        await self._db.pool.execute(
            "UPDATE protocol_runs SET status = 'done', done_modules = $2, done_at = now() WHERE id = $1",
            row["id"], done)
        self.done += 1
