"""Shared batch writer (A-4): many small writes -> one transaction (group commit).

`submit` is fire-and-forget (intermediate stages). `submit_sync` returns after the batch is
committed (transitions to waiting / sending / final statuses, invariant 5). A sync item
flushes the batch immediately; async-only batches wait for the flush interval.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import asyncpg

from autocash.core.storage.pg import Database, is_connection_error

log = logging.getLogger(__name__)

WriteFn = Callable[[asyncpg.Connection], Awaitable[Any]]
AfterFn = Callable[[Any], None]
ErrorFn = Callable[[BaseException], None]
E = TypeVar("E")


@dataclass(slots=True)
class _Item:
    fn: WriteFn
    fut: asyncio.Future[Any] | None
    after: AfterFn | None
    on_error: ErrorFn | None = None
    result: Any = None
    error: BaseException | None = None


class BatchWriter:
    def __init__(self, db: Database, *, flush_interval_ms: int = 20, batch_max: int = 500) -> None:
        self._db = db
        self.flush_interval = flush_interval_ms / 1000
        self.batch_max = batch_max
        self._items: deque[_Item] = deque()
        self._wake = asyncio.Event()
        self._urgent = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self.batches = 0
        self.last_batch_s = 0.0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="batch-writer")

    async def stop(self, *, drain: bool = True) -> None:
        self._stopping = True
        self._wake.set()
        self._urgent.set()
        if self._task is not None:
            if not drain:
                self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def __len__(self) -> int:
        return len(self._items)

    def submit(self, fn: WriteFn, after: AfterFn | None = None, on_error: ErrorFn | None = None, *,
               urgent: bool = False) -> None:
        self._items.append(_Item(fn, None, after, on_error))
        self._wake.set()
        if urgent:
            self._urgent.set()

    async def submit_sync(self, fn: WriteFn, after: AfterFn | None = None) -> Any:
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._items.append(_Item(fn, fut, after))
        self._wake.set()
        self._urgent.set()
        return await fut

    async def flush(self) -> None:
        """Wait until everything submitted so far is committed."""

        async def _noop(conn: asyncpg.Connection) -> None:
            return None

        await self.submit_sync(_noop)

    async def _run(self) -> None:
        while True:
            if not self._items:
                if self._stopping:
                    return
                self._wake.clear()
                await self._wake.wait()
                continue
            if not self._urgent.is_set() and len(self._items) < self.batch_max and not self._stopping:
                try:
                    await asyncio.wait_for(self._urgent.wait(), self.flush_interval)
                except TimeoutError:
                    pass
            self._urgent.clear()
            batch = [self._items.popleft() for _ in range(min(len(self._items), self.batch_max))]
            if self._items:
                self._urgent.set()
            await self._write(batch)

    async def _write(self, batch: list[_Item]) -> None:
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            try:
                async with self._db.pool.acquire() as conn:
                    try:
                        # Fast path: the whole batch in one transaction without savepoints.
                        async with conn.transaction():
                            for item in batch:
                                item.result, item.error = None, None
                                item.result = await item.fn(conn)
                    except Exception as exc:
                        if is_connection_error(exc):
                            raise
                        # Slow path: isolate the failing item with savepoints, keep the rest.
                        async with conn.transaction():
                            for item in batch:
                                item.result, item.error = None, None
                                try:
                                    async with conn.transaction():
                                        item.result = await item.fn(conn)
                                except Exception as item_exc:
                                    if is_connection_error(item_exc):
                                        raise
                                    item.error = item_exc
                break
            except Exception as exc:
                if not is_connection_error(exc):
                    log.exception("batch writer: unexpected failure, batch is retried")
                    await asyncio.sleep(0.5)
                    continue
                self._db.mark_down(exc)
                if self._stopping:
                    for item in batch:
                        if item.fut and not item.fut.done():
                            item.fut.set_exception(exc)
                    return
                await self._db.wait_available()
        self.batches += 1
        self.last_batch_s = loop.time() - started
        for item in batch:
            if item.error is not None:
                log.error("batch writer: item failed: %r", item.error)
                if item.on_error is not None:
                    item.on_error(item.error)
                if item.fut and not item.fut.done():
                    item.fut.set_exception(item.error)
                continue
            if item.after is not None:
                try:
                    item.after(item.result)
                except Exception:
                    log.exception("batch writer: after-commit callback failed")
            if item.fut and not item.fut.done():
                item.fut.set_result(item.result)


@dataclass(slots=True)
class _Group(Generic[E]):
    entries: list[E] = field(default_factory=list)
    futures: list[asyncio.Future[Any] | None] = field(default_factory=list)
    closed: bool = False


class Aggregator(Generic[E]):
    """Collects entries from many callers into ONE writer item (one round of statements).

    `write_batch(conn, entries)` returns a list of per-entry results; an Exception instance in
    that list fails only its own entry.
    """

    def __init__(
        self,
        writer: BatchWriter,
        write_batch: Callable[[asyncpg.Connection, list[E]], Awaitable[list[Any]]],
        *,
        urgent: bool,
    ) -> None:
        self._writer = writer
        self._write_batch = write_batch
        self._urgent = urgent
        self._group: _Group[E] | None = None

    def _add(self, entry: E, fut: asyncio.Future[Any] | None) -> None:
        group = self._group
        if group is None or group.closed:
            group = self._group = _Group()
            self._submit(group)
        group.entries.append(entry)
        group.futures.append(fut)

    def add(self, entry: E) -> None:
        self._add(entry, None)

    async def add_sync(self, entry: E) -> Any:
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._add(entry, fut)
        return await fut

    def _submit(self, group: _Group[E]) -> None:
        async def fn(conn: asyncpg.Connection) -> list[Any]:
            group.closed = True  # later entries start a new group; a retry reuses this one
            return await self._write_batch(conn, group.entries)

        def after(results: list[Any]) -> None:
            for fut, result in zip(group.futures, results, strict=True):
                if fut is None or fut.done():
                    continue
                if isinstance(result, BaseException):
                    fut.set_exception(result)
                else:
                    fut.set_result(result)

        def on_error(exc: BaseException) -> None:
            group.closed = True
            for fut in group.futures:
                if fut is not None and not fut.done():
                    fut.set_exception(exc)

        self._writer.submit(fn, after, on_error, urgent=self._urgent)
