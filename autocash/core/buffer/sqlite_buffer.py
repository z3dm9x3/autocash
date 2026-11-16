"""SQLite reserve buffer (FT-4.1, 4.3, 4.5, 4.6, A-6).

An event is ACCEPTED once it is committed here. All SQLite calls run in one dedicated
thread; the event loop only exchanges futures with it. Appends are group-committed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

REQUIRED_JOURNAL_MODE = "wal"
REQUIRED_SYNCHRONOUS = 2  # FULL
_SYNC_NAMES = {"OFF": 0, "NORMAL": 1, "FULL": 2, "EXTRA": 3}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ukey        TEXT NOT NULL UNIQUE,
    trace_id    TEXT NOT NULL,
    urgent      INTEGER NOT NULL,
    payload     TEXT NOT NULL,
    buffered_at REAL NOT NULL,
    uploaded_at REAL,
    wait_reason TEXT
);
CREATE INDEX IF NOT EXISTS events_pending ON events (seq) WHERE uploaded_at IS NULL;
CREATE INDEX IF NOT EXISTS events_uploaded ON events (uploaded_at) WHERE uploaded_at IS NOT NULL;
"""


class BufferPragmaError(RuntimeError):
    """Actual journal_mode / synchronous differ from the required ones (FT-4.5)."""


@dataclass(slots=True)
class BufferRow:
    seq: int
    trace_id: str
    payload: dict[str, Any]
    buffered_at: float
    wait_reason: str | None


@dataclass(slots=True)
class _Op:
    kind: str
    arg: Any
    fut: asyncio.Future[Any]


class SqliteBuffer:
    def __init__(
        self,
        path: Path,
        *,
        journal_mode: str = "WAL",
        synchronous: str = "FULL",
        soft_limit_mb: int = 512,
        hard_limit_mb: int = 1024,
        top_priority: int = 1,
    ) -> None:
        self.path = path
        self._journal_mode = journal_mode
        self._synchronous = synchronous
        self._soft = soft_limit_mb * 1024 * 1024
        self._hard = hard_limit_mb * 1024 * 1024
        self._top_priority = top_priority
        self._ops: queue.SimpleQueue[_Op | None] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.level = "ok"  # ok | soft | hard
        self.dropped = 0
        self.pending_event = asyncio.Event()
        self.urgent_event = asyncio.Event()
        self.on_level: Callable[[str, int], None] | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        ready: asyncio.Future[None] = self._loop.create_future()
        self._thread = threading.Thread(target=self._run, args=(ready,), name="sqlite-buffer", daemon=True)
        self._thread.start()
        await ready

    async def stop(self) -> None:
        if self._thread is not None:
            self._ops.put(None)
            await asyncio.to_thread(self._thread.join)
            self._thread = None

    def set_limits(self, soft_limit_mb: int, hard_limit_mb: int, top_priority: int) -> None:
        self._soft, self._hard = soft_limit_mb * 1024 * 1024, hard_limit_mb * 1024 * 1024
        self._top_priority = top_priority

    # ---- API used from the event loop -------------------------------------------------
    def _submit(self, kind: str, arg: Any) -> asyncio.Future[Any]:
        assert self._loop is not None
        fut: asyncio.Future[Any] = self._loop.create_future()
        self._ops.put(_Op(kind, arg, fut))
        return fut

    def append(self, record: dict[str, Any]) -> asyncio.Future[int]:
        """Enqueue synchronously (keeps arrival order); await the result for durability.

        Resolves to 1 when stored, 0 when the same event is already in the buffer, -1 when the
        record was rejected by the disk limit.
        """
        return self._submit("append", record)

    def fetch_pending(self, limit: int) -> asyncio.Future[list[BufferRow]]:
        return self._submit("fetch", limit)

    def mark_uploaded(self, seqs: list[int]) -> asyncio.Future[None]:
        return self._submit("mark", seqs)

    def set_wait_reason(self, reason: str | None) -> asyncio.Future[int]:
        """Mark all not yet uploaded events as waiting with a reason code (item 3.3)."""
        return self._submit("wait", reason)

    def purge(self, retention_s: float) -> asyncio.Future[int]:
        return self._submit("purge", retention_s)

    def stats(self) -> asyncio.Future[dict[str, Any]]:
        return self._submit("stats", None)

    # ---- buffer thread ------------------------------------------------------------------
    def _open(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        conn.execute(f"PRAGMA journal_mode={self._journal_mode}")
        conn.execute(f"PRAGMA synchronous={self._synchronous}")
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        sync = int(conn.execute("PRAGMA synchronous").fetchone()[0])
        if mode != REQUIRED_JOURNAL_MODE or sync != REQUIRED_SYNCHRONOUS:
            conn.close()
            raise BufferPragmaError(
                f"SQLite: journal_mode={mode}, synchronous={sync}; "
                f"требуется journal_mode={REQUIRED_JOURNAL_MODE}, synchronous={REQUIRED_SYNCHRONOUS} (FULL)"
            )
        conn.executescript(_SCHEMA)
        return conn

    def _resolve(self, fut: asyncio.Future[Any], value: Any = None, exc: BaseException | None = None) -> None:
        def _set() -> None:
            if fut.done():
                return
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(value)

        assert self._loop is not None
        self._loop.call_soon_threadsafe(_set)

    def _run(self, ready: asyncio.Future[None]) -> None:
        try:
            conn = self._open()
        except BaseException as exc:
            self._resolve(ready, exc=exc)
            return
        self._resolve(ready)
        assert self._loop is not None
        try:
            while True:
                op = self._ops.get()
                if op is None:
                    break
                batch = [op]
                while len(batch) < 2000:
                    try:
                        nxt = self._ops.get_nowait()
                    except queue.Empty:
                        break
                    if nxt is None:
                        self._ops.put(None)
                        break
                    batch.append(nxt)
                self._process(conn, batch)
        finally:
            conn.close()

    def _process(self, conn: sqlite3.Connection, batch: list[_Op]) -> None:
        i = 0
        while i < len(batch):
            if batch[i].kind == "append":
                j = i
                while j < len(batch) and batch[j].kind == "append":
                    j += 1
                self._append(conn, batch[i:j])
                i = j
                continue
            op = batch[i]
            i += 1
            try:
                self._resolve(op.fut, self._single(conn, op))
            except Exception as exc:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                log.exception("sqlite buffer: %s failed", op.kind)
                self._resolve(op.fut, exc=exc)

    def _size(self) -> int:
        total = 0
        for suffix in ("", "-wal"):
            try:
                total += Path(str(self.path) + suffix).stat().st_size
            except OSError:
                pass
        return total

    def _update_level(self, conn: sqlite3.Connection) -> None:
        size = self._size()
        pending = 0
        if size >= self._soft:
            # The file does not shrink on purge: only a real backlog counts against the limit.
            pending = int(conn.execute("SELECT count(*) FROM events WHERE uploaded_at IS NULL").fetchone()[0])
        level = "ok"
        if pending:
            level = "hard" if size >= self._hard else "soft"
        if level != self.level:
            self.level = level
            if self.on_level and self._loop:
                self._loop.call_soon_threadsafe(self.on_level, level, size)

    def _append(self, conn: sqlite3.Connection, ops: list[_Op]) -> None:
        self._update_level(conn)
        now = time.time()
        results: list[int] = []  # 1 stored, 0 duplicate, -1 rejected by the disk limit
        urgent = False
        try:
            conn.execute("BEGIN IMMEDIATE")
            for op in ops:
                rec = op.arg
                if self.level == "hard" and rec.get("src_priority", 5) > self._top_priority:
                    results.append(-1)
                    continue
                rec["buffered_at"] = now
                cur = conn.execute(
                    "INSERT OR IGNORE INTO events (ukey, trace_id, urgent, payload, buffered_at) VALUES (?,?,?,?,?)",
                    (rec["ukey"], rec["trace_id"], int(bool(rec.get("is_command"))),
                     json.dumps(rec, ensure_ascii=False), now),
                )
                urgent = urgent or bool(rec.get("is_command"))
                results.append(1 if cur.rowcount else 0)
            conn.execute("COMMIT")
        except Exception as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            log.exception("sqlite buffer: append failed")
            for op in ops:
                self._resolve(op.fut, exc=exc)
            return
        self.dropped += results.count(-1)
        assert self._loop is not None
        for op, ok in zip(ops, results, strict=True):
            self._resolve(op.fut, ok)
        if urgent:
            self._loop.call_soon_threadsafe(self.urgent_event.set)
        self._loop.call_soon_threadsafe(self.pending_event.set)

    def _single(self, conn: sqlite3.Connection, op: _Op) -> Any:
        if op.kind == "fetch":
            rows = conn.execute(
                "SELECT seq, trace_id, payload, buffered_at, wait_reason FROM events "
                "WHERE uploaded_at IS NULL ORDER BY seq LIMIT ?",
                (op.arg,),
            ).fetchall()
            return [BufferRow(r[0], r[1], json.loads(r[2]), r[3], r[4]) for r in rows]
        if op.kind == "mark":
            now = time.time()
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany("UPDATE events SET uploaded_at=? WHERE seq=?", [(now, s) for s in op.arg])
            conn.execute("COMMIT")
            return None
        if op.kind == "wait":
            cur = conn.execute(
                "UPDATE events SET wait_reason=? WHERE uploaded_at IS NULL AND wait_reason IS NOT ?",
                (op.arg, op.arg),
            )
            return cur.rowcount
        if op.kind == "purge":
            cur = conn.execute(
                "DELETE FROM events WHERE uploaded_at IS NOT NULL AND uploaded_at < ?", (time.time() - op.arg,)
            )
            return cur.rowcount
        if op.kind == "stats":
            pending, total = conn.execute(
                "SELECT count(*) FILTER (WHERE uploaded_at IS NULL), count(*) FROM events"
            ).fetchone()
            waiting = conn.execute(
                "SELECT wait_reason, count(*) FROM events WHERE uploaded_at IS NULL AND wait_reason IS NOT NULL "
                "GROUP BY wait_reason"
            ).fetchall()
            return {"pending": pending, "total": total, "waiting": dict(waiting), "size": self._size(),
                    "level": self.level, "dropped": self.dropped}
        raise ValueError(op.kind)
