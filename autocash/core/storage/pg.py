"""PostgreSQL access: pool, availability state, migrations, partitions."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from autocash.core.trace.codes import REASONS

log = logging.getLogger(__name__)
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class DbUnavailable(RuntimeError):
    pass


def is_connection_error(exc: BaseException) -> bool:
    """True when the failure means 'PostgreSQL is unavailable', not 'bad statement'."""
    return isinstance(
        exc,
        (
            OSError,
            TimeoutError,
            DbUnavailable,
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            asyncpg.CannotConnectNowError,
            asyncpg.AdminShutdownError,
            asyncpg.CrashShutdownError,
            asyncpg.TooManyConnectionsError,
        ),
    )


class Database:
    def __init__(self, dsn: str, *, max_size: int = 6, command_timeout: float = 10.0) -> None:
        self._dsn = dsn
        self._max_size = max_size
        self._command_timeout = command_timeout
        self._pool: asyncpg.Pool | None = None
        self.available = False
        self.up_event = asyncio.Event()
        self.on_change: Callable[[bool, str], None] | None = None
        self._partitions: set[tuple[int, int]] = set()

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise DbUnavailable("PostgreSQL pool is not created")
        return self._pool

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn, min_size=1, max_size=self._max_size, command_timeout=self._command_timeout,
                timeout=3.0,
            )
        else:
            async with self._pool.acquire(timeout=3.0) as conn:
                await conn.execute("SELECT 1")
        self.mark_up()

    async def close(self) -> None:
        if self._pool is not None:
            pool, self._pool = self._pool, None
            try:
                await asyncio.wait_for(pool.close(), 3.0)
            except Exception:
                pool.terminate()

    def mark_down(self, exc: BaseException) -> None:
        if self.available:
            self.available = False
            self.up_event.clear()
            log.error("PostgreSQL unavailable: %r", exc)
            if self.on_change:
                self.on_change(False, repr(exc))

    def mark_up(self) -> None:
        if not self.available:
            self.available = True
            self.up_event.set()
            if self.on_change:
                self.on_change(True, "")

    async def wait_available(self, retry_s: float = 0.5, max_retry_s: float = 5.0) -> None:
        """Block until PostgreSQL answers; used by every writer after a connection error."""
        delay = retry_s
        while True:
            try:
                await self.connect()
                return
            except Exception as exc:
                if not is_connection_error(exc):
                    # A failure to connect that we do not recognise is still "unavailable": the
                    # caller must keep waiting, not die. It is logged with its traceback.
                    log.error("PostgreSQL: unexpected error while connecting: %r", exc, exc_info=exc)
                self.mark_down(exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_retry_s)

    async def migrate(self) -> list[str]:
        applied: list[str] = []
        async with self.pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            async with conn.transaction():
                await conn.execute("LOCK TABLE schema_migrations IN EXCLUSIVE MODE")
                done = {r["name"] for r in await conn.fetch("SELECT name FROM schema_migrations")}
                for file in sorted(MIGRATIONS_DIR.glob("*.sql")):
                    if file.name in done:
                        continue
                    await conn.execute(file.read_text(encoding="utf-8"))
                    await conn.execute("INSERT INTO schema_migrations (name) VALUES ($1)", file.name)
                    applied.append(file.name)
                await conn.executemany(
                    "INSERT INTO reason_codes (code, meaning, status) VALUES ($1, $2, $3) "
                    "ON CONFLICT (code) DO UPDATE SET meaning = EXCLUDED.meaning, status = EXCLUDED.status",
                    [(str(code), meaning, status) for code, (meaning, status) in REASONS.items()],
                )
        return applied

    async def ensure_partitions(self, conn: asyncpg.Connection, epochs: Iterable[float]) -> None:
        """Create monthly partitions of `messages` for the given message dates."""
        for ts in epochs:
            d = datetime.fromtimestamp(ts, UTC)
            key = (d.year, d.month)
            if key in self._partitions:
                continue
            ny, nm = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS messages_{d.year}_{d.month:02d} PARTITION OF messages "
                f"FOR VALUES FROM ('{d.year}-{d.month:02d}-01+00') TO ('{ny}-{nm:02d}-01+00')"
            )
            self._partitions.add(key)

    def forget_partitions(self) -> None:
        self._partitions.clear()


async def detach_old_partitions(conn: asyncpg.Connection, before: datetime) -> list[str]:
    """Remove outdated data by detaching and dropping monthly partitions (FT-5.4)."""
    rows: list[Any] = await conn.fetch(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'messages' ORDER BY c.relname"
    )
    dropped = []
    for row in rows:
        name: str = row["relname"]
        _, year, month = name.rsplit("_", 2)
        end = datetime(int(year) + (int(month) == 12), int(month) % 12 + 1, 1, tzinfo=UTC)
        if end <= before:
            await conn.execute(f"ALTER TABLE messages DETACH PARTITION {name}")
            await conn.execute(f"DROP TABLE {name}")
            dropped.append(name)
    return dropped
