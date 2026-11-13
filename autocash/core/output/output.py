"""Output: every outgoing message is an outbound task with a trace (FT-9.1, FT-9.3).

Stage 1a: one account sends sequentially. `random_id` is stored with the task before the
send; a retry reuses it and RANDOM_ID_DUPLICATE means "already delivered". Account routing,
pacing per chat and TTL waiting by account state arrive with stage 1b.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from typing import Any

import asyncpg

from autocash.core import crashpoints
from autocash.core.config.loader import ConfigStore
from autocash.core.gateway.gateway import OwnMessages
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import FloodWait, Limiter, RandomIdDuplicate, TelegramClient, TelegramError
from autocash.core.trace.codes import Reason, Status

log = logging.getLogger(__name__)

# Telegram error -> (status, reason code). Unknown codes are stored as is (FT-11.5).
_FINAL_ERRORS: dict[str, Reason] = {
    "CHAT_WRITE_FORBIDDEN": Reason.CHAT_WRITE_FORBIDDEN,
    "USER_BANNED_IN_CHANNEL": Reason.CHAT_WRITE_FORBIDDEN,
    "CHANNEL_PRIVATE": Reason.CHAT_DELETED,
    "USER_KICKED": Reason.CHAT_DELETED,
    "USER_IS_BLOCKED": Reason.USER_IS_BLOCKED,
}
_WAIT_ERRORS: dict[str, Reason] = {
    "PEER_FLOOD": Reason.ACCOUNT_RESTRICTED,
    "USER_DEACTIVATED_BAN": Reason.ACCOUNT_LOST,
    "AUTH_KEY_UNREGISTERED": Reason.ACCOUNT_LOST,
}

_SENDING = """
UPDATE outbound_tasks SET stage = 'sending', status = 'in_progress', account_id = $2, wait_until = NULL,
       assigned_at = coalesce(assigned_at, now()), sending_at = now(), attempts = attempts + 1
WHERE id = $1 AND finished_at IS NULL
RETURNING attempts
"""

_FINISH = """
UPDATE outbound_tasks SET stage = $2, status = $3, reason_code = $4, finished_at = now(), delivered_msg_id = $5,
       wait_until = NULL
WHERE id = $1 AND finished_at IS NULL
"""

_WAIT = """
UPDATE outbound_tasks SET status = 'waiting', reason_code = $2, last_wait_reason = $2,
       wait_until = now() + make_interval(secs => $3::float8)
WHERE id = $1 AND finished_at IS NULL
"""

_ATTEMPT = "INSERT INTO outbound_attempts (task_id, account_id, result, error_code) VALUES ($1, $2, $3, $4)"


class Output:
    def __init__(self, store: ConfigStore, db: Database, writer: BatchWriter, own: OwnMessages) -> None:
        self._store = store
        self._db = db
        self._writer = writer
        self._own = own
        self._queue: asyncio.PriorityQueue[tuple[int, int]] = asyncio.PriorityQueue()
        self._queued: set[int] = set()
        self._client: TelegramClient | None = None
        self._limiter: Limiter | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._timers: set[asyncio.TimerHandle] = set()
        self.delivered = 0
        self.busy = False
        self.failed = 0

    def set_account(self, client: TelegramClient, limiter: Limiter) -> None:
        self._client, self._limiter = client, limiter

    def enqueue(self, ids: list[int], priority: int = 2) -> None:
        for task_id in ids:
            if task_id not in self._queued:
                self._queued.add(task_id)
                self._queue.put_nowait((priority, task_id))

    def _later(self, delay: float, task_id: int, priority: int) -> None:
        handle: asyncio.TimerHandle | None = None

        def fire() -> None:
            if handle is not None:
                self._timers.discard(handle)
            self.enqueue([task_id], priority)

        handle = asyncio.get_running_loop().call_later(delay, fire)
        self._timers.add(handle)

    async def start(self) -> None:
        await self._db.wait_available()
        # Unfinished tasks continue from their last recorded stage (FT-11.6); a task caught in
        # 'sending' is repeated with the same random_id.
        rows = await self._db.pool.fetch(
            "SELECT id, priority FROM outbound_tasks WHERE finished_at IS NULL ORDER BY priority, id"
        )
        for row in rows:
            self.enqueue([row["id"]], row["priority"])
        self._tasks.append(asyncio.create_task(self._worker(), name="output"))

    async def stop(self) -> None:
        for handle in self._timers:
            handle.cancel()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def direct_send(self, chat_id: int, text: str) -> bool:
        """Emergency channel: send without PostgreSQL (alerts while the database is down)."""
        if self._client is None or self._limiter is None:
            return False
        client = self._client
        random_id = secrets.randbits(63) or 1
        self._own.register(chat_id, text)
        try:
            await self._limiter.call(0, "send", lambda: client.send_message(chat_id, text, random_id=random_id))
            return True
        except Exception:
            log.exception("direct send to %s failed", chat_id)
            return False

    async def _worker(self) -> None:
        while True:
            priority, task_id = await self._queue.get()
            self._queued.discard(task_id)
            self.busy = True
            try:
                await self._send(task_id, priority)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_connection_error(exc):
                    self._db.mark_down(exc)
                    await self._db.wait_available()
                else:
                    log.exception("output: task %s failed", task_id)
                self._later(1.0, task_id, priority)
            finally:
                self.busy = False

    async def _write(self, sql: str, *args: Any, attempt: tuple[Any, ...] | None = None) -> Any:
        async def fn(conn: asyncpg.Connection) -> Any:
            result = await conn.fetchval(sql, *args) if "RETURNING" in sql else await conn.execute(sql, *args)
            if attempt is not None:
                await conn.execute(_ATTEMPT, *attempt)
            return result

        # Transitions to 'sending', 'waiting' and final statuses are written synchronously (invariant 5).
        return await self._writer.submit_sync(fn)

    async def _send(self, task_id: int, priority: int) -> None:
        if self._client is None or self._limiter is None:
            self._later(1.0, task_id, priority)
            return
        client, limiter = self._client, self._limiter
        cfg = self._store.current.config.output
        row = await self._db.pool.fetchrow(
            "SELECT *, (expires_at IS NOT NULL AND expires_at < now()) AS expired FROM outbound_tasks WHERE id = $1",
            task_id,
        )
        if row is None or row["finished_at"] is not None:
            return
        account = client.account_id
        if row["expired"]:
            # Final status keeps the last reason the task was waiting for (FT-9.6).
            await self._write(_FINISH, task_id, row["stage"], str(Status.EXPIRED), str(Reason.TTL_EXPIRED), None)
            self.failed += 1
            return
        attempts = await self._write(_SENDING, task_id, account)
        if attempts is None:
            return
        crashpoints.hit("output_before_send")
        self._own.register(row["chat_id"], row["text"])
        try:
            msg_id: int | None = await limiter.call(
                priority, "send",
                lambda: client.send_message(row["chat_id"], row["text"], random_id=row["random_id"],
                                            reply_to=row["reply_to"]),
            )
            result = "delivered"
        except RandomIdDuplicate:
            msg_id, result = None, "duplicate"  # delivered by a previous attempt
        except FloodWait as exc:
            await self._write(_WAIT, task_id, str(Reason.FLOOD_WAIT), exc.seconds,
                              attempt=(task_id, account, "flood_wait", exc.args[0]))
            self._later(exc.seconds, task_id, priority)
            return
        except TelegramError as exc:
            await self._on_error(task_id, priority, account, exc.code, attempts, cfg.max_attempts, cfg.retry_base_s)
            return
        except (OSError, TimeoutError) as exc:
            await self._on_error(task_id, priority, account, f"NETWORK:{type(exc).__name__}", attempts,
                                 cfg.max_attempts, cfg.retry_base_s)
            return
        crashpoints.hit("output_after_send")
        await self._write(_FINISH, task_id, "delivered", str(Status.DELIVERED), None, msg_id,
                          attempt=(task_id, account, result, None))
        self.delivered += 1

    async def _on_error(self, task_id: int, priority: int, account: int, code: str, attempts: int,
                        max_attempts: int, retry_base_s: float) -> None:
        attempt = (task_id, account, "error", code)
        final = _FINAL_ERRORS.get(code)
        if final is not None:
            await self._write(_FINISH, task_id, "sending", str(Status.NOT_DELIVERED), str(final), None,
                              attempt=attempt)
            self.failed += 1
            return
        wait = _WAIT_ERRORS.get(code)
        if wait is not None:
            await self._write(_WAIT, task_id, str(wait), 30.0, attempt=attempt)
            self._later(30.0, task_id, priority)
            return
        if attempts >= max_attempts:
            # An unknown Telegram code is stored as is to extend the dictionary later (FT-11.5).
            await self._write(_FINISH, task_id, "sending", str(Status.NOT_DELIVERED), code, None, attempt=attempt)
            self.failed += 1
            return
        delay = retry_base_s * 2 ** (attempts - 1)
        await self._write(_WAIT, task_id, code, delay, attempt=attempt)
        self._later(delay, task_id, priority)
