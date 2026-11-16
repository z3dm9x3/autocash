"""Output: every outgoing message is an outbound task with a trace (section 4.9 of the TZ).

Routing (FT-9.4, FT-9.5):
  * a private dialog is answered only by the account of that dialog; while it is not active
    the task waits until its TTL with ACCOUNT_PAUSED / ACCOUNT_RESTRICTED / ACCOUNT_LOST;
  * a group message or an internal notification is sent by any active account that is in the
    chat and may write there; when the assigned account drops out the task moves to another
    one, and with no suitable account it waits with NO_ACCOUNT_IN_CHAT.
Each account sends its tasks one by one, different accounts send in parallel (FT-9.2).
`random_id` is stored with the task before the send; a retry reuses it and
RANDOM_ID_DUPLICATE means "already delivered" (FT-9.3).
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections import deque
from dataclasses import dataclass
from functools import partial
from typing import Any

import asyncpg

from autocash.core import crashpoints
from autocash.core.config.loader import ConfigStore
from autocash.core.gateway.gateway import OwnMessages
from autocash.core.pool.limiter import CLIENT_REPLY, COMMAND_REPLY, NOTIFY
from autocash.core.pool.pool import Account, AccountPool
from autocash.core.pool.scheduler import SEND, Scheduler
from autocash.core.pool.state import BLOCKED, CHAT_GONE, WAIT_REASON, WRITE_FORBIDDEN, AccountState
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import FloodWait, RandomIdDuplicate, TelegramError
from autocash.core.trace.codes import Reason, Status

log = logging.getLogger(__name__)

# Task priority (FT-9.8: command replies, client replies, notifications, bulk) -> budget class (FT-10.2).
_BUDGET = {0: COMMAND_REPLY, 1: CLIENT_REPLY}

_LOAD = """
SELECT id, priority, chat_id, dialog_account_id, stage, account_id, finished_at,
       extract(epoch FROM expires_at)::float8 AS expires
FROM outbound_tasks WHERE id = $1
"""
_ASSIGN = """
UPDATE outbound_tasks SET stage = 'assigned', status = 'in_progress', account_id = $2, reason_code = NULL,
       wait_until = NULL, assigned_at = now()
WHERE id = $1 AND finished_at IS NULL
"""
_SENDING = """
UPDATE outbound_tasks SET stage = 'sending', status = 'in_progress', account_id = $2, reason_code = NULL,
       wait_until = NULL, assigned_at = coalesce(assigned_at, now()), sending_at = now(), attempts = attempts + 1
WHERE id = $1 AND finished_at IS NULL
RETURNING attempts
"""
_FINISH = """
UPDATE outbound_tasks SET stage = coalesce($2, stage), status = $3, reason_code = $4, finished_at = now(),
       delivered_msg_id = $5, wait_until = NULL
WHERE id = $1 AND finished_at IS NULL
"""
_WAIT = """
UPDATE outbound_tasks SET status = 'waiting', reason_code = $2, last_wait_reason = $2,
       wait_until = CASE WHEN $3::float8 IS NULL THEN expires_at ELSE to_timestamp($3::float8) END
WHERE id = $1 AND finished_at IS NULL
"""
_ATTEMPT = "INSERT INTO outbound_attempts (task_id, account_id, result, error_code) VALUES ($1, $2, $3, $4)"


@dataclass(slots=True, eq=False)
class _Task:
    id: int
    priority: int
    chat_id: int
    dialog_account_id: int | None
    expires: float | None
    pinned: int | None = None  # the account that must repeat a send whose outcome is unknown
    account: int | None = None  # assigned, waiting in that account's queue
    wait_reason: str | None = None
    retry: bool = False  # waiting for a retry of its own, not for the pool
    in_inbox: bool = False
    sending: bool = False
    timer: asyncio.TimerHandle | None = None


class Output:
    def __init__(self, store: ConfigStore, db: Database, writer: BatchWriter, own: OwnMessages,
                 pool: AccountPool, scheduler: Scheduler) -> None:
        self._store = store
        self._db = db
        self._writer = writer
        self._own = own
        self._pool = pool
        self._scheduler = scheduler
        self._inbox: asyncio.Queue[int] = asyncio.Queue()
        self._tasks: dict[int, _Task] = {}
        self._pending: set[int] = set()  # ids in the inbox that are not loaded yet
        self._queues: dict[int, asyncio.PriorityQueue[tuple[int, int]]] = {}
        self._workers: dict[int, asyncio.Task[None]] = {}
        self._router: asyncio.Task[None] | None = None
        self._routing = 0
        self._sending = 0
        self._acc_next: dict[int, float] = {}
        self._chat_next: dict[tuple[int, int], float] = {}
        self._group_window: dict[tuple[int, int], deque[float]] = {}
        self._started = False
        self.delivered = 0
        self.failed = 0

    # ---- intake of tasks -----------------------------------------------------------------
    def enqueue(self, ids: list[int], priority: int = 2) -> None:
        for task_id in ids:
            if task_id in self._tasks:
                self._wake(self._tasks[task_id])
            elif task_id not in self._pending:
                self._pending.add(task_id)
                self._inbox.put_nowait(task_id)

    def _wake(self, task: _Task) -> None:
        if task.in_inbox or task.sending or task.id not in self._tasks:
            return
        self._clear_timer(task)
        task.in_inbox = True
        self._inbox.put_nowait(task.id)

    @staticmethod
    def _clear_timer(task: _Task) -> None:
        if task.timer is not None:
            task.timer.cancel()
            task.timer = None

    def _wake_later(self, task: _Task, delay: float) -> None:
        self._clear_timer(task)

        def fire() -> None:
            task.timer = None
            self._wake(task)

        task.timer = asyncio.get_running_loop().call_later(max(delay, 0.0) + 0.01, fire)

    def _forget(self, task: _Task) -> None:
        self._clear_timer(task)
        self._tasks.pop(task.id, None)

    def on_pool_change(self) -> None:
        """An account or a membership link changed: waiting and queued tasks are routed again (FT-11.8)."""
        if not self._started:
            return
        for task in list(self._tasks.values()):
            if task.sending or task.in_inbox:
                continue
            if task.account is not None:
                if self._usable(task, task.account):
                    continue
                task.account = None
                self._wake(task)
            elif task.wait_reason is not None and not task.retry:
                self._wake(task)

    @property
    def busy(self) -> bool:
        """Work that will proceed by itself; tasks waiting for an account or a pause do not count."""
        return bool(
            self._inbox.qsize() or self._routing or self._sending
            or any(t.account is not None or t.retry for t in self._tasks.values())
        )

    def stats(self) -> dict[str, Any]:
        waiting: dict[str, int] = {}
        for task in self._tasks.values():
            if task.wait_reason:
                waiting[task.wait_reason] = waiting.get(task.wait_reason, 0) + 1
        return {"queued": {acc: q.qsize() for acc, q in self._queues.items()}, "waiting": waiting,
                "delivered": self.delivered, "failed": self.failed}

    # ---- lifecycle -----------------------------------------------------------------------
    async def start(self) -> None:
        await self._db.wait_available()
        # Unfinished tasks continue from their last recorded stage (FT-11.6).
        rows = await self._db.pool.fetch(
            "SELECT id FROM outbound_tasks WHERE finished_at IS NULL ORDER BY priority, id")
        self._started = True
        self.enqueue([row["id"] for row in rows])
        self._router = asyncio.create_task(self._route_loop(), name="output-router")

    async def stop(self) -> None:
        self._started = False
        tasks = [t for t in (self._router, *self._workers.values()) if t is not None]
        for task in self._tasks.values():
            self._clear_timer(task)
        for worker in tasks:
            worker.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._router = None
        self._workers.clear()

    # ---- emergency channel (NFT-18) ------------------------------------------------------
    async def direct_send(self, chat_id: int, text: str, exclude: int | None = None) -> bool:
        """Send without PostgreSQL: alarms and notifications while the database is down.

        `exclude` is the account the alarm is about: another account or a bot is preferred.
        """
        members = self._pool.membership.members(chat_id)
        candidates = [a for a in self._pool.accounts.values()
                      if a.connected and not a.lost and not a.removed and not a.restricted]
        candidates.sort(key=lambda a: (
            a.account_id == exclude, a.state is not AccountState.ACTIVE,
            bool(members) and a.account_id not in members, a.kind != "bot", a.account_id,
        ))
        for acc in candidates[:3]:
            client = acc.client
            random_id = secrets.randbits(63) or 1
            self._own.register(chat_id, text)
            try:
                msg_id = await acc.limiter.call(
                    COMMAND_REPLY, "send", partial(client.send_message, chat_id, text, random_id=random_id))
            except Exception as exc:
                log.error("direct send to %s through %s failed: %r", chat_id, acc.account_id, exc)
                if isinstance(exc, TelegramError) and not isinstance(exc, FloodWait):
                    self._pool.apply_error(acc, exc.code, chat_id)
                continue
            self._own.delivered(chat_id, msg_id)
            return True
        return False

    # ---- routing -------------------------------------------------------------------------
    async def _route_loop(self) -> None:
        while True:
            task_id = await self._inbox.get()
            self._pending.discard(task_id)
            known = self._tasks.get(task_id)
            if known is not None:
                known.in_inbox = False
            self._routing += 1
            try:
                await self._route(task_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_connection_error(exc):
                    self._db.mark_down(exc)
                    await self._db.wait_available()
                else:
                    log.exception("output: routing of task %s failed", task_id)
                    await asyncio.sleep(1.0)
                self.enqueue([task_id])
            finally:
                self._routing -= 1

    async def _route(self, task_id: int) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            row = await self._db.pool.fetchrow(_LOAD, task_id)
            if row is None or row["finished_at"] is not None:
                return
            task = _Task(row["id"], row["priority"], row["chat_id"], row["dialog_account_id"], row["expires"])
            if row["stage"] == "sending":
                # The process died during a send: only the same account can repeat it with the same
                # random_id and learn that the message was already delivered (FT-9.3).
                task.pinned = row["account_id"]
            self._tasks[task_id] = task
        if task.sending:
            return
        self._clear_timer(task)
        task.retry = False
        if task.expires is not None and time.time() >= task.expires:
            # The final status keeps the last reason the task was waiting for (FT-9.6).
            await self._finish(task, None, Status.EXPIRED, Reason.TTL_EXPIRED)
            self.failed += 1
            return
        account_id, reason, until = self._choose(task)
        if account_id is None:
            assert reason is not None
            await self._park(task, str(reason), until)
            return
        task.account, task.wait_reason = account_id, None
        self._writer.submit(lambda conn: conn.execute(_ASSIGN, task_id, account_id))
        queue = self._queues.get(account_id)
        if queue is None:
            queue = self._queues[account_id] = asyncio.PriorityQueue()
        queue.put_nowait((task.priority, task.id))
        if account_id not in self._workers or self._workers[account_id].done():
            self._workers[account_id] = asyncio.create_task(self._worker(account_id), name=f"output-{account_id}")

    def _senders(self, chat_id: int) -> list[int]:
        pool = self._pool
        return [a for a, can_send in pool.membership.members(chat_id).items()
                if can_send and pool.state(a) is AccountState.ACTIVE]

    def _usable(self, task: _Task, account_id: int) -> bool:
        if self._pool.state(account_id) is not AccountState.ACTIVE:
            return False
        return task.chat_id > 0 or task.pinned == account_id or account_id in self._senders(task.chat_id)

    def _dialog_account(self, task: _Task) -> int | None:
        if task.dialog_account_id is not None:
            return task.dialog_account_id
        members = list(self._pool.membership.members(task.chat_id))
        if len(members) == 1:
            return members[0]
        live = self._pool.live()
        return live[0].account_id if len(live) == 1 and not members else None

    def _choose(self, task: _Task) -> tuple[int | None, Reason | None, float | None]:
        """The account for the task, or the reason it has to wait (and until when, if known)."""
        pool = self._pool
        fixed = task.pinned if task.pinned is not None else (
            self._dialog_account(task) if task.chat_id > 0 else None)
        if task.pinned is not None or task.chat_id > 0:
            if fixed is None:
                return None, Reason.NO_ACCOUNT_IN_CHAT, None
            state = pool.state(fixed)
            if state is AccountState.ACTIVE:
                return fixed, None, None
            acc = pool.accounts.get(fixed)
            until = acc.limiter.paused_until() if acc is not None and state is AccountState.PAUSED else None
            return None, WAIT_REASON[state], until
        senders = self._senders(task.chat_id)
        if not senders:
            return None, Reason.NO_ACCOUNT_IN_CHAT, None
        preferred = self._scheduler.account_for(SEND, task.chat_id)
        if preferred in senders:
            return preferred, None, None
        return min(senders, key=lambda a: (self._queues[a].qsize() if a in self._queues else 0, a)), None, None

    async def _park(self, task: _Task, reason: str, until: float | None, retry: bool = False) -> None:
        task.account = None
        task.retry = retry
        if task.wait_reason != reason:
            # Transitions to 'waiting' are written synchronously (invariant 5).
            await self._write(_WAIT, task.id, reason, until)
            task.wait_reason = reason
        wake_at = min((t for t in (until, task.expires) if t is not None), default=None)
        if wake_at is not None:
            self._wake_later(task, wake_at - time.time())

    # ---- sending -------------------------------------------------------------------------
    async def _worker(self, account_id: int) -> None:
        queue = self._queues[account_id]
        while True:
            _, task_id = await queue.get()
            task = self._tasks.get(task_id)
            if task is None or task.account != account_id or task.sending:
                continue  # finished or routed elsewhere meanwhile
            acc = self._pool.accounts.get(account_id)
            task.sending = True
            self._sending += 1
            try:
                if acc is not None and self._usable(task, account_id):
                    await self._pace(acc, task)
                if acc is None or not self._usable(task, account_id):
                    self._release(task)
                    continue
                await self._send(acc, task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_connection_error(exc):
                    self._db.mark_down(exc)
                    await self._db.wait_available()
                else:
                    log.exception("output: task %s failed", task_id)
                    await asyncio.sleep(1.0)
                self._release(task)
            finally:
                task.sending = False
                self._sending -= 1
                if task.id in self._tasks and task.account is None and task.wait_reason is None:
                    self._wake(task)

    def _release(self, task: _Task) -> None:
        """Give the task back to the router (it is woken when the worker lets go of it)."""
        task.account = None
        task.wait_reason = None

    async def _pace(self, acc: Account, task: _Task) -> None:
        """Send tempo by account and by chat (FT-9.7)."""
        cfg = self._store.current.config.output.pacing.get(acc.kind)
        if cfg is None:
            return
        key = (acc.account_id, task.chat_id)
        now = time.monotonic()
        ready = max(self._acc_next.get(acc.account_id, 0.0), self._chat_next.get(key, 0.0))
        if task.chat_id < 0 and cfg.group_per_minute > 0:
            window = self._group_window.setdefault(key, deque())
            while window and now - window[0] >= 60.0:
                window.popleft()
            if len(window) >= cfg.group_per_minute:
                ready = max(ready, window[0] + 60.0)
        if ready > now:
            await asyncio.sleep(ready - now)

    def _paced(self, acc: Account, task: _Task) -> None:
        cfg = self._store.current.config.output.pacing.get(acc.kind)
        if cfg is None:
            return
        key = (acc.account_id, task.chat_id)
        now = time.monotonic()
        self._acc_next[acc.account_id] = now + cfg.per_account_interval_s
        self._chat_next[key] = now + cfg.per_chat_interval_s
        if task.chat_id < 0 and cfg.group_per_minute > 0:
            self._group_window.setdefault(key, deque()).append(now)
        if len(self._chat_next) > 5000:
            self._chat_next = {k: v for k, v in self._chat_next.items() if v > now}

    async def _write(self, sql: str, *args: Any, attempt: tuple[Any, ...] | None = None) -> Any:
        async def fn(conn: asyncpg.Connection) -> Any:
            result = await conn.fetchval(sql, *args) if "RETURNING" in sql else await conn.execute(sql, *args)
            if attempt is not None:
                await conn.execute(_ATTEMPT, *attempt)
            return result

        # Transitions to 'sending', 'waiting' and final statuses are written synchronously (invariant 5).
        return await self._writer.submit_sync(fn)

    async def _finish(self, task: _Task, stage: str | None, status: Status, reason: str | None,
                      msg_id: int | None = None, attempt: tuple[Any, ...] | None = None) -> None:
        await self._write(_FINISH, task.id, stage, str(status), reason, msg_id, attempt=attempt)
        self._forget(task)

    async def _send(self, acc: Account, task: _Task) -> None:
        cfg = self._store.current.config.output
        row = await self._db.pool.fetchrow(
            "SELECT text, random_id, reply_to, finished_at, (expires_at IS NOT NULL AND expires_at < now()) AS expired "
            "FROM outbound_tasks WHERE id = $1", task.id,
        )
        if row is None or row["finished_at"] is not None:
            self._forget(task)
            return
        if row["expired"]:
            await self._finish(task, None, Status.EXPIRED, Reason.TTL_EXPIRED)
            self.failed += 1
            return
        account, client, chat_id = acc.account_id, acc.client, task.chat_id
        attempts = await self._write(_SENDING, task.id, account)
        if attempts is None:
            self._forget(task)
            return
        crashpoints.hit("output_before_send")
        self._own.register(chat_id, row["text"])
        try:
            msg_id: int | None = await acc.limiter.call(
                _BUDGET.get(task.priority, NOTIFY), "send",
                lambda: client.send_message(chat_id, row["text"], random_id=row["random_id"],
                                            reply_to=row["reply_to"]),
            )
            result = "delivered"
        except RandomIdDuplicate:
            msg_id, result = None, "duplicate"  # delivered by a previous attempt
        except FloodWait as exc:
            # The limiter paused the account and the pool moved its duties; the task is routed
            # again: to another account for a group, to waiting for a private dialog.
            await self._write(_ATTEMPT, task.id, account, "flood_wait", exc.args[0])
            task.pinned = None
            self._release(task)
            return
        except TelegramError as exc:
            await self._on_error(acc, task, exc.code, attempts, cfg.max_attempts, cfg.retry_base_s)
            return
        except (OSError, TimeoutError) as exc:
            task.pinned = account  # the outcome is unknown: only this account may repeat the send
            await self._retry(task, account, f"NETWORK:{type(exc).__name__}", attempts, cfg.max_attempts,
                              cfg.retry_base_s)
            return
        crashpoints.hit("output_after_send")
        self._paced(acc, task)
        if msg_id is not None:
            self._own.delivered(chat_id, msg_id)
        await self._finish(task, "delivered", Status.DELIVERED, None, msg_id,
                           attempt=(task.id, account, result, None))
        self.delivered += 1

    async def _on_error(self, acc: Account, task: _Task, code: str, attempts: int, max_attempts: int,
                        retry_base_s: float) -> None:
        account = acc.account_id
        attempt = (task.id, account, "error", code)
        task.pinned = None  # Telegram answered: the message was not delivered by this attempt
        changed = self._pool.apply_error(acc, code, task.chat_id)  # FT-10.5: the pool learns first
        if code in BLOCKED:
            await self._finish(task, "sending", Status.NOT_DELIVERED, Reason.USER_IS_BLOCKED, attempt=attempt)
        elif code in WRITE_FORBIDDEN and (task.chat_id > 0 or not self._senders(task.chat_id)):
            await self._finish(task, "sending", Status.NOT_DELIVERED, Reason.CHAT_WRITE_FORBIDDEN, attempt=attempt)
        elif code in CHAT_GONE and task.chat_id > 0:
            await self._finish(task, "sending", Status.NOT_DELIVERED, Reason.CHAT_DELETED, attempt=attempt)
        elif changed:
            # The account dropped out (restricted, lost, kicked, read-only): another account takes
            # the task, or it waits with a reason code (FT-9.5, FT-11.8).
            await self._write(_ATTEMPT, *attempt)
            self._release(task)
            return
        else:
            await self._retry(task, account, code, attempts, max_attempts, retry_base_s)
            return
        self.failed += 1

    async def _retry(self, task: _Task, account: int, code: str, attempts: int, max_attempts: int,
                     retry_base_s: float) -> None:
        attempt = (task.id, account, "error", code)
        if attempts >= max_attempts:
            # An unknown Telegram code is stored as is to extend the dictionary later (FT-11.5).
            await self._finish(task, "sending", Status.NOT_DELIVERED, code, attempt=attempt)
            self.failed += 1
            return
        delay = retry_base_s * 2 ** (attempts - 1)
        await self._write(_ATTEMPT, *attempt)
        task.wait_reason = None
        await self._park(task, code, time.time() + delay, retry=True)
