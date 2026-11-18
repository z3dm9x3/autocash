"""Outbox (OBX): the only component that writes to Telegram (OBX_readme.md, D-OBX-1).

Modules create tasks (OBX_tasks); the Outbox sends them by its rules:
  * OBX_Router (OBX_router.py): a task bound to an account (a private dialog, an account named by
    the module) goes to that account; while it is not active the task waits until its TTL with
    ACCOUNT_PAUSED / ACCOUNT_RESTRICTED / ACCOUNT_LOST. An unbound task (a group, a notification)
    goes to the least loaded account that may write there; with none it waits NO_ACCOUNT_IN_CHAT;
  * OBX_Antifloodwaiter (OBX_antiflood.py): writes of one account are spread evenly, within the
    limits of the configuration; a FloodWait pauses the account on top of it;
  * OBX_Finalized: a write is confirmed when Telegram returns the message number. No number within
    `confirm_timeout_s` (the connection or the account was lost): the same account repeats the send
    with the same random_id, and RANDOM_ID_DUPLICATE means "already delivered" (FT-9.3). A blocked
    user, a gone chat, a forbidden write: final "not delivered" at once. Past the TTL: final
    "expired". Final failures are reported to the group logs (throttled) and to the monitor log.
Each account sends its tasks one by one, different accounts send in parallel (FT-9.2).
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

import asyncpg

from autocash.core import crashpoints
from autocash.core.config.loader import ConfigStore
from autocash.core.gateway.gateway import OwnMessages
from autocash.core.outbox.OBX_antiflood import Antiflood
from autocash.core.outbox.OBX_router import Decision, choose
from autocash.core.outbox.OBX_tasks import OutboundDraft, insert_outbound
from autocash.core.pool.limiter import CLIENT_REPLY, COMMAND_REPLY, NOTIFY
from autocash.core.pool.pool import Account, AccountPool
from autocash.core.pool.scheduler import SEND, Scheduler
from autocash.core.pool.state import BLOCKED, CHAT_GONE, WAIT_REASON, WRITE_FORBIDDEN, AccountState
from autocash.core.storage.pg import Database, is_connection_error
from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import FloodWait, RandomIdDuplicate, TelegramError
from autocash.core.trace.codes import Reason, Status

log = logging.getLogger(__name__)

# Task kind -> API budget class of the pool limiter (FT-10.2).
_BUDGET = {"command_reply": COMMAND_REPLY, "client_reply": CLIENT_REPLY}

_LOAD = """
SELECT id, priority, chat_id, dialog_account_id, stage, account_id, finished_at, kind, action, binding,
       extract(epoch FROM expires_at)::float8 AS expires
FROM outbound_tasks WHERE id = $1
"""
_ASSIGN = """
UPDATE outbound_tasks SET stage = 'assigned', status = 'in_progress', account_id = $2, reason_code = NULL,
       wait_until = NULL, assigned_at = now(), router_used = $3, router_load = $4::jsonb
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
       delivered_msg_id = $5, wait_until = NULL, antiflood_wait_ms = antiflood_wait_ms + $6,
       confirmed_at = CASE WHEN $3 = 'delivered' THEN now() ELSE confirmed_at END
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
    kind: str = "notify"
    action: str = "send"
    bound: bool = False  # binding dialog / explicit: only dialog_account_id may send it
    pinned: int | None = None  # the account that must repeat a send whose outcome is unknown
    account: int | None = None  # assigned, waiting in that account's queue
    wait_reason: str | None = None
    retry: bool = False  # waiting for a retry of its own, not for the pool
    in_inbox: bool = False
    sending: bool = False
    timer: asyncio.TimerHandle | None = None
    waited_ms: int = 0  # held back by the antiflood


class Outbox:
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
        self.antiflood = Antiflood()
        self._started = False
        # Final failures go to the group logs (set by the App), throttled; never about the logs themselves.
        self.on_failure: Callable[[str], None] | None = None
        self._failure_last = 0.0
        self._failure_count = 0
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

    def restart(self, ids: list[int]) -> None:
        """Tasks reopened or released by /retry: reload them from PostgreSQL and route again."""
        for task_id in ids:
            task = self._tasks.get(task_id)
            if task is not None and task.sending:
                continue  # its worker is about to send it anyway
            if task is not None:
                self._forget(task)
        self.enqueue(ids)

    def cancelled(self, ids: list[int]) -> None:
        """Tasks cancelled by /cancel are dropped from memory (a worker skips them)."""
        for task_id in ids:
            task = self._tasks.get(task_id)
            if task is not None and not task.sending:
                self._forget(task)

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
        # The antiflood window survives a restart: the writes of the last minute (D-OBX-2).
        recent = await self._db.pool.fetch(
            "SELECT a.account_id, t.chat_id, extract(epoch FROM now() - a.at)::float8 AS age FROM outbound_attempts a "
            "JOIN outbound_tasks t ON t.id = a.task_id WHERE a.at > now() - interval '60 seconds' "
            "AND a.result IN ('delivered', 'duplicate', 'pinned') AND a.account_id IS NOT NULL ORDER BY a.at")
        now = time.monotonic()
        for rec in recent:
            self.antiflood.wrote(None, int(rec["account_id"]), int(rec["chat_id"]), at=now - float(rec["age"]))
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
            task = _Task(row["id"], row["priority"], row["chat_id"], row["dialog_account_id"], row["expires"],
                         row["kind"], row["action"], row["binding"] != "none")
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
        decision, reason, until = self._choose(task)
        if decision is None:
            assert reason is not None
            await self._park(task, str(reason), until)
            return
        account_id = decision.account_id
        task.account, task.wait_reason = account_id, None
        load = json.dumps({str(k): v for k, v in decision.load.items()}) if decision.load else None
        self._writer.submit(lambda conn: conn.execute(_ASSIGN, task_id, account_id, decision.routed, load))
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
        if task.bound and task.dialog_account_id is not None and account_id != task.dialog_account_id:
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

    def _choose(self, task: _Task) -> tuple[Decision | None, Reason | None, float | None]:
        """The account for the task (OBX_Router), or the reason it has to wait (and until when)."""
        pool = self._pool
        if task.pinned is not None:
            fixed: int | None = task.pinned
        elif task.chat_id > 0:
            fixed = self._dialog_account(task)
        elif task.bound:
            fixed = task.dialog_account_id  # an account named by the module
        else:
            fixed = None
        if task.pinned is not None or task.chat_id > 0 or task.bound:
            if fixed is None:
                return None, Reason.NO_ACCOUNT_IN_CHAT, None
            state = pool.state(fixed)
            if state is AccountState.ACTIVE:
                if task.chat_id < 0 and task.pinned is None and fixed not in self._senders(task.chat_id):
                    return None, Reason.NO_ACCOUNT_IN_CHAT, None
                return Decision(fixed, False), None, None
            acc = pool.accounts.get(fixed)
            until = acc.limiter.paused_until() if acc is not None and state is AccountState.PAUSED else None
            return None, WAIT_REASON[state], until
        decision = choose(self._senders(task.chat_id), self.antiflood,
                          lambda a: self._queues[a].qsize() if a in self._queues else 0,
                          self._scheduler.account_for(SEND, task.chat_id))
        if decision is None:
            return None, Reason.NO_ACCOUNT_IN_CHAT, None
        return decision, None, None

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
        """OBX_Antifloodwaiter: wait until this account may write to this chat (D-OBX-2)."""
        cfg = self._store.current.config.outbox.antiflood.get(acc.kind)
        delay = self.antiflood.delay(cfg, acc.account_id, task.chat_id)
        if delay > 0:
            task.waited_ms += int(delay * 1000)
            await asyncio.sleep(delay)

    def _paced(self, acc: Account, task: _Task) -> None:
        cfg = self._store.current.config.outbox.antiflood.get(acc.kind)
        self.antiflood.wrote(cfg, acc.account_id, task.chat_id)

    async def _write(self, sql: str, *args: Any, attempt: tuple[Any, ...] | None = None) -> Any:
        async def fn(conn: asyncpg.Connection) -> Any:
            result = await conn.fetchval(sql, *args) if "RETURNING" in sql else await conn.execute(sql, *args)
            if attempt is not None:
                await conn.execute(_ATTEMPT, *attempt)
            return result

        # Transitions to 'sending', 'waiting' and final statuses are written synchronously (invariant 5).
        return await self._writer.submit_sync(fn)

    async def _finish(self, task: _Task, stage: str | None, status: Status, reason: str | None,
                      msg_id: int | None = None, attempt: tuple[Any, ...] | None = None,
                      follow: OutboundDraft | None = None) -> None:
        waited, task.waited_ms = task.waited_ms, 0

        async def fn(conn: asyncpg.Connection) -> int | None:
            await conn.execute(_FINISH, task.id, stage, str(status), reason, msg_id, waited)
            if attempt is not None:
                await conn.execute(_ATTEMPT, *attempt)
            if follow is None:
                return None
            # "Send, then pin": the pin is a task of its own, bound to the same account (D-OBX-5).
            row = await conn.fetchrow("SELECT trace_id, module, dedup_key, config_version, "
                                      "extract(epoch FROM expires_at - now())::float8 AS left_s "
                                      "FROM outbound_tasks WHERE id = $1", task.id)
            follow.dedup_key = f"{row['dedup_key']}:pin"
            return await insert_outbound(conn, follow, parent_trace_id=str(row["trace_id"]), module=row["module"],
                                         ttl_s=max(float(row["left_s"] or 0), 60.0),
                                         config_version=row["config_version"] or "")

        follow_id = await self._writer.submit_sync(fn)
        self._forget(task)
        if follow_id:
            self.enqueue([follow_id])
        if status in (Status.NOT_DELIVERED, Status.EXPIRED):
            self._report(task, reason)

    def _report(self, task: _Task, reason: str | None) -> None:
        """A final failure: the monitor log always, the group logs at most once per window (D-OBX-4)."""
        log.warning("outbox: task %s to %s finally failed: %s", task.id, task.chat_id, reason)
        logs = self._store.current.config.service_chats.logs
        if self.on_failure is None or task.chat_id == logs:
            return
        self._failure_count += 1
        now = time.monotonic()
        window = self._store.current.config.outbox.failure_notice_window_s
        if self._failure_last and now - self._failure_last < window:
            return
        self._failure_last, count, self._failure_count = now, self._failure_count, 0
        more = f" (неудач с прошлого сообщения: {count})" if count > 1 else ""
        self.on_failure(f"Outbox: задача {task.id} в чат {task.chat_id} не доставлена: {reason}{more}")

    async def _send(self, acc: Account, task: _Task) -> None:
        cfg = self._store.current.config.outbox
        row = await self._db.pool.fetchrow(
            "SELECT text, random_id, reply_to, finished_at, pin_msg_id, pin_after_send, "
            "(expires_at IS NOT NULL AND expires_at < now()) AS expired FROM outbound_tasks WHERE id = $1", task.id,
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
        if task.action == "pin":
            await self._pin(acc, task, int(row["pin_msg_id"]), attempts, cfg)
            return
        self._own.register(chat_id, row["text"])
        try:
            # OBX_Finalized: the message number must come back within confirm_timeout_s.
            msg_id: int | None = await acc.limiter.call(
                _BUDGET.get(task.kind, NOTIFY), "send",
                lambda: asyncio.wait_for(client.send_message(chat_id, row["text"], random_id=row["random_id"],
                                                             reply_to=row["reply_to"]), cfg.confirm_timeout_s),
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
        follow = None
        if row["pin_after_send"] and msg_id is not None:
            follow = OutboundDraft(chat_id=chat_id, text="", rule="pin", kind=task.kind, via_account=account,
                                   action="pin", pin_msg_id=msg_id)
        await self._finish(task, "delivered", Status.DELIVERED, None, msg_id,
                           attempt=(task.id, account, result, None), follow=follow)
        self.delivered += 1

    async def _pin(self, acc: Account, task: _Task, msg_id: int, attempts: int, cfg: Any) -> None:
        """Action "pin": silently, for both sides of a private dialog (D-OBX-5)."""
        account, client, chat_id = acc.account_id, acc.client, task.chat_id
        try:
            await acc.limiter.call(_BUDGET.get(task.kind, NOTIFY), "pin", lambda: asyncio.wait_for(
                client.pin_message(chat_id, msg_id), cfg.confirm_timeout_s))
        except FloodWait as exc:
            await self._write(_ATTEMPT, task.id, account, "flood_wait", exc.args[0])
            self._release(task)
            return
        except TelegramError as exc:
            await self._on_error(acc, task, exc.code, attempts, cfg.max_attempts, cfg.retry_base_s)
            return
        except (OSError, TimeoutError) as exc:
            await self._retry(task, account, f"NETWORK:{type(exc).__name__}", attempts, cfg.max_attempts,
                              cfg.retry_base_s)
            return
        self._paced(acc, task)
        await self._finish(task, "delivered", Status.DELIVERED, None, msg_id,
                           attempt=(task.id, account, "pinned", None))
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
