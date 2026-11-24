"""OBX_Express: the express lane of the Outbox (D-QK-1, D-QK-2).

For writes where the reaction time is the whole point (QuickKast: the operator's trigger is replaced
by a template). The regular path routes a task through the inbox, the account queue, the even pacing
of the antiflood and the tempo of the limiter: seconds. The express lane, for one account:
  1. writes the tasks to `outbound_tasks` in ONE transaction on its own connection, already in the
     stage `sending` with their random_id (invariants 2, 3: a crash repeats the same task, never a
     second one; a send repeated with the same random_id is answered "duplicate");
  2. starts every write AT ONCE and in parallel (delete + send), through `AccountLimiter.express`:
     no queue, no pacing, no tempo. Only a pause of Telegram (FloodWait) stops it;
  3. counts the writes in the antiflood window of the account, so the regular tasks of that account
     move later (QK first, everything else after it);
  4. writes the finals through the batch writer, off the critical path.
A failed write is not repeated: a template that comes late is worse than none (D-QK-3). A task left
`sending` by a crash is finished by the regular Outbox after the restart, or expires (short TTL).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import asyncpg

from autocash.core.outbox.OBX_tasks import OutboundDraft, insert_outbound
from autocash.core.pool.state import AccountState
from autocash.core.storage.pg import is_connection_error
from autocash.core.telegram.api import FloodWait, RandomIdDuplicate, TelegramError
from autocash.core.trace.codes import Reason, Status

if TYPE_CHECKING:
    from autocash.core.outbox.OBX_outbox import Outbox

log = logging.getLogger(__name__)

_SENDING = """
UPDATE outbound_tasks SET stage = 'sending', status = 'in_progress', account_id = $2, assigned_at = now(),
       sending_at = now(), attempts = 1, router_used = false
WHERE id = ANY($1::bigint[])
"""
_FINISH = """
UPDATE outbound_tasks SET stage = $2, status = $3, reason_code = $4, finished_at = now(), delivered_msg_id = $5,
       confirmed_at = CASE WHEN $3 = 'delivered' THEN now() ELSE confirmed_at END
WHERE id = $1 AND finished_at IS NULL
"""
_ATTEMPT = "INSERT INTO outbound_attempts (task_id, account_id, result, error_code) VALUES ($1, $2, $3, $4)"


class ExpressRefused(Exception):
    """The lane cannot write now (account not active, paused by Telegram, database down, a repeat)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(slots=True)
class ExpressResult:
    task_id: int
    action: str
    ok: bool
    msg_id: int | None  # send: the delivered message; delete: the deleted one
    code: str | None  # the error code when not ok
    done_at: float  # wall clock when Telegram answered


@dataclass(slots=True)
class ExpressRun:
    stored_at: float  # the tasks were committed (wall clock)
    results: list[ExpressResult]


class Express:
    def __init__(self, outbox: Outbox) -> None:
        self._ob = outbox
        self._conn: asyncpg.Connection | None = None
        self._conn_lock = asyncio.Lock()
        self.runs = 0

    async def _connection(self) -> asyncpg.Connection:
        """A connection of its own, held: the lane never waits for the shared pool."""
        if self._conn is None or self._conn.is_closed():
            async with self._conn_lock:
                if self._conn is None or self._conn.is_closed():
                    self._conn = await self._ob._db.pool.acquire()
        return self._conn

    async def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                await self._ob._db.pool.release(conn)
            except Exception:  # the pool may be closing already
                log.debug("express: connection not released", exc_info=True)

    async def run(self, account_id: int, drafts: list[OutboundDraft], module: str, *, ttl_s: float = 30.0,
                  parent_trace_id: str | None = None, clear_draft: bool = False) -> ExpressRun:
        """`clear_draft`: a send also empties the cloud draft of its chat (QuickCast2, D-QK2-2)."""
        ob = self._ob
        acc = ob._pool.accounts.get(account_id)
        state = ob._pool.state(account_id)
        if acc is None or state is not AccountState.ACTIVE:
            # A paused account is a FloodWait of Telegram: nothing can be written before it ends.
            raise ExpressRefused(str(Reason.FLOOD_WAIT) if state is AccountState.PAUSED else f"ACCOUNT_{state}")
        for draft in drafts:
            if acc.limiter.paused_until(draft.action) is not None:
                raise ExpressRefused(str(Reason.FLOOD_WAIT))
        rt = ob._store.current
        try:
            ids = await self._store(account_id, drafts, module, ttl_s, parent_trace_id, rt.version)
        except _Duplicate as exc:
            raise ExpressRefused("DUPLICATE") from exc
        except Exception as exc:
            if is_connection_error(exc) or isinstance(exc, asyncpg.InterfaceError):
                await self.close()
                raise ExpressRefused(str(Reason.DB_UNAVAILABLE)) from exc
            raise
        stored_at = time.time()
        self.runs += 1
        timeout = rt.config.outbox.confirm_timeout_s
        calls: list[Awaitable[ExpressResult]] = []
        for task_id, draft in zip(ids, drafts, strict=True):
            if draft.action == "send":
                ob._own.register(draft.chat_id, draft.text)  # the echo of our send is origin=system
            calls.append(self._call(acc, task_id, draft, timeout, clear_draft))
        results = list(await asyncio.gather(*calls))
        self._finish(account_id, drafts, results)
        return ExpressRun(stored_at, results)

    async def _store(self, account_id: int, drafts: list[OutboundDraft], module: str, ttl_s: float,
                     parent_trace_id: str | None, version: str) -> list[int]:
        conn = await self._connection()
        async with conn.transaction():
            ids: list[int] = []
            for draft in drafts:
                task_id = await insert_outbound(conn, draft, parent_trace_id=parent_trace_id, module=module,
                                                ttl_s=ttl_s, config_version=version)
                if task_id is None:
                    raise _Duplicate()
                ids.append(task_id)
            await conn.execute(_SENDING, ids, account_id)
        return ids

    async def _call(self, acc: Any, task_id: int, draft: OutboundDraft, timeout: float,
                    clear_draft: bool = False) -> ExpressResult:
        client, chat_id = acc.client, draft.chat_id
        fn: Callable[[], Awaitable[Any]]
        if draft.action == "send":
            extra = {"clear_draft": True} if clear_draft else {}
            fn = lambda: asyncio.wait_for(client.send_message(  # noqa: E731
                chat_id, draft.text, random_id=draft.random_id, reply_to=draft.reply_to, **extra), timeout)
        elif draft.action == "delete":
            assert draft.target_msg_id is not None
            fn = lambda: asyncio.wait_for(client.delete_messages(chat_id, [draft.target_msg_id]), timeout)  # noqa: E731
        else:
            assert draft.pin_msg_id is not None
            fn = lambda: asyncio.wait_for(client.pin_message(chat_id, draft.pin_msg_id), timeout)  # noqa: E731
        try:
            value = await acc.limiter.express(draft.action, fn)
        except RandomIdDuplicate:
            return ExpressResult(task_id, draft.action, True, None, None, time.time())
        except FloodWait:
            return ExpressResult(task_id, draft.action, False, None, str(Reason.FLOOD_WAIT), time.time())
        except TelegramError as exc:
            return ExpressResult(task_id, draft.action, False, None, exc.code, time.time())
        except (OSError, TimeoutError) as exc:
            return ExpressResult(task_id, draft.action, False, None, f"NETWORK:{type(exc).__name__}", time.time())
        done = time.time()
        if draft.action == "send":
            return ExpressResult(task_id, "send", True, int(value), None, done)
        if draft.action == "delete":
            ok = bool(value)
            return ExpressResult(task_id, "delete", ok, draft.target_msg_id, None if ok else "NOT_DELETED", done)
        return ExpressResult(task_id, draft.action, True, draft.pin_msg_id, None, done)

    def _finish(self, account_id: int, drafts: list[OutboundDraft], results: list[ExpressResult]) -> None:
        ob = self._ob
        acc = ob._pool.accounts.get(account_id)
        cfg = ob._store.current.config.outbox.antiflood.get(acc.kind) if acc is not None else None
        for draft, res in zip(drafts, results, strict=True):
            if res.code != str(Reason.FLOOD_WAIT):
                # Every write that reached Telegram counts in the window: the regular tasks move later.
                ob.antiflood.wrote(cfg, account_id, draft.chat_id, factor=ob.fuse.factor(account_id))
            if res.action == "send" and res.msg_id is not None:
                ob._own.delivered(draft.chat_id, res.msg_id)
            if res.ok:
                ob.delivered += 1
            else:
                ob.failed += 1

        async def write(conn: asyncpg.Connection) -> None:
            for res in results:
                if res.ok:
                    result = {"send": "delivered", "delete": "deleted", "pin": "pinned"}[res.action]
                    await conn.execute(_FINISH, res.task_id, "delivered", str(Status.DELIVERED), None, res.msg_id)
                else:
                    result = "flood_wait" if res.code == str(Reason.FLOOD_WAIT) else (
                        "not_deleted" if res.code == "NOT_DELETED" else "error")
                    reason = str(Reason.DELETE_FAILED) if res.code == "NOT_DELETED" else res.code
                    await conn.execute(_FINISH, res.task_id, None, str(Status.NOT_DELIVERED), reason, None)
                await conn.execute(_ATTEMPT, res.task_id, account_id, result, None if res.ok else res.code)

        ob._writer.submit(write, urgent=True)



class _Duplicate(Exception):
    """The same express task exists (dedup key): it was handled already; the transaction rolls back."""
