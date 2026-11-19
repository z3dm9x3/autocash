"""Dynamic pool of 1-10 Telegram accounts (FT-1.1..1.5, 1.8).

Every account has its own connection, its own supervisor task and its own limiter: a failure,
a reconnect or a FloodWait of one account touches only that account (P-4). The pool keeps
the state of every account, the membership map and the update counters (pts) of chats; it
tells its listeners (scheduler, Output) about every change so duties move within seconds.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

import asyncpg

from autocash.core.config.loader import ConfigStore
from autocash.core.pool.limiter import READ, AccountLimiter
from autocash.core.pool.membership import Membership
from autocash.core.pool.state import BLOCKED, CHAT_GONE, COVERING, LOSING, RESTRICTING, WRITE_FORBIDDEN, AccountState
from autocash.core.storage.pg import Database
from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import EventHandler, FloodWait, RawEvent, TelegramClient, TelegramError

log = logging.getLogger(__name__)

MAX_ACCOUNTS = 10
Reconciler = Callable[[int], Awaitable[bool]]

_ACCOUNT = """
INSERT INTO accounts (account_id, name, kind, state, state_since, last_error)
VALUES ($1, $2, $3, $4, now(), $5)
ON CONFLICT (account_id) DO UPDATE SET name = EXCLUDED.name, kind = EXCLUDED.kind, state = EXCLUDED.state,
    state_since = now(), last_error = EXCLUDED.last_error
"""
_STATE_LOG = "INSERT INTO account_state_log (account_id, old_state, new_state, reason) VALUES ($1, $2, $3, $4)"
_PAUSE = """
INSERT INTO flood_pauses (account_id, method, paused_until) VALUES ($1, $2, to_timestamp($3::float8))
ON CONFLICT (account_id, method) DO UPDATE SET paused_until = EXCLUDED.paused_until, created_at = now()
"""
_PTS = """
INSERT INTO chat_poll_state (chat_id, pts) VALUES ($1, $2)
ON CONFLICT (chat_id) DO UPDATE SET pts = EXCLUDED.pts, updated_at = now() WHERE chat_poll_state.pts < EXCLUDED.pts
"""


@dataclass(eq=False)
class Account:
    client: TelegramClient
    name: str
    kind: str
    limiter: AccountLimiter
    account_id: int = 0
    state: AccountState = AccountState.DISCONNECTED
    reason: str | None = None
    connected: bool = False
    restricted: bool = False
    lost: bool = False
    removed: bool = False
    dialogs_loaded: bool = False
    state_since: float = field(default_factory=time.time)
    started: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None


class PoolFull(RuntimeError):
    pass


class AccountPool:
    def __init__(self, store: ConfigStore, db: Database, writer: BatchWriter) -> None:
        self._store = store
        self._db = db
        self._writer = writer
        self.accounts: dict[int, Account] = {}
        self._all: list[Account] = []  # including accounts that have not connected yet
        self.user_ids: set[int] = set()  # read by the Gateway to recognise the pool's own messages
        self.membership = Membership(writer, self._changed)
        self.pts: dict[int, int] = {}  # chat -> counter up to which everything is accepted
        self.gaps: set[int] = set()  # chats that must be caught up through getChannelDifference
        self.last_event: dict[int, float] = {}
        self.api_ready = asyncio.Event()  # stored FloodWait pauses are loaded: active calls may start
        self.intake = True
        self.reconcilers: list[Reconciler] = []
        self.changed_at = time.monotonic()
        self._seen_pts: dict[int, int] = {}
        self._dirty_pts: set[int] = set()
        self._pts_snapshot: dict[int, int] = {}
        self._stored_pauses: dict[int, list[tuple[str, float]]] = {}
        self._handler: EventHandler | None = None
        self._alarm: Callable[[str, int | None], None] = lambda text, exclude: None
        self._notify: Callable[[str], None] = lambda text: None
        self._listeners: list[Callable[[], None]] = []
        self._bg: set[asyncio.Task[Any]] = set()
        self._timers: set[asyncio.TimerHandle] = set()
        self._zero_coverage: set[int] = set()
        self._coverage_check: asyncio.TimerHandle | None = None
        # Set by the App from the Outbox (D-OBX-7): the read budget of an account, the fuse, the router of reads.
        self.read_gate: Callable[[int, str, str], Awaitable[None]] | None = None
        self.on_flood: Callable[[int, str, float], None] | None = None
        self.pick_reader: Callable[[list[int], int | None], int | None] | None = None
        self._flusher: asyncio.Task[None] | None = None

    # ---- wiring --------------------------------------------------------------------------
    def bind(self, handler: EventHandler, alarm: Callable[[str, int | None], None],
             notify: Callable[[str], None]) -> None:
        self._handler, self._alarm, self._notify = handler, alarm, notify

    def subscribe(self, listener: Callable[[], None]) -> None:
        self._listeners.append(listener)

    def _spawn(self, coro: Any, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    def _later(self, delay: float, fn: Callable[[], None]) -> None:
        handle: asyncio.TimerHandle | None = None

        def fire() -> None:
            if handle is not None:
                self._timers.discard(handle)
            fn()

        handle = asyncio.get_running_loop().call_later(max(delay, 0.0), fire)
        self._timers.add(handle)

    def _changed(self) -> None:
        self.changed_at = time.monotonic()
        for listener in self._listeners:
            try:
                listener()
            except Exception:
                log.exception("pool listener failed")
        if self._coverage_check is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            self._coverage_check = loop.call_later(0.05, self._check_coverage)

    # ---- queries -------------------------------------------------------------------------
    def state(self, account_id: int) -> AccountState:
        acc = self.accounts.get(account_id)
        return acc.state if acc is not None else AccountState.REMOVED

    def active(self) -> list[Account]:
        return [a for a in self.accounts.values() if a.state is AccountState.ACTIVE]

    def live(self) -> list[Account]:
        return [a for a in self.accounts.values() if a.state not in (AccountState.REMOVED, AccountState.LOST)]

    def coverage(self, chat_id: int) -> int:
        return sum(1 for a in self.membership.members(chat_id) if self.state(a) in COVERING)

    def required_coverage(self, chat_id: int) -> int:
        rt = self._store.current
        cfg = rt.config.coverage
        roles = rt.source(chat_id).roles
        return max((cfg.required_by_role.get(role, cfg.default) for role in roles), default=cfg.default)

    def under_covered(self) -> list[tuple[int, int, int]]:
        """(chat, coverage, required) for configured sources below the required coverage (FT-1.5)."""
        out = []
        for chat_id, src in self._store.current.sources.items():
            if src.policy == "ignore":
                continue
            have, need = self.coverage(chat_id), self.required_coverage(chat_id)
            if have < need:
                out.append((chat_id, have, need))
        return out

    def _check_coverage(self) -> None:
        self._coverage_check = None
        covering = [a for a in self.accounts.values() if a.state in COVERING]
        if not self.api_ready.is_set() or not covering or not all(a.dialogs_loaded for a in covering):
            return  # the map is not complete yet: no false alarms at start
        zero = {chat for chat, have, _ in self.under_covered() if have == 0}
        for chat_id in sorted(zero - self._zero_coverage):
            name = self._store.current.source(chat_id).cfg.name or str(chat_id)
            self._alarm(f"Покрытие 0: в чате «{name}» ({chat_id}) нет ни одного аккаунта пула", None)
        self._zero_coverage = zero

    # ---- adding and removing accounts ------------------------------------------------------
    async def add(self, client: TelegramClient, name: str = "") -> Account:
        """Connect an account and put it under its own supervisor. Returns after the first attempt."""
        if len(self.live()) >= MAX_ACCOUNTS:
            raise PoolFull(f"the pool is limited to {MAX_ACCOUNTS} accounts")
        cfg = self._store.current.config.pool
        kind = "bot" if client.kind == "bot" else "user"
        acc = Account(client=client, name=name, kind=kind, limiter=AccountLimiter(
            concurrency=cfg.api_concurrency, rate_per_s=cfg.api_rate_per_s.get(kind, 1.0),
            backfill_share=cfg.backfill_share, flood_slowdown=cfg.flood_slowdown,
            flood_recovery_s=cfg.flood_recovery_s,
        ))
        acc.limiter.on_pause = partial(self._on_pause, acc)
        acc.limiter.gate = partial(self._gate, acc)
        acc.limiter.on_flood = partial(self._flood, acc)
        if hasattr(client, "extra_call"):
            client.extra_call = acc.limiter.gate  # a call the adapter makes by itself counts as a read
        self._all.append(acc)
        acc.task = asyncio.create_task(self._supervise(acc), name=f"account-{name or id(client)}")
        await acc.started.wait()
        return acc

    async def remove(self, account_id: int, reason: str = "выведен администратором") -> None:
        acc = self.accounts.get(account_id)
        if acc is None or acc.removed:
            return
        acc.removed = True
        if acc.task is not None:
            acc.task.cancel()
            await asyncio.gather(acc.task, return_exceptions=True)
        try:
            await acc.client.stop()
        except Exception:
            log.exception("account %s: stop failed", account_id)
        acc.connected = False
        self.user_ids.discard(account_id)
        self.refresh(acc, reason)
        self.membership.drop_account(account_id)

    def _register(self, acc: Account) -> None:
        acc.account_id = int(acc.client.account_id)
        acc.name = acc.name or str(acc.account_id)
        old = self.accounts.get(acc.account_id)
        if old is not None and old is not acc:
            old.removed = True  # the same account added again replaces its previous entry
        self.accounts[acc.account_id] = acc
        self.user_ids.add(acc.account_id)
        for method, until in self._stored_pauses.pop(acc.account_id, []):
            acc.limiter.restore(method, until)
            self._later(until - time.time() + 0.02, partial(self.refresh, acc, "пауза FloodWait истекла"))

    async def _supervise(self, acc: Account) -> None:
        cfg = self._store.current.config.pool
        delay = cfg.reconnect_s
        while not acc.removed and not acc.lost:
            try:
                await acc.client.start(partial(self._on_event, acc))
            except asyncio.CancelledError:
                raise
            except TelegramError as exc:
                if exc.code in LOSING and acc.client.account_id:
                    self._register(acc)
                    self._lose(acc, exc.code)
                    acc.started.set()
                    return
                log.error("account %s: start failed: %s", acc.name, exc.code)
            except Exception as exc:
                log.error("account %s: start failed: %r", acc.name, exc)
            else:
                self._register(acc)
                acc.connected = True
                delay = cfg.reconnect_s
                self.refresh(acc, "подключён")
                self._spawn(self._load_dialogs(acc), f"dialogs-{acc.account_id}")
                acc.started.set()
                try:
                    await acc.client.wait_disconnected()
                except asyncio.CancelledError:
                    raise
                except TelegramError as exc:
                    if exc.code in LOSING:
                        self._lose(acc, exc.code)
                        return
                except Exception as exc:
                    log.error("account %s: connection failed: %r", acc.name, exc)
                acc.connected = False
                if acc.removed or acc.lost:
                    return
                self.refresh(acc, "соединение потеряно")
            acc.started.set()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)

    def _lose(self, acc: Account, code: str) -> None:
        acc.lost = True
        acc.connected = False
        self.refresh(acc, code)
        self._spawn(self._stop_client(acc), f"stop-{acc.account_id}")

    async def _stop_client(self, acc: Account) -> None:
        try:
            await acc.client.stop()
        except Exception:
            log.exception("account %s: stop failed", acc.account_id)

    # ---- state ---------------------------------------------------------------------------
    def _effective(self, acc: Account) -> AccountState:
        if acc.removed:
            return AccountState.REMOVED
        if acc.lost:
            return AccountState.LOST
        if not acc.connected:
            return AccountState.DISCONNECTED
        if acc.restricted:
            return AccountState.RESTRICTED
        if acc.limiter.paused_until() is not None:
            return AccountState.PAUSED
        return AccountState.ACTIVE

    def refresh(self, acc: Account, reason: str | None = None) -> None:
        """Recompute the state; a change is logged, stored and announced (FT-1.3)."""
        new = self._effective(acc)
        if new is acc.state or not acc.account_id:
            return
        old, acc.state, acc.reason, acc.state_since = acc.state, new, reason, time.time()
        log.warning("account %s (%s): %s -> %s (%s)", acc.account_id, acc.name, old, new, reason)
        args = (acc.account_id, acc.name, acc.kind, str(new), reason)
        log_args = (acc.account_id, str(old), str(new), reason)

        async def write(conn: asyncpg.Connection) -> None:
            await conn.execute(_ACCOUNT, *args)
            await conn.execute(_STATE_LOG, *log_args)

        self._writer.submit(write)
        if new is AccountState.RESTRICTED:
            self._alarm(f"Аккаунт {acc.name} ({acc.account_id}) ограничен на отправку ({reason}); приём продолжается",
                        acc.account_id)
        elif new is AccountState.LOST:
            self._alarm(f"Аккаунт {acc.name} ({acc.account_id}) потерян: {reason}", acc.account_id)
        self._changed()

    def set_restricted(self, account_id: int, restricted: bool, reason: str) -> None:
        """Lifting a restriction is a manual decision (command of stage 1c)."""
        acc = self.accounts.get(account_id)
        if acc is not None:
            acc.restricted = restricted
            self.refresh(acc, reason)

    def _on_pause(self, acc: Account, method: str, until: float) -> None:
        if not acc.account_id:
            return
        account_id = acc.account_id
        self._writer.submit(lambda conn: conn.execute(_PAUSE, account_id, method, until))
        self.refresh(acc, f"FLOOD_WAIT {method} на {max(until - time.time(), 0):.0f} с")
        self._later(until - time.time() + 0.02, partial(self.refresh, acc, "пауза FloodWait истекла"))

    def apply_error(self, acc: Account, code: str, chat_id: int | None = None) -> bool:
        """A Telegram error updates the pool and the membership map (FT-10.5). True if it did."""
        if code in RESTRICTING:
            acc.restricted = True
            self.refresh(acc, code)
        elif code in LOSING:
            self._lose(acc, code)
        elif code in CHAT_GONE and chat_id is not None:
            self.membership.remove(chat_id, acc.account_id)
        elif code in WRITE_FORBIDDEN and chat_id is not None:
            self.membership.set(chat_id, acc.account_id, False)
        elif code in BLOCKED and chat_id is not None:
            account_id = acc.account_id
            self._writer.submit(lambda conn: conn.execute(
                "INSERT INTO dialog_marks (account_id, chat_id, mark) VALUES ($1, $2, 'USER_IS_BLOCKED') "
                "ON CONFLICT DO NOTHING", account_id, chat_id))
        else:
            return False
        return True

    # ---- stored state --------------------------------------------------------------------
    async def load_state(self) -> None:
        """Read what must survive a restart: FloodWait pauses, pts, membership. Then open active calls."""
        pool = self._db.pool
        await pool.execute("DELETE FROM flood_pauses WHERE paused_until <= now()")
        for row in await pool.fetch("SELECT account_id, method, extract(epoch FROM paused_until) AS until "
                                    "FROM flood_pauses"):
            acc = self.accounts.get(row["account_id"])
            until = float(row["until"])
            if acc is None:
                self._stored_pauses.setdefault(row["account_id"], []).append((row["method"], until))
                continue
            acc.limiter.restore(row["method"], until)
            self._later(until - time.time() + 0.02, partial(self.refresh, acc, "пауза FloodWait истекла"))
        for row in await pool.fetch("SELECT chat_id, pts FROM chat_poll_state"):
            self.pts.setdefault(row["chat_id"], row["pts"])
        self.membership.load(await pool.fetch("SELECT chat_id, account_id, can_send FROM chat_membership"))
        for acc in list(self.accounts.values()):
            self.refresh(acc, "пауза FloodWait сохранена до рестарта")
        self.api_ready.set()
        if self._flusher is None:
            self._flusher = asyncio.create_task(self._flush_loop(), name="pool-pts")
        self._changed()

    def _set_pts(self, chat_id: int, pts: int) -> None:
        if pts > self.pts.get(chat_id, -1):
            self.pts[chat_id] = pts
            self._dirty_pts.add(chat_id)

    def flush_pts(self, final: bool = False) -> None:
        """Store counters one tick late, so a stored pts never runs ahead of the SQLite buffer."""
        rows = list(self._pts_snapshot.items())
        self._pts_snapshot = {chat: self.pts[chat] for chat in self._dirty_pts}
        self._dirty_pts.clear()
        if final:
            rows, self._pts_snapshot = [*rows, *self._pts_snapshot.items()], {}
        if rows:
            self._writer.submit(lambda conn: conn.executemany(_PTS, rows))

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            self.flush_pts()

    # ---- dialogs -------------------------------------------------------------------------
    async def _load_dialogs(self, acc: Account) -> None:
        """One request for the dialog list, through the limiter (FT-1.4); also finds gaps (FT-2.6)."""
        await self.api_ready.wait()
        while acc.connected and not acc.removed:
            try:
                dialogs = await acc.limiter.call(READ, "getDialogs", acc.client.get_dialogs)
            except FloodWait as exc:
                await asyncio.sleep(exc.seconds + 0.05)
                continue
            except TelegramError as exc:
                if not self.apply_error(acc, exc.code):
                    log.error("account %s: dialog list failed: %s", acc.account_id, exc.code)
                break
            except Exception as exc:
                log.error("account %s: dialog list failed: %r", acc.account_id, exc)
                break
            # A bot gets no dialog list from Telegram: its map is built from updates and from
            # what was stored before (docs/decisions.md, D-1b-5).
            if dialogs or acc.kind != "bot":
                self.membership.replace(acc.account_id, dialogs)
            for dialog in dialogs:
                if dialog.pts is None:
                    continue
                known = self.pts.get(dialog.chat_id)
                if known is None:
                    self._set_pts(dialog.chat_id, dialog.pts)  # first sight: history is a backfill matter
                elif dialog.pts > known:
                    self.gaps.add(dialog.chat_id)
                self._seen_pts[dialog.chat_id] = max(self._seen_pts.get(dialog.chat_id, 0), dialog.pts)
            break
        acc.dialogs_loaded = True
        self._changed()

    # ---- incoming updates ----------------------------------------------------------------
    async def _on_event(self, acc: Account, ev: RawEvent) -> None:
        if not self.intake or self._handler is None:
            return
        chat_id = ev.chat_id
        if ev.kind == "joined":
            if chat_id is not None:
                self.membership.set(chat_id, acc.account_id, True, ev.chat_kind)
                if ev.pts is not None and chat_id not in self.pts:
                    self._set_pts(chat_id, ev.pts)
            return
        if ev.kind == "left":
            if chat_id is not None:
                self.membership.remove(chat_id, acc.account_id)
            return
        if chat_id is not None:
            self.membership.note(chat_id, acc.account_id, ev.chat_kind)
            self.last_event[chat_id] = time.monotonic()
            if ev.pts is not None:
                seen = self._seen_pts.get(chat_id)
                if seen is not None and ev.pts > seen + 1:
                    self.gaps.add(chat_id)  # updates were skipped: catch up through the difference
                    self._changed()
                if seen is None or ev.pts > seen:
                    self._seen_pts[chat_id] = ev.pts
        await self._handler(ev)
        if chat_id is not None and ev.pts is not None and chat_id not in self.gaps:
            self._set_pts(chat_id, ev.pts)

    async def deliver(self, acc: Account, events: list[RawEvent]) -> None:
        """Events obtained through getChannelDifference go the same way as push updates."""
        if self._handler is None:
            return
        for ev in events:
            if ev.kind in ("joined", "left") or ev.chat_id is None:
                continue
            self.last_event[ev.chat_id] = time.monotonic()
            await self._handler(ev)

    async def _gate(self, acc: Account, method: str) -> None:
        if self.read_gate is not None:
            await self.read_gate(acc.account_id, acc.kind, method)

    def _flood(self, acc: Account, method: str, seconds: float) -> None:
        if self.on_flood is not None:
            self.on_flood(acc.account_id, method, seconds)

    def reader(self, candidates: list[int], duty: int | None) -> int | None:
        """The account that reads (OBX_Router for reads): the least loaded, the duty on a tie."""
        if not candidates:
            return None
        if self.pick_reader is not None:
            return self.pick_reader(candidates, duty)
        return duty if duty in candidates else min(candidates)

    async def reread(self, chat_id: int, msg_ids: list[int], prefer: int | None = None) -> bool:
        """Re-read messages through an active reader of the chat (FT-З.10): the events go the usual
        way, so an edit missed by push becomes a new version; an unchanged message is a duplicate."""
        readers = [a for a in self.membership.members(chat_id) if self.state(a) is AccountState.ACTIVE]
        chosen = self.reader(readers, prefer)
        if chosen is None:
            return False
        acc = self.accounts[chosen]
        ids = sorted(set(msg_ids))
        for i in range(0, len(ids), 100):
            try:
                events = await acc.limiter.call(READ, "getMessages", partial(acc.client.get_messages, chat_id,
                                                                             ids[i:i + 100]))
            except FloodWait:
                return False
            except TelegramError as exc:
                self.apply_error(acc, exc.code, chat_id)
                return False
            await self.deliver(acc, events)
        return True

    def caught_up(self, chat_id: int, pts: int) -> None:
        self._set_pts(chat_id, pts)
        self._seen_pts[chat_id] = max(self._seen_pts.get(chat_id, 0), pts)
        if chat_id in self.gaps:
            self.gaps.discard(chat_id)
            self._changed()

    async def too_long(self, chat_id: int, pts_to: int | None) -> None:
        """"difference too long": the source gets a gap mark and open entities are reconciled (FT-2.7)."""
        pts_from = self.pts.get(chat_id)
        target = pts_to if pts_to is not None else self._seen_pts.get(chat_id, pts_from or 0)
        name = self._store.current.source(chat_id).cfg.name or str(chat_id)
        self._notify(f"Разрыв в источнике «{name}» ({chat_id}): пропущено слишком много обновлений, нужна сверка")
        gap_id = await self._writer.submit_sync(lambda conn: conn.fetchval(
            "INSERT INTO source_gaps (chat_id, pts_from, pts_to) VALUES ($1, $2, $3) RETURNING id",
            chat_id, pts_from, target))
        self.caught_up(chat_id, target)
        reconciled = False
        for reconcile in self.reconcilers:
            try:
                reconciled = bool(await reconcile(chat_id)) or reconciled
            except Exception:
                log.exception("reconcile of chat %s failed", chat_id)
        if reconciled:
            self._writer.submit(lambda conn: conn.execute(
                "UPDATE source_gaps SET status = 'reconciled', reconciled_at = now() WHERE id = $1", gap_id))

    # ---- lifecycle -----------------------------------------------------------------------
    async def stop(self) -> None:
        self.intake = False
        for handle in self._timers:
            handle.cancel()
        if self._coverage_check is not None:
            self._coverage_check.cancel()
        tasks: list[asyncio.Task[Any]] = [a.task for a in self._all if a.task is not None]
        if self._flusher is not None:
            tasks.append(self._flusher)
        for task in [*tasks, *self._bg]:
            task.cancel()
        await asyncio.gather(*tasks, *self._bg, return_exceptions=True)
        for acc in self._all:
            try:
                await acc.client.stop()
            except Exception:
                log.exception("account %s: stop failed", acc.account_id)
        self.flush_pts(final=True)
