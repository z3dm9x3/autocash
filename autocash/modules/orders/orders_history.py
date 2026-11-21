"""orders: the slow scan of the archive of Cash Monitor Dev, inside PostgreSQL (orders_readme.md, D-ORDERS-2..5).

The work is done by SQL: orders_history_step reads one range of raw_messages through dblink, parses the cards
and writes the orders; orders_history_settle gives roles to the users of the cards and writes their pairs
TGID - MelID. Python only paces the steps, joins the pairs into players (players_store.heal, a batch at a
time), and tells the in-memory directory about the roles.

The connection string of the archive is read from the environment (AUTOCASH_HISTORY_DSN) or from the
settings.ini of Cash Monitor Dev at start, kept in memory, never written or printed.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from autocash.modules.orders.orders_store import HISTORY_SOURCE
from autocash.modules.players import players_store

if TYPE_CHECKING:
    from autocash.modules.orders.orders_module import OrdersConfig, OrdersModule

log = logging.getLogger(__name__)

HEAL_BATCH = 500  # links joined into players per transaction: the lock of the graph is never held long


class HistoryRunner:
    def __init__(self, module: OrdersModule) -> None:
        self.module = module
        self.next_at = 0.0
        self.last_refresh = 0.0
        self.last_settle = 0.0
        self.conninfo: str | None = None
        self.ready: bool | None = None  # None: not checked yet; False: cannot run (told once)
        self.settled_once = False
        self.errors = 0

    def _source_dsn(self, cfg: OrdersConfig) -> str | None:
        env = os.environ.get(cfg.history.dsn_env)
        if env:
            return env
        if not cfg.history.source_ini:
            return None
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(Path(cfg.history.source_ini), encoding="utf-8-sig")
        if not parser.has_option(cfg.history.source_section, cfg.history.source_key):
            return None
        return parser.get(cfg.history.source_section, cfg.history.source_key).strip() or None

    async def _check(self, cfg: OrdersConfig) -> bool:
        svc = self.module.svc
        if self.ready is not None:
            return self.ready
        problem = None
        self.conninfo = self._source_dsn(cfg)
        if self.conninfo is None:
            problem = (f"нет строки подключения к архиву: переменная {cfg.history.dsn_env} или source_ini "
                       f"({cfg.history.source_section}.{cfg.history.source_key})")
        elif not await svc.db.pool.fetchval("SELECT 1 FROM pg_extension WHERE extname = 'dblink'"):
            problem = "в базе нет расширения dblink: .\\deploy\\windows\\autocash-pg.ps1 dblink (суперпользователь)"
        self.ready = problem is None
        if problem is not None:
            log.error("orders: the history is off: %s", problem)
            svc.notify(f"orders: история не читается — {problem}")
        return self.ready

    async def tick(self, cfg: OrdersConfig) -> None:
        """One step when it is due (the module's periodic hook calls this every second)."""
        h = cfg.history
        now = time.monotonic()
        if not h.enabled or now < self.next_at or not await self._check(cfg):
            return
        refresh = now - self.last_refresh >= h.refresh_interval_s
        try:
            result = await self._step(cfg, refresh)
        except Exception as exc:
            self.errors += 1
            self.next_at = time.monotonic() + h.error_pause_s
            await self._error(exc)
            return
        self.errors = 0
        if refresh:
            self.last_refresh = now
        if result.get("state") == "paused":
            self.next_at = now + h.idle_interval_s
            return
        caught_up = bool(result.get("caught_up"))
        # While the first pass runs a step follows a step; at the end of the archive the tail is read slower.
        self.next_at = now + (h.idle_interval_s if caught_up else h.interval_s)
        if await self._caught_up() and (not self.settled_once or now - self.last_settle >= h.settle_interval_s):
            self.last_settle = now
            await self.settle(cfg)

    async def _step(self, cfg: OrdersConfig, refresh: bool) -> dict[str, Any]:
        h = cfg.history
        pool = self.module.svc.db.pool
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = {int(h.statement_timeout_ms) * 3}")
            raw = await conn.fetchval(
                "SELECT orders_history_step($1, $2, $3, $4::bigint[], $5, $6, $7)", self.conninfo, HISTORY_SOURCE,
                h.batch, h.bots, refresh, h.refresh_days, h.statement_timeout_ms)
        result: dict[str, Any] = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
        if result.get("cards"):
            log.info("orders: history %s..%s of %s: %s cards", result.get("from"), result.get("to"), result.get("top"),
                     result.get("cards"))
        return result

    async def _caught_up(self) -> bool:
        return bool(await self.module.svc.db.pool.fetchval(
            "SELECT caught_up_at IS NOT NULL FROM orders_history WHERE source = $1", HISTORY_SOURCE))

    async def _error(self, exc: Exception) -> None:
        text = f"{type(exc).__name__}: {exc}"[:400]
        log.warning("orders: a history step failed: %s", text)
        svc = self.module.svc
        try:
            await svc.db.pool.execute(
                "INSERT INTO orders_history (source, last_error, last_error_at) VALUES ($1, $2, now()) "
                "ON CONFLICT (source) DO UPDATE SET last_error = EXCLUDED.last_error, last_error_at = now()",
                HISTORY_SOURCE, text)
        except Exception:
            log.debug("orders: the error of the history is not stored", exc_info=True)
        if self.errors == 3:  # once per run of failures: the step keeps being retried after a pause
            svc.notify(f"orders: история — шаг не выполняется: {text[:200]}")

    async def settle(self, cfg: OrdersConfig) -> dict[str, Any]:
        """Roles, pairs and names from the cards changed since the last run; then the pairs join players."""
        svc = self.module.svc
        pool = svc.db.pool
        exclude = sorted({*cfg.history.bots, *cfg.history.exclude_tg})
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '600s'")
            raw = await conn.fetchval("SELECT orders_history_settle($1, $2::text[], $3::bigint[])", HISTORY_SOURCE,
                                      [t.upper() for t in cfg.staff_user_types], exclude)
        res: dict[str, Any] = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
        roles = {int(k): str(v) for k, v in (res.get("roles") or {}).items()}
        if roles:
            svc.runtime().directory.set_users(roles)
        # A player found to be staff (he writes in a group of a kassa): his links stop joining players.
        for tg_id in res.get("demoted") or []:
            rows = await pool.fetch("SELECT mel_id FROM players_links WHERE tg_id = $1 AND state = 'active'", tg_id)
            for r in rows:
                await players_store.set_state(pool, int(tg_id), str(r["mel_id"]), "staff", author="orders:history")
        joined = 0
        while True:
            n = await players_store.heal(pool, HEAL_BATCH)
            joined += n
            if n < HEAL_BATCH:
                break
        res["joined"] = joined
        cashers = sorted(t for t, r in roles.items() if r == "casher")
        players = sum(1 for r in roles.values() if r == "player")
        if not self.settled_once:
            self.settled_once = True
            if roles or res.get("links"):
                svc.notify(f"orders: история прочитана — персонал (casher): {len(cashers)}, игроков (player): "
                           f"{players}, новых пар TGID—MelID: {res.get('links', 0)}, склеено в игроков: {joined}")
        elif cashers:
            more = f" … и ещё {len(cashers) - 20}" if len(cashers) > 20 else ""
            svc.notify(f"orders: по истории новый персонал (casher): {', '.join(map(str, cashers[:20]))}{more}")
        log.info("orders: settle: roles %d (casher %d, player %d), links %s, names %s, joined %d", len(roles),
                 len(cashers), players, res.get("links"), res.get("names"), joined)
        return res
