"""Debug and control commands (FT-11.9, 12.4, 12.6, 12.7, 13.3).

The commands are served by the core module `system`, subscribed to `command.*`: every command
is an inbound task with a trace (author = sender, source = chat, arguments = message fields,
result = the outbound reply linked by parent_trace_id). A user without rights gets a refusal
and the task ends "cancelled" with ACCESS_DENIED.

`/status` and `/accounts` also work while PostgreSQL is down: the Gateway hands such a command
to `emergency()`, which answers from memory through the emergency channel (NFT-18).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ClassVar

import asyncpg

from autocash.actions.send import OutboundDraft, insert_outbound
from autocash.core.commands import render
from autocash.core.commands.rights import can_run
from autocash.core.trace.codes import Reason, Status
from autocash.core.trace.monitor import STAGE_TITLES
from autocash.modules.api import Context, Event, Module

if TYPE_CHECKING:
    from autocash.core.app import App

log = logging.getLogger(__name__)

COMMANDS = ("trace", "stuck", "retry", "cancel", "queue", "accounts", "coverage", "status", "reload", "metrics")
EMERGENCY = ("status", "accounts")  # served without PostgreSQL (NFT-18)
_LINK = re.compile(r"(?:https?://)?t\.me/c/(\d+)/(\d+)")
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_FILTER_KEYS = {"reason", "module", "stage", "chat", "kind"}

# A resolver turns an argument of /trace (a key of a module entity) into trace ids; modules add theirs.
TraceResolver = Callable[[str], Awaitable[list[str]]]


class CommandError(ValueError):
    pass


class SystemCommands(Module):
    name: ClassVar[str] = "system"
    subscriptions: ClassVar[list[str]] = ["command.*"]
    commands: ClassVar[list[str]] = list(COMMANDS)
    concurrency: ClassVar[int] = 4
    timeout_s: ClassVar[float] = 10.0

    def __init__(self, app: App) -> None:
        self.app = app
        self.resolvers: list[TraceResolver] = []
        self._emergency_answered: dict[tuple[int, int], float] = {}
        self._bg: set[asyncio.Task[Any]] = set()

    # ---- entry points ------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        name = str(rec.fields.get("command") or (rec.msg_type or "").removeprefix("command."))
        if name not in COMMANDS or not event.external:
            return  # another module's command, or our own message (rules fire for external senders only)
        args = [str(a) for a in rec.fields.get("args") or []]
        rt = self.app.store.current
        if not can_run(rt, rec.sender_id, rec.chat_id, name):
            ctx.reply(f"Недостаточно прав для /{name}.", rule=f"command.{name}", kind="command_reply")
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        if self._emergency_answered.pop((rec.chat_id, rec.msg_id), None) is not None:
            return  # already answered through the emergency channel while PostgreSQL was down
        if name in rt.config.commands.heavy:
            # A heavy report: confirm at once, deliver the result when it is ready (FT-12.6).
            ctx.reply(rt.config.commands.ack_text, rule=f"command.{name}", kind="command_reply")
            self._spawn(self._deliver_later(event, name, args))
            return
        try:
            text = await self.run(name, args, author=rec.sender_id)
        except CommandError as exc:
            text = str(exc)
        ctx.reply(render.clip(text), rule=f"command.{name}", kind="command_reply")

    def emergency(self, rec: dict[str, Any]) -> None:
        """Gateway hook for every accepted command: answers /status and /accounts without PostgreSQL."""
        if self.app.db.available and self.app.ready.is_set():
            return
        command = next((f for f in rec.get("features") or [] if f.get("k") == "command"), None)
        if command is None or command["v"] not in EMERGENCY or rec.get("origin") != "external":
            return
        key = (rec["chat_id"], rec["msg_id"])
        if key in self._emergency_answered:
            return  # the same command received by several accounts of the pool
        if not can_run(self.app.store.current, rec.get("sender_id"), rec["chat_id"], command["v"]):
            return  # the refusal is recorded by the regular path once PostgreSQL is back
        self._emergency_answered[key] = time.time()
        self._spawn(self._emergency_reply(rec["chat_id"], command["v"]))

    async def _emergency_reply(self, chat_id: int, name: str) -> None:
        text = await self.status() if name == "status" else self.accounts()
        await self.app.output.direct_send(chat_id, render.clip(text + "\n(аварийный канал: PostgreSQL недоступен)"))

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _deliver_later(self, event: Event, name: str, args: list[str]) -> None:
        rec = event.record
        try:
            text = await self.run(name, args, author=rec.sender_id)
        except CommandError as exc:
            text = str(exc)
        except Exception as exc:
            log.exception("command /%s failed", name)
            text = f"Отчёт /{name} не построен: {exc!r}"[:500]
        rt = self.app.store.current
        draft = OutboundDraft(
            chat_id=rec.chat_id, text=render.clip(text), rule=f"command.{name}.result", kind="command_reply",
            reply_to=rec.msg_id, via_account=rec.account_id if rec.chat_id > 0 else None,
            dedup_key=f"{rec.trace_id}:system:{name}:result",
        )
        ttl = rt.config.output.ttl_s.get("command_reply", 300.0)

        async def write(conn: asyncpg.Connection) -> int | None:
            return await insert_outbound(conn, draft, parent_trace_id=rec.trace_id, module=self.name, ttl_s=ttl,
                                         config_version=rt.version)

        self.app.writer.submit(write, after=lambda task_id: self.app.output.enqueue([task_id]) if task_id else None)

    async def run(self, name: str, args: list[str], *, author: int | None = None) -> str:
        if name == "trace":
            return await self.trace(args)
        if name == "stuck":
            return await self.stuck(args[0] if args else None)
        if name == "retry":
            return await self.retry(args)
        if name == "cancel":
            return await self.cancel(args, author)
        if name == "queue":
            return render.queues(self.app.queues.stats(), self.app.output.stats())
        if name == "accounts":
            return self.accounts()
        if name == "coverage":
            return self.coverage()
        if name == "status":
            return await self.status()
        if name == "reload":
            ok, detail = self.app.reload_config()
            return f"Конфигурация перезагружена: версия {detail}" if ok else f"Конфигурация не применена: {detail}"
        if name == "metrics":
            return await self.metrics(args)
        raise CommandError(f"Неизвестная команда /{name}")

    # ---- /accounts, /coverage, /status: in-memory, no PostgreSQL --------------------------------
    def account_rows(self) -> list[dict[str, Any]]:
        pool = self.app.pool
        duties = self.app.scheduler.summary()
        rows = []
        for acc in sorted(pool.accounts.values(), key=lambda a: a.account_id):
            rows.append({
                "account_id": acc.account_id, "name": acc.name, "kind": acc.kind, "state": str(acc.state),
                "reason": acc.reason, "since": acc.state_since, "paused_until": acc.limiter.paused_until(),
                "budget": acc.limiter.budget_left(), "load": acc.limiter.load,
                "duties": duties.get(acc.account_id, {}),
            })
        return rows

    def accounts(self) -> str:
        return render.accounts(self.account_rows(), self.app.store.current.config.report_timezone)

    def coverage(self) -> str:
        rt = self.app.store.current
        under = [(chat, rt.source(chat).cfg.name or str(chat), have, need)
                 for chat, have, need in self.app.pool.under_covered()]
        approx = [(chat, p.get("title") or rt.source(chat).cfg.name or str(chat))
                  for chat, p in self.app.gateway.presence.items() if p.get("dedup_approx")]
        return render.coverage(under, approx)

    async def status(self) -> str:
        app = self.app
        rt = app.store.current
        buf = await app.buffer.stats()
        states: dict[str, int] = {}
        for acc in app.pool.accounts.values():
            states[str(acc.state)] = states.get(str(acc.state), 0) + 1
        under = app.pool.under_covered()
        queues = app.queues.stats()
        stuck = app.monitor.snapshot.counts()
        ready = (f"{render.dur(app.ready_at - app.started_at)}" if app.ready_at
                 else f"ещё не готова ({render.uptime(app.started_at)} с запуска)")
        lines = [
            f"Autocash: работает {render.uptime(app.started_at)}, полная готовность за {ready}",
            f"Конфигурация: {rt.version}",
            f"PostgreSQL: {'доступен' if app.db.available else 'НЕДОСТУПЕН'}",
            f"Буфер SQLite: невыгружено {buf['pending']}, уровень {buf['level']}"
            + (", ожидают: " + ", ".join(f"{r} {n}" for r, n in buf["waiting"].items()) if buf["waiting"] else "")
            + (f", отклонено по лимиту {buf['dropped']}" if buf["dropped"] else ""),
            f"Принято событий: {app.gateway.accepted}",
            "Пул: " + (", ".join(f"{s} {n}" for s, n in sorted(states.items())) or "пуст"),
            f"Недопокрытых чатов: {len(under)}" + (f" (из них с покрытием 0: {sum(1 for _, h, _ in under if h == 0)})"
                                                    if under else ""),
            "Очереди модулей: " + (", ".join(f"{n} {q['size']}" for n, q in queues.items()) or "нет"),
            "Вывод: " + ", ".join(f"{k} {v}" for k, v in app.output.stats().get("waiting", {}).items())
            if app.output.stats().get("waiting") else "Вывод: ожидающих нет",
            "Зависшие: " + (", ".join(f"{s} {n}" for s, n in sorted(stuck.items())) or "нет"),
            f"Без правила (unknown): {sum(app.sorter.unknown.values())}",
        ]
        if not app.pool.active():
            lines.append("⚠️ Нет ни одного активного аккаунта")
        return "\n".join(lines)

    # ---- /stuck, /metrics -----------------------------------------------------------------------
    async def stuck(self, stage: str | None) -> str:
        monitor = self.app.monitor
        snap = await monitor.check() if self.app.db.available else monitor.snapshot
        by_stage = snap.by_stage()
        if stage:
            if stage not in STAGE_TITLES:
                raise CommandError(f"Неизвестный этап {stage!r}. Этапы: {', '.join(STAGE_TITLES)}")
            by_stage = {k: v for k, v in by_stage.items() if k == stage}
        examples: dict[str, list[str]] = {}
        for task in sorted(snap.tasks, key=lambda t: -t.age):
            bucket = examples.setdefault(task.stage, [])
            if len(bucket) < 5:
                where = task.module or (f"аккаунт {task.account_id}" if task.account_id else "")
                bucket.append(f"{task.trace_id} — {render.dur(task.age)}" + (f", {where}" if where else ""))
        norms = dict(self.app.store.current.config.stage_norms_s)
        return render.stuck(by_stage, examples, STAGE_TITLES, norms)

    async def metrics(self, args: list[str]) -> str:
        hours = 24.0
        dim = "all"
        for arg in args:
            if arg.replace(".", "", 1).isdigit():
                hours = float(arg)
            elif arg in ("source", "module", "account", "all"):
                dim = arg
            else:
                raise CommandError("Формат: /metrics [часы] [all|source|module|account]")
        monitor = self.app.monitor
        await monitor.rollup()
        rows = await monitor.metrics(hours, dim)
        finals = await monitor.finals(hours, dim)
        lines = [f"Метрики этапов за {hours:g} ч, срез: {dim}"]
        for r in rows:
            label = "" if dim == "all" else f" [{r['dim_value']}]"
            lines.append(f"• {r['task_kind']}/{r['stage']}{label}: {r['count']} задач, "
                         f"медиана {render.dur(r['p50'])}, p95 {render.dur(r['p95'])}")
        if finals:
            lines.append("Финальные статусы:")
            for r in finals:
                label = "" if dim == "all" else f" [{r['dim_value']}]"
                lines.append(f"• {r['task_kind']}{label}: {render.status(r['status'], r['reason_code'] or None)} "
                             f"— {r['count']}")
        if len(lines) == 1:
            lines.append("Данных за период нет.")
        return "\n".join(lines)

    # ---- /trace ------------------------------------------------------------------------------
    async def resolve(self, arg: str) -> list[str]:
        if _UUID.match(arg):
            return [arg.lower()]
        db = self.app.db.reports
        link = _LINK.search(arg)
        if link:
            chat_id, msg_id = int("-100" + link.group(1)), int(link.group(2))
            rows = await db.fetch(
                "SELECT trace_id::text FROM trace_inbound WHERE chat_id = $1 AND msg_id = $2 ORDER BY accepted_at",
                chat_id, msg_id)
            return [r["trace_id"] for r in rows]
        found: list[str] = []
        for resolver in self.resolvers:
            found += await resolver(arg)
        return found

    async def trace(self, args: list[str]) -> str:
        if not args:
            raise CommandError("Формат: /trace <trace_id | ключ сущности модуля | ссылка на сообщение>")
        ids = await self.resolve(args[0])
        if not ids:
            return f"Задачи по «{args[0]}» не найдены."
        tz = self.app.store.current.config.report_timezone
        db = self.app.db.reports
        parts = []
        for trace_id in ids[:5]:
            t = await db.fetchrow("SELECT * FROM trace_inbound WHERE trace_id = $1::uuid", trace_id)
            if t is not None:
                modules = await db.fetch("SELECT * FROM trace_module WHERE trace_id = $1::uuid ORDER BY module",
                                         trace_id)
                outbound = await db.fetch(
                    "SELECT * FROM outbound_tasks WHERE parent_trace_id = $1::uuid ORDER BY id", trace_id)
                pairs = [(o, await self._attempts(o["id"])) for o in outbound]
                parts.append(render.inbound_trace(t, list(modules), pairs, tz))
                continue
            o = await db.fetchrow("SELECT * FROM outbound_tasks WHERE trace_id = $1::uuid", trace_id)
            if o is not None:
                text = render.outbound_trace(o, await self._attempts(o["id"]), tz)
                if o["parent_trace_id"] is not None:
                    text += f"\nпорождена входящей {o['parent_trace_id']}"
                parts.append(text)
                continue
            parts.append(f"Задача {trace_id} не найдена.")
        if len(ids) > 5:
            parts.append(f"… и ещё {len(ids) - 5}")
        return "\n\n".join(parts)

    async def _attempts(self, task_id: int) -> list[Any]:
        rows: list[Any] = await self.app.db.reports.fetch(
            "SELECT * FROM outbound_attempts WHERE task_id = $1 ORDER BY id", task_id)
        return rows

    # ---- /retry, /cancel ----------------------------------------------------------------------
    @staticmethod
    def parse_filter(args: list[str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for arg in args:
            key, sep, value = arg.partition("=")
            if not sep or key not in _FILTER_KEYS or not value:
                raise CommandError("Фильтр: key=value, ключи: " + ", ".join(sorted(_FILTER_KEYS)))
            out[key] = value
        return out

    async def _select(self, args: list[str]) -> tuple[list[tuple[str, str]], list[int], bool]:
        """Targets: (trace_id, module) of failed module processing and outbound task ids.

        With an explicit trace id finished failures are included; a filter selects only tasks
        that are still waiting (a mass restart of finished tasks needs explicit ids).
        """
        if not args:
            raise CommandError("Укажите trace_id или фильтр key=value")
        db = self.app.db.pool
        if len(args) == 1 and _UUID.match(args[0]):
            trace_id = args[0].lower()
            mods = await db.fetch("SELECT trace_id::text, module FROM trace_module "
                                  "WHERE trace_id = $1::uuid AND status = 'waiting'", trace_id)
            outs = await db.fetch("SELECT id FROM outbound_tasks WHERE trace_id = $1::uuid "
                                  "OR (parent_trace_id = $1::uuid AND status <> 'delivered')", trace_id)
            return [(r["trace_id"], r["module"]) for r in mods], [r["id"] for r in outs], True
        f = self.parse_filter(args)
        mods_q, outs_q = [], []
        if f.get("kind", "module") == "module" and f.get("stage", "module_done") == "module_done":
            rows = await db.fetch(
                "SELECT m.trace_id::text, m.module FROM trace_module m LEFT JOIN trace_inbound t USING (trace_id) "
                "WHERE m.status = 'waiting' AND ($1::text IS NULL OR m.reason_code = $1) "
                "AND ($2::text IS NULL OR m.module = $2) AND ($3::bigint IS NULL OR t.chat_id = $3) LIMIT 500",
                f.get("reason"), f.get("module"), _int(f.get("chat")))
            mods_q = [(r["trace_id"], r["module"]) for r in rows]
        if f.get("kind", "outbound") == "outbound":
            stage = f.get("stage")
            rows = await db.fetch(
                "SELECT id FROM outbound_tasks WHERE finished_at IS NULL AND status = 'waiting' "
                "AND ($1::text IS NULL OR reason_code = $1) AND ($2::text IS NULL OR module = $2) "
                "AND ($3::bigint IS NULL OR chat_id = $3) AND ($4::text IS NULL OR stage = $4) LIMIT 500",
                f.get("reason"), f.get("module"), _int(f.get("chat")),
                {"out_assigned": "queued", "out_sending": "assigned", "out_delivered": "sending"}.get(stage or "",
                                                                                                      stage))
            outs_q = [r["id"] for r in rows]
        return mods_q, outs_q, False

    async def retry(self, args: list[str]) -> str:
        mods, outs, explicit = await self._select(args)
        restarted_mods = await self.app.queues.retry(mods)
        ttl = self.app.store.current.config.output.ttl_s
        reopened: list[int] = []
        if outs:
            statuses = ["waiting", "in_progress"] + (["not_delivered", "expired", "cancelled"] if explicit else [])
            rows = await self.app.db.pool.fetch(
                "UPDATE outbound_tasks SET finished_at = NULL, status = 'in_progress', reason_code = NULL, "
                "wait_until = NULL, stuck_at = NULL, stuck_stage = NULL, "
                "stage = CASE WHEN finished_at IS NULL THEN stage ELSE 'queued' END, "
                "expires_at = CASE WHEN finished_at IS NULL THEN expires_at "
                "                  ELSE now() + make_interval(secs => ($3::jsonb ->> kind)::float8) END "
                "WHERE id = ANY($1::bigint[]) AND status = ANY($2::text[]) AND stage <> 'sending' RETURNING id",
                outs, statuses, _json(ttl))
            reopened = [r["id"] for r in rows]
            self.app.output.restart(reopened)
        if not restarted_mods and not reopened:
            return "Подходящих задач для перезапуска нет."
        return (f"Перезапущено: обработок модулем {len(restarted_mods)}, исходящих {len(reopened)}."
                + ("\n" + "\n".join(f"• {t} ({m})" for t, m in restarted_mods[:10]) if restarted_mods else ""))

    async def cancel(self, args: list[str], author: int | None) -> str:
        mods, outs, _ = await self._select(args)
        who = str(author) if author is not None else "unknown"
        db = self.app.db.pool
        cancelled_out: list[int] = []
        if outs:
            rows = await db.fetch(
                "UPDATE outbound_tasks SET status = 'cancelled', reason_code = $2, finished_at = now(), "
                "cancelled_by = $3, wait_until = NULL "
                "WHERE id = ANY($1::bigint[]) AND finished_at IS NULL AND stage <> 'sending' RETURNING id",
                outs, str(Reason.CANCELLED_BY_USER), who)
            cancelled_out = [r["id"] for r in rows]
            self.app.output.cancelled(cancelled_out)
        cancelled_mods = 0
        if mods:
            async with db.acquire() as conn, conn.transaction():
                done = await conn.fetch(
                    "UPDATE trace_module SET status = 'cancelled', reason_code = $3, cancelled_by = $4 "
                    "WHERE (trace_id, module) IN (SELECT * FROM unnest($1::uuid[], $2::text[])) "
                    "AND status = 'waiting' RETURNING trace_id",
                    [t for t, _ in mods], [m for _, m in mods], str(Reason.CANCELLED_BY_USER), who)
                cancelled_mods = len(done)
                await conn.execute(
                    "UPDATE trace_inbound SET status = 'cancelled', reason_code = $2, finished_at = now() "
                    "WHERE trace_id = ANY($1::uuid[]) AND status = 'waiting'",
                    [r["trace_id"] for r in done], str(Reason.CANCELLED_BY_USER))
        if not cancelled_out and not cancelled_mods:
            return "Подходящих задач для отмены нет (задачи на этапе отправки не отменяются)."
        return f"Отменено: исходящих {len(cancelled_out)}, обработок модулем {cancelled_mods}. Автор: {who}."


def _int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise CommandError(f"Не число: {value}") from exc


def _json(value: dict[str, float]) -> str:
    return json.dumps(value)
