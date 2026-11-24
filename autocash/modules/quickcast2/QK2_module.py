"""QK2: QuickCast2 - a trigger typed in the input field, never sent, becomes a template (QK2_readme.md).

An exception module, like QuickKast (D-QK-1), but on the cloud DRAFT of a private dialog (D-QK2-1):
the operator types `TRIGGER\\` in the input field of a dialog of a pool account and does not send it.
Their client saves the draft to the cloud, Telegram tells every session of the account
(UpdateDraftMessage), and the Gateway calls `draft()` with it, before anything else:
  1. memory only: a draft of a private dialog that ends with a backslash, the rest being a trigger of
     the active profile of the QuickKast catalog (the same templates, `-N` and `-list` included) ->
     a task of its own is started;
  2. the task hands ONE write to the express lane of the Outbox: send the template with `clear_draft`,
     so Telegram empties the draft by itself (D-QK2-2). Nothing was sent, so nothing is deleted;
  3. the times go to `qk2_events`; `/qk2 stats` shows them.
The catalog is the one of QuickKast (D-QK2-3): `/qk add`, `/qk set` and `x` / `v` act on both.

Management: `/qk2 ...` in a group with the role tech, Admin and Dev (regular command path).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.core.commands.rights import can_run, deny
from autocash.core.outbox.OBX_express import ExpressRefused, ExpressRun
from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.core.telegram.api import RawEvent
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, Event, Module, Services
from autocash.modules.quickkast.QK_catalog import Catalog, Resolved, shared

log = logging.getLogger(__name__)

PREFIX = "QK2"
NAME = "quickcast2"
MARK = "\\"  # the draft ends with it: `зд\`
SWITCH_ROLE = "tech"
COMMAND = re.compile(r"/qk2(?:@\w+)?\s*(\S*)\s*(.*)", re.S | re.I)
FAILURE_WINDOW_S = 600.0
BUSY_S = 10.0  # one template at a time per dialog: further draft updates of it are skipped meanwhile
HELP = """QuickCast2 — шаблон без отправки триггера:
в поле ввода личного диалога аккаунта набрать ТРИГГЕР\\ (обратная косая черта в конце) и НЕ отправлять.
Черновик уходит в облако, шаблон отправляется, поле ввода очищается.
ТРИГГЕР-N\\ — вариант N, ТРИГГЕР-list\\ — все варианты. Шаблоны и профили — общие с QuickKast (/qk).

/qk2 on | off — включить или выключить
/qk2 stats — скорость срабатываний"""


class QK2Config(BaseModel):
    ttl_s: float = 30.0  # a template not sent in this time is not sent at all (as D-QK-3)


class QuickCast2Module(Module):
    name: ClassVar[str] = NAME
    subscriptions: ClassVar[list[str]] = ["command.qk2"]
    commands: ClassVar[list[str]] = ["qk2"]
    config_model: ClassVar[type[BaseModel] | None] = QK2Config
    migrations: ClassVar[Path | None] = Path(__file__).parent / "QK2_migrations"
    draft_path: ClassVar[bool] = True

    def __init__(self) -> None:
        self.catalog = Catalog()  # replaced by the shared one at start
        self.enabled = True
        self._seen: OrderedDict[tuple[int, int, str, float], None] = OrderedDict()
        self._busy: dict[tuple[int, int], float] = {}  # (account, chat) -> monotonic start of a firing
        self._tasks: set[asyncio.Task[None]] = set()
        self._failure_at = 0.0
        self._failures = 0
        self.fired = 0

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    def cfg(self) -> QK2Config:
        mod = self.svc.runtime().config.modules.get(NAME)
        return QK2Config.model_validate(mod.config if mod is not None else {})

    async def start(self) -> None:
        pool = self.svc.db.pool
        self.catalog = shared(pool)
        if not self.catalog.loaded:
            await self.catalog.load(pool)  # QuickKast not loaded yet: the same rows; it loads them again
        self.enabled = bool(await pool.fetchval("SELECT enabled FROM qk2_settings WHERE id = 1"))
        log.info("QK2: QuickCast2 is %s, profile %s (the catalog of QuickKast)", "on" if self.enabled else "off",
                 self.catalog.active)

    # ---- the draft hook (Gateway; memory only) ---------------------------------------------------------
    def draft(self, event: RawEvent, received: float) -> None:
        cat = self.catalog
        if (not self.enabled or not cat.loaded or event.chat_kind != "private" or event.chat_id is None
                or event.chat_id <= 0 or event.chat_id == event.account_id or event.media_type is not None):
            return
        text = event.text.strip()
        if len(text) < 2 or not text.endswith(MARK):
            return  # an emptied draft (also ours, after the send) or not a trigger
        entered = text[:-1].strip()
        if not entered or not cat.chat_enabled(event.account_id, event.chat_id):
            return
        resolved = cat.resolve(entered)
        if resolved is None:
            return
        dialog = (event.account_id, event.chat_id)
        started = self._busy.get(dialog)
        if started is not None and time.monotonic() - started < BUSY_S:
            return  # the same draft synced again while its template is on the way
        stamp = event.date.timestamp() if event.date is not None else received
        key = (event.account_id, event.chat_id, text, stamp)
        if key in self._seen:
            return
        matched = time.time()
        self._seen[key] = None
        if len(self._seen) > 10_000:
            self._seen.popitem(last=False)
        self._busy[dialog] = time.monotonic()
        task = asyncio.create_task(self._fire(event, text, resolved, stamp, received, matched),
                                   name=f"qk2-{event.chat_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _fire(self, event: RawEvent, text: str, resolved: Resolved, stamp: float, received: float,
                    matched: float) -> None:
        assert event.chat_id is not None
        account, chat = event.account_id, event.chat_id
        draft = OutboundDraft(chat_id=chat, text=self._reply_text(resolved), rule="qk2.send", kind="client_reply",
                              via_account=account, priority=0, reply_to=event.reply_to,
                              dedup_key=f"{NAME}:{account}:{chat}:{stamp:.3f}:send")
        express = self.svc.express
        run: ExpressRun | None = None
        status, reason = "done", None
        try:
            if express is None:
                raise ExpressRefused("NO_EXPRESS_LANE")
            run = await express(account, [draft], NAME, self.cfg().ttl_s, clear_draft=True)
        except ExpressRefused as exc:
            status, reason = "refused", exc.reason
        except Exception as exc:  # the hook must never break the account's intake
            log.exception("QK2: draft in %s via %s failed", chat, account)
            status, reason = "failed", type(exc).__name__
        finally:
            self._busy.pop((account, chat), None)
        if run is not None:
            bad = [r for r in run.results if not r.ok]
            if bad:
                status, reason = "failed", ", ".join(f"{r.action}:{r.code}" for r in bad)
        self.fired += 1
        if status != "done":
            self._failure(account, chat, text, reason)
        await self._safe(self._record(event, text, resolved, received, matched, run, status, reason))

    @staticmethod
    def _reply_text(resolved: Resolved) -> str:
        if resolved.kind == "list":
            lines = [f"QuickKast {resolved.trigger}: {len(resolved.rows)} вариант(а):"]
            lines += [f"{resolved.trigger}-{variant}\n{template}" for variant, template in resolved.rows]
            return "\n\n".join(lines)
        if resolved.kind == "missing_variant":
            return f"Для триггера {resolved.trigger} доступно только {resolved.count} вариант(а)."
        return resolved.template or ""

    async def _record(self, event: RawEvent, text: str, resolved: Resolved, received: float, matched: float,
                      run: ExpressRun | None, status: str, reason: str | None) -> None:
        def ts(value: float | None) -> datetime | None:
            return datetime.fromtimestamp(value, UTC) if value is not None else None

        send = run.results[0] if run is not None and run.results else None
        await self.svc.db.pool.execute(
            "INSERT INTO qk2_events (account_id, chat_id, entered, kind, profile, trigger, variant, draft_date, "
            "received_at, matched_at, stored_at, sent_at, send_task_id, sent_msg_id, status, reason) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            event.account_id, event.chat_id, text, resolved.kind, self.catalog.active, resolved.trigger,
            resolved.variant, event.date, ts(received), ts(matched), ts(run.stored_at if run else None),
            ts(send.done_at if send and send.ok else None), send.task_id if send else None,
            send.msg_id if send else None, status, reason)

    def _failure(self, account: int, chat: int, text: str, reason: str | None) -> None:
        """To the monitor log always; to `logs` at most once in FAILURE_WINDOW_S, with the count."""
        log.warning("QK2: draft %r in %s via %s: %s", text, chat, account, reason)
        self._failures += 1
        now = time.monotonic()
        if self._failure_at and now - self._failure_at < FAILURE_WINDOW_S:
            return
        self._failure_at, count, self._failures = now, self._failures, 0
        more = f"; неудач с прошлого сообщения: {count}" if count > 1 else ""
        self.svc.notify(f"{PREFIX}: черновик «{text}» в диалоге {chat} (аккаунт {account}) не отработал: "
                        f"{reason}{more}")

    @staticmethod
    async def _safe(coro: Any) -> None:
        try:
            await coro
        except Exception:  # bookkeeping must not hide the result of the trigger
            log.warning("QK2: bookkeeping failed", exc_info=True)

    # ---- /qk2 ------------------------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if event.msg_type != "command.qk2" or rec.origin == "system":
            return
        rt = self.svc.runtime()
        if not can_run(rt, rec.sender_id, rec.chat_id, "qk2"):
            deny(self.svc.notify, rt, rec.sender_id, rec.chat_id, "qk2")  # the chat gets nothing (D-RM-8)
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        if rt.group_role(rec.chat_id) != SWITCH_ROLE:
            ctx.reply(f"Команда /qk2 работает только в группе с ролью {SWITCH_ROLE}.", rule="command.qk2",
                      kind="command_reply")
            return
        ctx.reply(await self._command(rec.text or "", rec.sender_id), rule="command.qk2", kind="command_reply")

    async def _command(self, text: str, user: int | None) -> str:
        match = COMMAND.fullmatch(text.strip())
        op = match.group(1).lower() if match else ""
        if op in ("on", "off"):
            self.enabled = op == "on"
            await self.svc.db.pool.execute(
                "UPDATE qk2_settings SET enabled = $1, updated_by = $2, updated_at = now() WHERE id = 1",
                self.enabled, user)
            self.svc.notify(f"{PREFIX}: QuickCast2 {'включён' if self.enabled else 'выключен'}; автор {user}")
            return f"QuickCast2: {'ON' if self.enabled else 'OFF'}. Профиль: {self.catalog.active}."
        if op == "stats":
            return await self._stats()
        return HELP

    async def _stats(self) -> str:
        pool = self.svc.db.pool
        row = await pool.fetchrow(
            "WITH e AS (SELECT extract(epoch FROM sent_at - received_at) * 1000 AS total, "
            "extract(epoch FROM stored_at - received_at) * 1000 AS db FROM qk2_events "
            "WHERE status = 'done' AND received_at > now() - interval '24 hours') "
            "SELECT count(*) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY total) AS p50, "
            "percentile_cont(0.9) WITHIN GROUP (ORDER BY total) AS p90, max(total) AS mx, "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY db) AS db50 FROM e")
        bad = await pool.fetchval(
            "SELECT count(*) FROM qk2_events WHERE status <> 'done' AND received_at > now() - interval '24 hours'")
        if not row or not row["n"]:
            return f"QuickCast2 за 24 часа: срабатываний нет. Неудач: {bad}."

        def ms(value: Any) -> str:
            return f"{float(value):.0f} мс" if value is not None else "—"

        return (f"QuickCast2 за 24 часа: {row['n']} шаблонов, неудач {bad}.\n"
                f"От прихода черновика до ответа Telegram на отправку: p50 {ms(row['p50'])}, "
                f"p90 {ms(row['p90'])}, max {ms(row['mx'])}; запись в БД p50 {ms(row['db50'])}.\n"
                "Сюда не входит задержка клиента: он сохраняет черновик в облако после паузы в наборе.")
