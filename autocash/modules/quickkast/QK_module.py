"""QK: QuickKast - an operator's trigger in a private dialog becomes a template (QK_readme.md, D-QK-1..5).

An exception module (D-QK-1): the trigger does not take the regular path (buffer -> PostgreSQL ->
sorter -> queue -> module -> Outbox queue -> pacing), which costs seconds. The Gateway calls
`express()` with the update itself, before the buffer:
  1. memory only: an outgoing message of a pool account, typed by a person (origin human), in a
     private dialog, equal to a trigger of the active profile -> a task of its own is started;
  2. the task hands two writes to the express lane of the Outbox (OBX_express): delete the trigger
     and send the template, committed in one transaction, then sent to Telegram AT ONCE and in
     parallel, with no queue and no pacing; the regular tasks of the account move after them;
  3. the times of every stage go to `qk_events` (off the critical path); `/qk stats` shows them.
The message with the trigger still takes the regular path too (it is stored like any message);
no other module reacts to it (it is the account's own message).

Management: `/qk ...` in a group with the role tech, Admin and Dev (regular command path).
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
from autocash.modules.quickkast.QK_catalog import (
    RESERVED_PROFILE,
    RESERVED_TRIGGERS,
    STANDARD,
    VARIANT_SUFFIX,
    Catalog,
    Resolved,
    shared,
    store_chat,
)

log = logging.getLogger(__name__)

PREFIX = "QK"
NAME = "quickkast"
SWITCH_ROLE = "tech"  # /qk works only in a group with this role (as /as, D-QK-5)
COMMAND = re.compile(r"/qk(?:@\w+)?\s*(\S*)\s*(.*)", re.S | re.I)  # op, the rest (its newlines kept)
WORD = re.compile(r"(\S+)\s*(.*)", re.S)
FAILURE_WINDOW_S = 600.0  # failures go to logs at most once in this window
HELP = """QuickKast:
/qk on | off — включить или выключить
/qk add ТРИГГЕР ТЕКСТ — добавить вариант шаблона (текст может быть многострочным)
/qk add ТРИГГЕР — ответом на сообщение: его текст станет вариантом
/qk delete ТРИГГЕР — удалить триггер; /qk delete ТРИГГЕР-N — один вариант
/qk list [ПРОФИЛЬ] — шаблоны
/qk add profile ИМЯ — создать профиль
/qk set ИМЯ — выбрать профиль; /qk set — вернуться в standard
/qk profile list — профили
/qk stats — скорость срабатываний

В личном диалоге аккаунта: ТРИГГЕР — случайный вариант, ТРИГГЕР-N — вариант N,
ТРИГГЕР-list — все варианты. x или х — выключить QK в этом диалоге, v — включить."""


class QKConfig(BaseModel):
    ttl_s: float = 30.0  # a template not sent in this time is not sent at all (D-QK-3)


class QuickKastModule(Module):
    name: ClassVar[str] = NAME
    subscriptions: ClassVar[list[str]] = ["command.qk"]
    commands: ClassVar[list[str]] = ["qk"]
    config_model: ClassVar[type[BaseModel] | None] = QKConfig
    migrations: ClassVar[Path | None] = Path(__file__).parent / "QK_migrations"
    express_path: ClassVar[bool] = True

    def __init__(self) -> None:
        self.catalog = Catalog()
        self._seen: OrderedDict[tuple[int, int, int], None] = OrderedDict()  # (account, chat, msg) fired
        self._tasks: set[asyncio.Task[None]] = set()
        self._failure_at = 0.0
        self._failures = 0
        self.fired = 0

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    def cfg(self) -> QKConfig:
        mod = self.svc.runtime().config.modules.get(NAME)
        return QKConfig.model_validate(mod.config if mod is not None else {})

    async def start(self) -> None:
        self.catalog = shared(self.svc.db.pool)  # one catalog with QuickCast2 (D-QK2-3)
        await self.catalog.load(self.svc.db.pool)
        count = sum(len(v) for v in self.catalog.templates.values())
        log.info("QK: QuickKast is %s, profile %s, %d templates", "on" if self.catalog.enabled else "off",
                 self.catalog.active, count)

    # ---- the express hook (Gateway, before the buffer; memory only) ---------------------------------
    def express(self, event: RawEvent, origin: str, received: float) -> None:
        cat = self.catalog
        if (origin != "human" or not cat.loaded or event.chat_kind != "private" or event.chat_id is None
                or event.chat_id <= 0 or event.chat_id == event.account_id or event.sender_id != event.account_id
                or event.forwarded or event.media_type is not None):
            return
        text = event.text.strip()
        if not text:
            return
        key = (event.account_id, event.chat_id, event.msg_id)
        if key in self._seen:
            return  # the same update again
        lowered = text.lower()
        resolved: Resolved | None = None
        if lowered in RESERVED_TRIGGERS:
            toggle: bool | None = lowered == "v"
        else:
            if not cat.enabled or not cat.chat_enabled(event.account_id, event.chat_id):
                return
            resolved = cat.resolve(text)
            if resolved is None:
                return
            toggle = None
        matched = time.time()
        self._seen[key] = None
        if len(self._seen) > 10_000:
            self._seen.popitem(last=False)
        if toggle is not None:
            cat.set_chat(event.account_id, event.chat_id, toggle)  # memory first: the next message sees it
        task = asyncio.create_task(self._fire(event, text, resolved, toggle, received, matched),
                                   name=f"qk-{event.chat_id}-{event.msg_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _fire(self, event: RawEvent, text: str, resolved: Resolved | None, toggle: bool | None,
                    received: float, matched: float) -> None:
        assert event.chat_id is not None
        account, chat, msg_id = event.account_id, event.chat_id, event.msg_id
        drafts = [OutboundDraft(chat_id=chat, text="", rule="qk.delete", kind="client_reply", via_account=account,
                                action="delete", target_msg_id=msg_id, prefer_account=account, priority=0,
                                dedup_key=f"{NAME}:{account}:{chat}:{msg_id}:delete")]
        reply = self._reply_text(resolved)
        if reply is not None:
            drafts.append(OutboundDraft(chat_id=chat, text=reply, rule="qk.send", kind="client_reply",
                                        via_account=account, priority=0,
                                        dedup_key=f"{NAME}:{account}:{chat}:{msg_id}:send"))
        express = self.svc.express
        run: ExpressRun | None = None
        status, reason = "done", None
        try:
            if express is None:
                raise ExpressRefused("NO_EXPRESS_LANE")
            run = await express(account, drafts, NAME, self.cfg().ttl_s)
        except ExpressRefused as exc:
            status, reason = "refused", exc.reason
        except Exception as exc:  # the hook must never break the account's intake
            log.exception("QK: trigger %s in %s via %s failed", msg_id, chat, account)
            status, reason = "failed", type(exc).__name__
        if run is not None:
            bad = [r for r in run.results if not r.ok]
            if bad:
                status = "failed" if len(bad) == len(run.results) else "partial"
                reason = ", ".join(f"{r.action}:{r.code}" for r in bad)
        self.fired += 1
        if toggle is not None and status != "refused":
            await self._safe(store_chat(self.svc.db.pool, account, chat, toggle))
        if status != "done":
            self._failure(account, chat, text, status, reason)
        await self._safe(self._record(event, text, resolved, toggle, received, matched, run, status, reason))

    @staticmethod
    def _reply_text(resolved: Resolved | None) -> str | None:
        if resolved is None:
            return None  # x / х / v: only the trigger is deleted
        if resolved.kind == "list":
            lines = [f"QuickKast {resolved.trigger}: {len(resolved.rows)} вариант(а):"]
            lines += [f"{resolved.trigger}-{variant}\n{template}" for variant, template in resolved.rows]
            return "\n\n".join(lines)
        if resolved.kind == "missing_variant":
            return f"Для триггера {resolved.trigger} доступно только {resolved.count} вариант(а)."
        return resolved.template

    async def _record(self, event: RawEvent, text: str, resolved: Resolved | None, toggle: bool | None,
                      received: float, matched: float, run: ExpressRun | None, status: str,
                      reason: str | None) -> None:
        def ts(value: float | None) -> datetime | None:
            return datetime.fromtimestamp(value, UTC) if value is not None else None

        by = {r.action: r for r in run.results} if run is not None else {}
        delete, send = by.get("delete"), by.get("send")
        kind = resolved.kind if resolved is not None else ("chat_on" if toggle else "chat_off")
        await self.svc.db.pool.execute(
            "INSERT INTO qk_events (account_id, chat_id, trigger_msg_id, entered, kind, profile, trigger, variant, "
            "msg_date, received_at, matched_at, stored_at, deleted_at, sent_at, delete_task_id, send_task_id, "
            "sent_msg_id, status, reason) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, "
            "$16, $17, $18, $19)",
            event.account_id, event.chat_id, event.msg_id, text, kind,
            self.catalog.active if resolved is not None else None, resolved.trigger if resolved else None,
            resolved.variant if resolved else None, event.date, ts(received), ts(matched),
            ts(run.stored_at if run else None), ts(delete.done_at if delete and delete.ok else None),
            ts(send.done_at if send and send.ok else None), delete.task_id if delete else None,
            send.task_id if send else None, send.msg_id if send else None, status, reason)

    def _failure(self, account: int, chat: int, text: str, status: str, reason: str | None) -> None:
        """To the monitor log always; to `logs` at most once in FAILURE_WINDOW_S, with the count."""
        log.warning("QK: %s, trigger %r in %s via %s: %s", status, text, chat, account, reason)
        self._failures += 1
        now = time.monotonic()
        if self._failure_at and now - self._failure_at < FAILURE_WINDOW_S:
            return
        self._failure_at, count, self._failures = now, self._failures, 0
        more = f"; неудач с прошлого сообщения: {count}" if count > 1 else ""
        self.svc.notify(f"{PREFIX}: триггер «{text}» в диалоге {chat} (аккаунт {account}) не отработал: "
                        f"{reason}{more}")

    @staticmethod
    async def _safe(coro: Any) -> None:
        try:
            await coro
        except Exception:  # bookkeeping must not hide the result of the trigger
            log.warning("QK: bookkeeping failed", exc_info=True)

    # ---- /qk -------------------------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if event.msg_type != "command.qk" or rec.origin == "system":
            return
        rt = self.svc.runtime()
        if not can_run(rt, rec.sender_id, rec.chat_id, "qk"):
            deny(self.svc.notify, rt, rec.sender_id, rec.chat_id, "qk")  # the chat gets nothing (D-RM-8)
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        if rt.group_role(rec.chat_id) != SWITCH_ROLE:
            ctx.reply(f"Команда /qk работает только в группе с ролью {SWITCH_ROLE}.", rule="command.qk",
                      kind="command_reply")
            return
        answer = await self._command(rec.text or "", rec.sender_id, rec.chat_id, rec.reply_to)
        ctx.reply(answer, rule="command.qk", kind="command_reply")

    async def _command(self, text: str, user: int | None, chat: int, reply_to: int | None) -> str:
        match = COMMAND.fullmatch(text.strip())
        op, tail = (match.group(1).lower(), match.group(2).strip()) if match else ("", "")
        cat, pool = self.catalog, self.svc.db.pool
        if not op or op in ("help", "?"):
            return HELP
        if op in ("on", "off") and not tail:
            await cat.set_enabled(pool, op == "on", user)
            self.svc.notify(f"{PREFIX}: QuickKast {'включён' if op == 'on' else 'выключен'}; автор {user}")
            return f"QuickKast: {'ON' if cat.enabled else 'OFF'}. Профиль: {cat.active}."
        if op == "add":
            return await self._add(tail, user, chat, reply_to)
        if op == "delete":
            return await self._delete(tail)
        if op == "list":
            if tail.lower() == RESERVED_PROFILE:
                return self._profiles()
            profile = cat.find_profile(tail) if tail else cat.active
            if profile is None:
                return f"Профиль «{tail}» не найден."
            return self._list(profile)
        if op == "set":
            name = tail[len("profile"):].strip() if tail.lower().startswith("profile ") else tail
            return await self._set_profile(name or STANDARD, user)
        if op == "profile":
            if tail.lower() == "clear":
                return await self._set_profile(STANDARD, user)
            if tail.lower() == "list":
                return self._profiles()
            return "Используйте /qk profile list или /qk profile clear."
        if op == "stats":
            return await self._stats()
        return "Неизвестная команда QuickKast. /qk — справка."

    async def _add(self, tail: str, user: int | None, chat: int, reply_to: int | None) -> str:
        cat, pool = self.catalog, self.svc.db.pool
        if not tail:
            return "Укажите триггер: /qk add ТРИГГЕР ТЕКСТ."
        word = WORD.fullmatch(tail)
        assert word is not None  # tail is not empty
        first, more = word.group(1), word.group(2)
        if first.lower() == RESERVED_PROFILE:
            name = more.strip()
            if not name:
                return "Укажите название профиля."
            if name.casefold() == RESERVED_PROFILE:
                return "Имя profile запрещено. Выберите другое имя."
            created = await cat.create_profile(pool, name)
            if created:
                self.svc.notify(f"{PREFIX}: создан профиль {name}; автор {user}")
            return f"Профиль «{name}» создан. Выбрать: /qk set {name}" if created else f"Профиль «{name}» уже есть."
        trigger = first.strip()
        if trigger.lower() in RESERVED_TRIGGERS:
            return "Триггеры x, х и v зарезервированы."
        if VARIANT_SUFFIX.fullmatch(trigger):
            return "Триггер не может кончаться на -N или -list: так выбирают варианты."
        template = more.strip("\n").strip()
        if not template and reply_to is not None:
            template = str(await pool.fetchval(
                "SELECT text FROM messages WHERE chat_id = $1 AND msg_id = $2 AND text IS NOT NULL "
                "ORDER BY version DESC LIMIT 1", chat, reply_to) or "").strip()
        if not template:
            return "Нет текста шаблона: /qk add ТРИГГЕР ТЕКСТ или ответом на сообщение с текстом."
        variant = await cat.add_variant(pool, cat.active, trigger, template)
        self.svc.notify(f"{PREFIX}: шаблон {trigger}-{variant} в профиле {cat.active}; автор {user}")
        return f"QuickKast «{trigger}», вариант {variant}, сохранён в профиле {cat.active}."

    async def _delete(self, value: str) -> str:
        cat, pool = self.catalog, self.svc.db.pool
        if not value:
            return "Укажите триггер: /qk delete ТРИГГЕР или ТРИГГЕР-N."
        trigger, variant = value, None
        suffix = VARIANT_SUFFIX.fullmatch(value)
        if suffix and suffix.group(2).casefold() != "list" and cat.variants(cat.active, suffix.group(1)):
            trigger, variant = suffix.group(1), int(suffix.group(2))
        deleted = await cat.delete(pool, cat.active, trigger, variant)
        if deleted:
            return f"QuickKast «{value}» удалён из профиля {cat.active}."
        return f"QuickKast «{value}» в профиле {cat.active} не найден."

    def _list(self, profile: str) -> str:
        cat = self.catalog
        rows = cat.listing(profile)
        active = " (активный)" if profile.casefold() == cat.active.casefold() else ""
        head = f"QuickKast: {'ON' if cat.enabled else 'OFF'}\nПрофиль: {profile}{active}"
        if not rows:
            return f"{head}\nСписок пуст."
        return "\n\n".join([head, *(f"{t}-{v} — {text}" for t, v, text in rows)])

    def _profiles(self) -> str:
        cat = self.catalog
        names = sorted(cat.profiles.values(), key=str.casefold)
        return "\n".join(["Профили QuickKast:",
                          *(f"{'•' if n.casefold() == cat.active.casefold() else '◦'} {n}" for n in names)])

    async def _set_profile(self, name: str, user: int | None) -> str:
        profile = self.catalog.find_profile(name)
        if profile is None:
            return f"Профиль «{name}» не найден."
        await self.catalog.set_active(self.svc.db.pool, profile, user)
        self.svc.notify(f"{PREFIX}: активный профиль {profile}; автор {user}")
        return f"Активный профиль QuickKast: {profile}."

    async def _stats(self) -> str:
        row = await self.svc.db.pool.fetchrow(
            "WITH e AS (SELECT extract(epoch FROM greatest(deleted_at, sent_at) - received_at) * 1000 AS total, "
            "extract(epoch FROM sent_at - received_at) * 1000 AS send, "
            "extract(epoch FROM deleted_at - received_at) * 1000 AS del, "
            "extract(epoch FROM stored_at - received_at) * 1000 AS db FROM qk_events "
            "WHERE status = 'done' AND kind = 'template' AND received_at > now() - interval '24 hours') "
            "SELECT count(*) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY total) AS p50, "
            "percentile_cont(0.9) WITHIN GROUP (ORDER BY total) AS p90, max(total) AS mx, "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY send) AS send50, "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY del) AS del50, "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY db) AS db50 FROM e")
        bad = await self.svc.db.pool.fetchval(
            "SELECT count(*) FROM qk_events WHERE status <> 'done' AND received_at > now() - interval '24 hours'")
        if not row or not row["n"]:
            return f"QuickKast за 24 часа: срабатываний нет. Неудач: {bad}."

        def ms(value: Any) -> str:
            return f"{float(value):.0f} мс" if value is not None else "—"

        return (f"QuickKast за 24 часа: {row['n']} шаблонов, неудач {bad}.\n"
                f"От прихода триггера до ответа Telegram на удаление и отправку: "
                f"p50 {ms(row['p50'])}, p90 {ms(row['p90'])}, max {ms(row['mx'])}.\n"
                f"p50 по этапам: запись в БД {ms(row['db50'])}, удаление {ms(row['del50'])}, "
                f"отправка {ms(row['send50'])}.")
