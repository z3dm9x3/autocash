"""orders: the module "orders" - deposits and withdrawals of the kassas (orders_readme.md, D-ORDERS-1..7).

Now it is filled from the archive of Cash Monitor Dev (orders_history.py: inside PostgreSQL, through dblink)
and answers the staff commands /dev dep and /dev ww, written in the group logs. A reply and the command are
deleted after `delete_after_s` (the Outbox action delete: the sender first, then the other accounts).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

from pydantic import BaseModel

from autocash.core.commands import render
from autocash.core.commands.rights import can_run, deny
from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, EntityDecl, Event, Module, Services
from autocash.modules.orders import orders_store
from autocash.modules.orders.orders_history import HistoryRunner
from autocash.modules.orders.orders_store import HISTORY_SOURCE, Found
from autocash.modules.players import players_store

log = logging.getLogger(__name__)

NAME = "orders"
COMMAND = "dev"
MSK = ZoneInfo("Europe/Moscow")  # business time of the project

HELP = ("Команды:\n"
        "/dev dep mel <MelID> [N] — депозиты MelID\n"
        "/dev dep tg <TGID> [N] — депозиты TGID\n"
        "/dev dep player <TGID | MelID | tg:… | mel:… | #игрок> [N] — депозиты всех аккаунтов игрока\n"
        "/dev ww mel|tg|player … [N] — то же для выводов\n"
        "/dev hist [pause | resume] — чтение истории Cash Monitor Dev")

DEPOSIT_MARK = {"success": "✅", "rejected": "❌ отклонён", "cancelled": "✖ отменён", "receipt": "🧾 чек",
                "open": "⏳ открыт"}
WITHDRAWAL_MARK = {"success": "✅", "bare_ok": "✅ (без текста)", "fail": "❌", "bare_fail": "❌ (без текста)",
                   "none": "⏳ без итога"}


class HistoryCfg(BaseModel):
    enabled: bool = False
    dsn_env: str = "AUTOCASH_HISTORY_DSN"  # the connection string of the archive, if set in the environment
    source_ini: str | None = None  # else: settings.ini of Cash Monitor Dev (read at start, never printed)
    source_section: str = "postgres"
    source_key: str = "dsn"
    batch: int = 1000  # raw_messages ids per step
    interval_s: float = 5.0  # between steps of the first pass
    idle_interval_s: float = 30.0  # between steps at the end of the archive (the tail)
    refresh_interval_s: float = 120.0  # cards still open are read again this often ...
    refresh_days: int = 3  # ... if they are this young
    settle_interval_s: float = 120.0  # roles and pairs of new cards, after the first pass
    statement_timeout_ms: int = 60000  # of the remote queries
    error_pause_s: float = 60.0
    bots: list[int] = [7185799444, 7973286850]  # the kassa platform bot, the Polygon coordinator
    exclude_tg: list[int] = []  # never users of a kassa (more service accounts)


class OrdersConfig(BaseModel):
    history: HistoryCfg = HistoryCfg()
    delete_after_s: float = 300.0  # /dev: the command and its reply are deleted (0: kept)
    list_limit: int = 20  # lines of a reply by default
    list_max: int = 60
    staff_user_types: list[str] = ["CASHIER", "ADMIN"]  # «Тип пользователя» of staff making deposits for others


class OrdersModule(Module):
    name: ClassVar[str] = NAME
    migrations: ClassVar[Path | None] = Path(__file__).parent / "orders_migrations"
    subscriptions: ClassVar[list[str]] = [f"command.{COMMAND}"]
    commands: ClassVar[list[str]] = [COMMAND]
    commands_logs_chat: ClassVar[list[str]] = [COMMAND]
    entities: ClassVar[list[EntityDecl]] = [EntityDecl("order", "orders_deposits, orders_withdrawals")]
    config_model: ClassVar[type[BaseModel] | None] = OrdersConfig
    periodic_s: ClassVar[float | None] = 1.0  # the history paces itself (HistoryCfg)
    timeout_s: ClassVar[float] = 30.0

    def __init__(self) -> None:
        self.history = HistoryRunner(self)

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    def cfg(self) -> OrdersConfig:
        mod = self.svc.runtime().config.modules.get(NAME)
        return OrdersConfig.model_validate(mod.config if mod is not None else {})

    async def start(self) -> None:
        if await self.svc.db.pool.fetchval("SELECT to_regclass('players_links') IS NULL OR "
                                           "to_regclass('rm_users') IS NULL"):
            raise RuntimeError("orders needs the modules role_manager and players in `modules` of the configuration")

    async def periodic(self) -> None:
        await self.history.tick(self.cfg())

    # ---- /dev ------------------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if event.msg_type != f"command.{COMMAND}" or not event.external:
            return
        rt = self.svc.runtime()
        if not can_run(rt, rec.sender_id, rec.chat_id, COMMAND):
            deny(self.svc.notify, rt, rec.sender_id, rec.chat_id, COMMAND)  # the chat gets nothing (D-RM-8)
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        cfg = self.cfg()
        args = [str(a) for a in rec.fields.get("args") or []]
        try:
            text = await self.run(args, str(rec.sender_id), cfg)
        except ValueError as exc:
            text = str(exc)
        reply = ctx.reply(render.clip(text), rule=f"command.{COMMAND}", kind="command_reply")
        if cfg.delete_after_s > 0:
            # The reply carries players' data: it and the command go away (D-ORDERS-7, D-OBX-8).
            reply.delete_after_s = cfg.delete_after_s
            ctx.drafts.append(OutboundDraft(
                chat_id=rec.chat_id, text="", rule=f"command.{COMMAND}.delete", kind="command_reply",
                action="delete", target_msg_id=rec.msg_id, not_before_s=cfg.delete_after_s,
                prefer_account=rec.account_id))

    async def run(self, args: list[str], author: str, cfg: OrdersConfig) -> str:
        if not args:
            return HELP
        what = args[0].lower()
        if what in ("dep", "ww"):
            return await self._orders(what, args[1:], cfg)
        if what == "hist":
            return await self._history(args[1:], author)
        raise ValueError(f"Неизвестная команда /dev {args[0]}.\n{HELP}")

    async def _orders(self, what: str, args: list[str], cfg: OrdersConfig) -> str:
        if len(args) < 2 or args[0].lower() not in ("mel", "tg", "player"):
            raise ValueError(f"Формат: /dev {what} mel|tg|player <идентификатор> [N]")
        by, ident = args[0].lower(), args[1]
        limit = min(int(args[2]), cfg.list_max) if len(args) > 2 and args[2].isdigit() else cfg.list_limit
        db = self.svc.db.reports
        if by == "mel":
            tgs, mels, title = [], [ident], f"MelID {ident}"
        elif by == "tg":
            if not ident.isdigit():
                raise ValueError("TGID — число.")
            tgs, mels, title = [int(ident)], [], f"TGID {ident}"
        else:
            found = await players_store.resolve(db, ident)
            if found.ambiguous:
                return (f"«{ident}» есть у нескольких игроков (#{', #'.join(map(str, found.ambiguous))}). "
                        f"Уточните: tg:{ident}, mel:{ident} или #игрок.")
            if found.player_id is None:
                return f"Игрок «{ident}» не найден."
            ids = await players_store.idents(db, found.player_id)
            tgs, mels = [int(t) for t in ids["tg"]], ids["mel"]
            title = f"игрока #{found.player_id} (TGID {len(tgs)}, MelID {len(mels)})"
        if what == "dep":
            return _deposits_text(title, await orders_store.deposits(db, tgs, mels, limit), by)
        return _withdrawals_text(title, await orders_store.withdrawals(db, tgs, mels, limit), by)

    async def _history(self, args: list[str], author: str) -> str:
        pool = self.svc.db.pool
        action = args[0].lower() if args else "status"
        if action in ("pause", "resume"):
            state = "paused" if action == "pause" else "running"
            await pool.execute("INSERT INTO orders_history (source, state) VALUES ($1, $2) "
                               "ON CONFLICT (source) DO UPDATE SET state = EXCLUDED.state", HISTORY_SOURCE, state)
            self.history.next_at = 0.0
            word = "остановлено" if state == "paused" else "продолжено"
            self.svc.notify(f"orders: чтение истории {word}; автор {author}")  # a side effect (AGENTS.md, rule 9)
            return f"Чтение истории {word}."
        if action != "status":
            raise ValueError("Формат: /dev hist [pause | resume]")
        return _history_text(await orders_store.history(self.svc.db.reports), self.cfg())


# ---- texts -----------------------------------------------------------------------------------------------
def _money(x: Any) -> str:
    if x is None:
        return "—"
    v = float(x)
    text = f"{v:,.0f}" if v == int(v) else f"{v:,.2f}"
    return text.replace(",", " ")


def _when(dt: datetime | None) -> str:
    return dt.astimezone(MSK).strftime("%d.%m.%y %H:%M") if dt is not None else "—"


def _period(found: Found) -> str:
    if found.first is None:
        return ""
    return f"\nс {_when(found.first)} по {_when(found.last)} (МСК)"


def _deposits_text(title: str, found: Found, by: str) -> str:
    if not found.total:
        return f"Депозитов {title} нет."
    parts = []
    for status in ("success", "rejected", "cancelled", "receipt", "open"):
        n, s = found.by_status.get(status, (0, 0.0))
        if n:
            money = f" на {_money(s)} ₽" if status == "success" else ""
            parts.append(f"{DEPOSIT_MARK[status].split()[0]} {n}{money}")
    lines = [f"Депозиты {title}: {found.total} · " + " · ".join(parts) + _period(found)]
    for r in found.rows:
        who = []
        if by != "mel":
            who.append(f"mel {r['mel_id'] or '—'}")
        if by != "tg":
            who.append(f"tg {r['tg_id'] or '—'}")
        amount = r["receipt_amount"] if r["status"] == "success" and r["receipt_amount"] is not None else r["amount"]
        mark = DEPOSIT_MARK.get(r["status"], r["status"])
        if r["status"] in ("cancelled", "rejected") and r["cancel_reason"]:
            mark += f" ({r['cancel_reason'][:40]})"
        lines.append(f"• {_when(r['created_at'])} #{r['deposit_id']} · {r['title'] or r['chat_id']} · "
                     f"{_money(amount)} · {mark} · " + ", ".join(who))
    if found.total > len(found.rows):
        lines.append(f"… и ещё {found.total - len(found.rows)}")
    return "\n".join(lines)


def _withdrawals_text(title: str, found: Found, by: str) -> str:
    if not found.total:
        return f"Выводов {title} нет."
    ok = [found.by_status.get(s, (0, 0.0)) for s in ("success", "bare_ok")]
    bad = sum(found.by_status.get(s, (0, 0.0))[0] for s in ("fail", "bare_fail"))
    none = found.by_status.get("none", (0, 0.0))[0]
    head = f"Выводы {title}: {found.total} · ✅ {ok[0][0] + ok[1][0]} на {_money(ok[0][1] + ok[1][1])} ₽"
    head += (f" · ❌ {bad}" if bad else "") + (f" · ⏳ {none}" if none else "")
    lines = [head + _period(found)]
    for r in found.rows:
        who = []
        if by != "mel":
            who.append(f"mel {r['mel_id'] or '—'}")
        if by != "tg":
            who.append(f"tg {r['tg_id'] or '—'}")
        mark = WITHDRAWAL_MARK.get(r["result"], r["result"])
        if r["result"] == "fail" and r["result_text"]:
            mark += f" {r['result_text'][:50]}"
        lines.append(f"• {_when(r['created_at'])} · {r['title'] or r['chat_id']} · {_money(r['amount'])} · {mark} · "
                     + ", ".join(who))
    if found.total > len(found.rows):
        lines.append(f"… и ещё {found.total - len(found.rows)}")
    return "\n".join(lines)


def _history_text(h: dict[str, Any], cfg: OrdersConfig) -> str:
    st = h["state"]
    if st is None:
        on = "включено" if cfg.history.enabled else "выключено в конфигурации (orders.history.enabled)"
        return f"История Cash Monitor Dev ещё не читалась; чтение {on}."
    top = st["top_id"] or 0
    share = f" ({100 * st['last_id'] // top} %)" if top else ""
    state = "пауза" if st["state"] == "paused" else ("идёт" if cfg.history.enabled else "выключено в конфигурации")
    lines = [
        f"История Cash Monitor Dev: {state}",
        f"Прочитано: до id {st['last_id']} из {top}{share}; шагов {st['steps']}, карточек {st['cards']}",
        "Первый проход: " + (f"завершён {_when(st['caught_up_at'])}" if st["caught_up_at"] else "идёт"),
        f"Депозитов: {h['deposits']}, выводов: {h['withdrawals']}, касс: {h['kassas']}",
        f"Пары TGID—MelID из истории: {h['links']} (персонал: {h['staff_links']}); игроков всего: {h['players']}",
        f"Последний шаг: {_when(st['last_step_at'])}",
    ]
    if st["last_error"]:
        lines.append(f"Последняя ошибка ({_when(st['last_error_at'])}): {st['last_error'][:300]}")
    return "\n".join(lines)
