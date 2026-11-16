"""Module "deposits" (FT-З.1..З.11): payment requests of the kassas, their stages and statistics.

The module knows message TYPES and FIELDS only; how a kassa bot's text becomes a type with fields
is the sorting template in the configuration (P-2, P-3). Types it understands (names in config):

    request     a request was created; its edit may cancel it          deposit_id, tgid, mel_id, amount, method
    requisites  a cashier gave requisites                              (requisite features of the text)
    sent        the bot sent the requisites to the player              tgid, bonus, total
    receipt     "[ДЕПОЗИТ]": the receipt came; its edit is the final   deposit_id, amount, markers
    ocr         an OCR warning about a deposit                         deposit_id
    method_off / method_on / method_limits                             method_id, method, min, max
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import asyncpg
from pydantic import BaseModel

from autocash.actions.entities import Candidate, Window, register_report, score
from autocash.core.commands.rights import can_run
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, EntityDecl, Event, Module, TraceResolver
from autocash.modules.deposits import store
from autocash.modules.deposits.store import Msg, amount


class DepositsConfig(BaseModel):
    types: dict[str, str] = {
        "request": "deposit.request", "requisites": "deposit.requisites", "sent": "deposit.sent",
        "receipt": "deposit.receipt", "ocr": "deposit.ocr", "method_off": "method.off", "method_on": "method.on",
        "method_limits": "method.limits",
    }
    # OPEN-QUESTION: FT-З.9 — how requisites are tied to a request, in order of preference:
    # "reply" (the cashier answered the request) and "confirmation" (the next bot confirmation with TGID).
    binding: list[str] = ["reply", "confirmation"]
    success_markers: list[str] = ["✅"]
    reject_markers: list[str] = ["❌"]
    reconcile_after_min: float = 30  # OPEN-QUESTION: FT-З.10 — X
    stuck_after_h: float = 24  # OPEN-QUESTION: FT-З.11 — Y
    trusted_window_min: float = 120  # a request without Deposit ID is matched within this window (FT-З.7)
    reread_batch: int = 200
    # OPEN-QUESTION: stage 2 — the share of unknown among kassa bot messages that is still acceptable.
    unknown_share_max: float = 0.05


class DepositsModule(Module):
    name: ClassVar[str] = "deposits"
    subscriptions: ClassVar[list[str]] = ["deposit.*", "method.*", "command.summ"]
    entities: ClassVar[list[EntityDecl]] = [EntityDecl("deposit", "deposits, deposit_events, payment_methods")]
    commands: ClassVar[list[str]] = ["summ"]
    config_model: ClassVar[type[BaseModel] | None] = DepositsConfig
    concurrency: ClassVar[int] = 8
    periodic_s: ClassVar[float | None] = 60.0

    def __init__(self) -> None:
        register_report("deposits.by_idents", _by_idents)
        self._cfg = DepositsConfig()

    # ---- helpers -----------------------------------------------------------------------------------
    def cfg(self, ctx: Context | None = None) -> DepositsConfig:
        if ctx is not None:
            self._cfg = DepositsConfig.model_validate(ctx.config)
        elif self.services is not None:
            mcfg = self.services.runtime().config.modules.get(self.name)
            self._cfg = DepositsConfig.model_validate(mcfg.config if mcfg else {})
        return self._cfg

    def _tz(self) -> str:
        assert self.services is not None
        return str(self.services.runtime().config.report_timezone)

    @staticmethod
    def _msg(event: Event) -> Msg:
        r = event.record
        return Msg(r.trace_id, r.chat_id, r.msg_id, datetime.fromtimestamp(r.msg_date, UTC), r.sender_id, r.reply_to,
                   r.event, dict(r.fields), list(r.features))

    # ---- events ------------------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        if event.msg_type == "command.summ":
            await self._summ(event, ctx)
            return
        cfg = self.cfg(ctx)
        kind = next((k for k, t in cfg.types.items() if t == event.msg_type), None)
        if kind is None:
            return
        assert self.services is not None
        m = self._msg(event)
        async with self.services.db.pool.acquire() as conn, conn.transaction():
            await store.lock_chat(conn, m.chat_id)
            if kind == "request":
                touched = await self._request(conn, m, cfg)
            elif kind == "requisites":
                touched = await self._requisites(conn, m, cfg)
            elif kind == "sent":
                touched = await self._sent(conn, m, cfg)
            elif kind == "receipt":
                touched = await self._receipt(conn, m, cfg)
            elif kind == "ocr":
                touched = await self._ocr(conn, m)
            else:
                touched = await self._method(conn, m, kind)
            for chat_id, created_at in touched:
                await store.refresh_daily(conn, chat_id, created_at, self._tz())

    async def _request(self, conn: asyncpg.Connection, m: Msg, cfg: DepositsConfig) -> list[tuple[int, datetime]]:
        f = m.fields
        did = str(f.get("deposit_id") or "")
        if not did:
            did = await self._trusted(conn, m, cfg) or f"nodid:{m.chat_id}:{m.msg_id}"
        await store.lock_deposit(conn, did)
        dep = await store.get(conn, did)
        if m.event == "edit":
            if dep is None or dep["status"] != "open":
                return []  # after the final the status is fixed (FT-З.5)
            if f.get("cancelled_by") or f.get("cancel_reason"):
                if not await store.record(conn, did, m, "cancelled",
                                          {"by": f.get("cancelled_by"), "reason": f.get("cancel_reason")}):
                    return []
                await conn.execute("UPDATE deposits SET stage = 'cancelled', status = 'cancelled', final_at = $2, "
                                   "updated_at = now() WHERE deposit_id = $1", did, m.at)
                return [(dep["chat_id"], dep["created_at"])]
            return []
        if dep is None:
            if not await store.record(conn, did, m, "created"):
                return []
            await conn.execute(
                "INSERT INTO deposits (deposit_id, chat_id, kassas, request_msg_id, tgid, mel_id, method, amount, "
                "stage, created_at) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, 'created', $9)",
                did, m.chat_id, json.dumps([{"chat_id": m.chat_id, "at": m.at.isoformat()}]), m.msg_id,
                _s(f.get("tgid")), _s(f.get("mel_id")), _s(f.get("method")), amount(f.get("amount")), m.at)
            touched = [(m.chat_id, m.at)]
        elif dep["chat_id"] != m.chat_id:
            # The request went on in another kassa (redirected): the history of kassas is kept (FT-З.6).
            if not await store.record(conn, did, m, "moved", {"from": dep["chat_id"]}):
                return []
            await conn.execute(
                "UPDATE deposits SET chat_id = $2, request_msg_id = $3, "
                "kassas = kassas || $4::jsonb, updated_at = now() WHERE deposit_id = $1",
                did, m.chat_id, m.msg_id, json.dumps([{"chat_id": m.chat_id, "at": m.at.isoformat()}]))
            touched = [(dep["chat_id"], dep["created_at"]), (m.chat_id, dep["created_at"])]
        else:
            return []
        # Messages that came before the request was known.
        for p in await conn.fetch("SELECT * FROM deposit_pending WHERE chat_id = $1 AND used_by IS NULL "
                                  "AND kind = 'requisites' AND reply_to = $2 ORDER BY at", m.chat_id, m.msg_id):
            await self._bind_requisites(conn, did, p)
        tgid = _s(f.get("tgid"))
        if tgid and "confirmation" in cfg.binding:
            sent = await conn.fetchrow("SELECT * FROM deposit_pending WHERE chat_id = $1 AND used_by IS NULL "
                                       "AND kind = 'sent' AND tgid = $2 AND at >= $3 ORDER BY at LIMIT 1",
                                       m.chat_id, tgid, m.at)
            if sent is not None:
                await self._apply_sent(conn, did, sent, cfg)
        return touched

    async def _trusted(self, conn: asyncpg.Connection, m: Msg, cfg: DepositsConfig) -> str | None:
        """A request without Deposit ID (TRUSTED users) is tied by TGID, amount and time (FT-З.7)."""
        tgid, value = _s(m.fields.get("tgid")), amount(m.fields.get("amount"))
        if not tgid:
            return None
        rows = await conn.fetch(
            "SELECT deposit_id, amount, created_at FROM deposits WHERE chat_id = $1 AND tgid = $2 AND status = 'open' "
            "AND created_at BETWEEN $3 AND $4", m.chat_id, tgid, m.at - timedelta(minutes=cfg.trusted_window_min),
            m.at + timedelta(minutes=cfg.trusted_window_min))
        cands = [Candidate(r["deposit_id"], 1.0, {"at": r["created_at"].timestamp()}) for r in rows
                 if value is None or r["amount"] is None or r["amount"] == value]
        best = await score(cands, window=Window("at", m.at, cfg.trusted_window_min * 60))
        return best[0].key if best else None

    async def _bind_requisites(self, conn: asyncpg.Connection, did: str, p: Any) -> bool:
        data = json.loads(p["data"]) if isinstance(p["data"], str) else dict(p["data"])
        m = Msg(str(p["trace_id"]), p["chat_id"], p["msg_id"], p["at"], p["sender_id"], p["reply_to"], "new")
        await conn.execute("UPDATE deposit_pending SET used_by = $4 WHERE chat_id = $1 AND msg_id = $2 AND kind = $3",
                           p["chat_id"], p["msg_id"], p["kind"], did)
        if not await store.record(conn, did, m, "requisites", data):
            return False
        await conn.execute(
            "UPDATE deposits SET requisites = requisites || $2::jsonb, cashier_id = coalesce(cashier_id, $3), "
            "requisites_at = coalesce(requisites_at, $4), "
            "stage = CASE WHEN stage = 'created' THEN 'requisites' ELSE stage END, updated_at = now() "
            "WHERE deposit_id = $1 AND status = 'open'",
            did, json.dumps(data.get("requisites", [])), p["sender_id"], p["at"])
        return True

    async def _requisites(self, conn: asyncpg.Connection, m: Msg, cfg: DepositsConfig) -> list[tuple[int, datetime]]:
        if m.event != "new":
            return []
        data = {"requisites": store.masked_requisites(m.features)}
        await store.pend(conn, m, "requisites", data=data)
        if "reply" in cfg.binding and m.reply_to is not None:
            dep = await conn.fetchrow("SELECT deposit_id, chat_id, created_at FROM deposits WHERE chat_id = $1 "
                                      "AND request_msg_id = $2 AND status = 'open'", m.chat_id, m.reply_to)
            if dep is not None:
                p = await conn.fetchrow("SELECT * FROM deposit_pending WHERE chat_id = $1 AND msg_id = $2 "
                                        "AND kind = 'requisites'", m.chat_id, m.msg_id)
                if p is not None and p["used_by"] is None and await self._bind_requisites(conn, dep["deposit_id"], p):
                    return [(dep["chat_id"], dep["created_at"])]
        return []  # waits for its request or for the bot's confirmation

    async def _sent(self, conn: asyncpg.Connection, m: Msg, cfg: DepositsConfig) -> list[tuple[int, datetime]]:
        if m.event != "new":
            return []
        tgid = _s(m.fields.get("tgid"))
        if not tgid:
            return []
        data = {"bonus": _s(m.fields.get("bonus")), "total": _s(m.fields.get("total"))}
        await store.pend(conn, m, "sent", tgid=tgid, data=data)
        dep = await store.unsent_for_tgid(conn, m.chat_id, tgid, m.at)
        if dep is None:
            return []  # the request is not known yet: applied when it comes
        p = await conn.fetchrow("SELECT * FROM deposit_pending WHERE chat_id = $1 AND msg_id = $2 AND kind = 'sent'",
                                m.chat_id, m.msg_id)
        if p is None or p["used_by"] is not None:
            return []
        await self._apply_sent(conn, dep["deposit_id"], p, cfg)
        return [(dep["chat_id"], dep["created_at"])]

    async def _apply_sent(self, conn: asyncpg.Connection, did: str, p: Any, cfg: DepositsConfig) -> None:
        await store.lock_deposit(conn, did)
        dep = await store.get(conn, did)
        if dep is None or dep["status"] != "open":
            return
        if "confirmation" in cfg.binding:
            # Requisites a cashier gave in this kassa since the request (two sets in a row are both kept).
            for r in await conn.fetch("SELECT * FROM deposit_pending WHERE chat_id = $1 AND used_by IS NULL "
                                      "AND kind = 'requisites' AND at BETWEEN $2 AND $3 ORDER BY at",
                                      p["chat_id"], dep["created_at"], p["at"]):
                await self._bind_requisites(conn, did, r)
        data = json.loads(p["data"]) if isinstance(p["data"], str) else dict(p["data"])
        await conn.execute("UPDATE deposit_pending SET used_by = $4 WHERE chat_id = $1 AND msg_id = $2 AND kind = $3",
                           p["chat_id"], p["msg_id"], p["kind"], did)
        m = Msg(str(p["trace_id"]), p["chat_id"], p["msg_id"], p["at"], p["sender_id"], p["reply_to"], "new")
        if not await store.record(conn, did, m, "sent", data):
            return
        await conn.execute(
            "UPDATE deposits SET stage = CASE WHEN stage IN ('created', 'requisites') THEN 'sent' ELSE stage END, "
            "sent_at = coalesce(sent_at, $2), bonus = coalesce($3, bonus), total = coalesce($4, total), "
            "updated_at = now() WHERE deposit_id = $1",
            did, p["at"], amount(data.get("bonus")), amount(data.get("total")))
        await self._check_amount(conn, did)

    async def _receipt(self, conn: asyncpg.Connection, m: Msg, cfg: DepositsConfig) -> list[tuple[int, datetime]]:
        did = _s(m.fields.get("deposit_id"))
        if not did:
            return []
        await store.lock_deposit(conn, did)
        dep = await store.get(conn, did)
        if dep is None or dep["status"] != "open":
            return []
        markers = set(m.fields.get("markers") or [])
        if m.event == "edit" and markers & set(cfg.success_markers + cfg.reject_markers):
            # Until the final every edit is followed; the final fixes the status (FT-З.5).
            status = "success" if markers & set(cfg.success_markers) else "rejected"
            if not await store.record(conn, did, m, "final", {"status": status}):
                return []
            await conn.execute("UPDATE deposits SET stage = 'final', status = $2, final_at = $3, updated_at = now() "
                               "WHERE deposit_id = $1", did, status, m.at)
            return [(dep["chat_id"], dep["created_at"])]
        if m.event != "new":
            return []
        got = amount(m.fields.get("amount"))
        data = {"amount": str(got) if got is not None else None}
        if not await store.record(conn, did, m, "receipt", data):
            return []
        await conn.execute("UPDATE deposits SET stage = 'receipt', receipt_at = coalesce(receipt_at, $2), "
                           "updated_at = now() WHERE deposit_id = $1", did, m.at)
        await self._check_amount(conn, did)
        return [(dep["chat_id"], dep["created_at"])]

    @staticmethod
    async def _check_amount(conn: asyncpg.Connection, did: str) -> None:
        """Receipt amount = requested + bonus (FT-З.4). Checked once both the receipt and the
        confirmation with the bonus are known, whichever came first."""
        row = await conn.fetchrow(
            "SELECT d.amount, d.bonus, d.sent_at, d.marks, e.data AS receipt FROM deposits d "
            "JOIN deposit_events e ON e.deposit_id = d.deposit_id AND e.stage = 'receipt' "
            "WHERE d.deposit_id = $1 ORDER BY e.at LIMIT 1", did)
        if row is None or row["amount"] is None or row["sent_at"] is None:
            return
        marks = json.loads(row["marks"]) if isinstance(row["marks"], str) else row["marks"]
        if any(mk.get("kind") == "amount_mismatch" for mk in marks):
            return
        got = amount((json.loads(row["receipt"]) if isinstance(row["receipt"], str) else row["receipt"]).get("amount"))
        expected = row["amount"] + (row["bonus"] or 0)
        if got is not None and got != expected:
            await conn.execute("UPDATE deposits SET marks = marks || $2::jsonb WHERE deposit_id = $1", did,
                               json.dumps([{"kind": "amount_mismatch", "expected": str(expected), "got": str(got)}]))

    async def _ocr(self, conn: asyncpg.Connection, m: Msg) -> list[tuple[int, datetime]]:
        did = _s(m.fields.get("deposit_id"))
        if not did or not await store.record(conn, did, m, "ocr"):
            return []
        await conn.execute("UPDATE deposits SET marks = marks || $2::jsonb, updated_at = now() WHERE deposit_id = $1",
                           did, json.dumps([{"kind": "ocr", "msg_id": m.msg_id, "text": _s(m.fields.get("text"))}]))
        return []

    async def _method(self, conn: asyncpg.Connection, m: Msg, kind: str) -> list[tuple[int, datetime]]:
        f = m.fields
        name = _s(f.get("method")) or _s(f.get("method_id"))
        if not name:
            return []
        event = {"method_off": "off", "method_on": "on", "method_limits": "limits"}[kind]
        data = {"method_id": _s(f.get("method_id")), "min": _s(f.get("min")), "max": _s(f.get("max"))}
        fresh = await conn.fetchval(
            "INSERT INTO payment_method_history (chat_id, method, event, data, trace_id, at) "
            "VALUES ($1, $2, $3, $4::jsonb, $5::uuid, $6) ON CONFLICT DO NOTHING RETURNING 1",
            m.chat_id, name, event, json.dumps(data), m.trace_id, m.at)
        if not fresh:
            return []
        await conn.execute(
            "INSERT INTO payment_methods (chat_id, name, method_id, enabled, min_amount, max_amount) "
            "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (chat_id, name) DO UPDATE SET "
            "method_id = coalesce(EXCLUDED.method_id, payment_methods.method_id), "
            "enabled = CASE WHEN $7 = 'limits' THEN payment_methods.enabled ELSE EXCLUDED.enabled END, "
            "min_amount = coalesce(EXCLUDED.min_amount, payment_methods.min_amount), "
            "max_amount = coalesce(EXCLUDED.max_amount, payment_methods.max_amount), updated_at = now()",
            m.chat_id, name, data["method_id"], event != "off", amount(data["min"]), amount(data["max"]), event)
        if event != "off":
            return []
        # The bot redirected the open requests of a switched-off method (FT-З.4, "перенаправлена").
        touched = []
        for dep in await conn.fetch("SELECT deposit_id, chat_id, created_at FROM deposits WHERE chat_id = $1 "
                                    "AND status = 'open' AND (method = $2 OR method LIKE $2 || ' %')", m.chat_id, name):
            if await store.record(conn, dep["deposit_id"], m, "redirected", {"method": name}):
                await conn.execute(
                    "UPDATE deposits SET marks = marks || $2::jsonb, updated_at = now() WHERE deposit_id = $1",
                    dep["deposit_id"], json.dumps([{"kind": "redirected", "method": name, "at": m.at.isoformat()}]))
                touched.append((dep["chat_id"], dep["created_at"]))
        return touched

    # ---- periodic work: stuck deposits, reconciliation (FT-З.10, FT-З.11) -------------------------------
    async def periodic(self) -> None:
        assert self.services is not None
        cfg = self.cfg()
        pool = self.services.db.pool
        stuck = await pool.fetch(
            "UPDATE deposits SET stuck = true, updated_at = now() WHERE status = 'open' AND NOT stuck "
            "AND created_at < now() - make_interval(secs => $1) RETURNING deposit_id, chat_id, created_at",
            cfg.stuck_after_h * 3600)
        if stuck:
            async with pool.acquire() as conn, conn.transaction():
                for chat_id, created_at in {(r["chat_id"], r["created_at"]) for r in stuck}:
                    await store.refresh_daily(conn, chat_id, created_at, self._tz())
            listed = ", ".join(f"{r['deposit_id']} ({r['chat_id']})" for r in stuck[:20])
            self.services.notify(f"Заявки без финала дольше {cfg.stuck_after_h:g} ч: {len(stuck)} — {listed}"
                                 + (" …" if len(stuck) > 20 else ""))
        await self.reconcile_open(cfg.reconcile_after_min * 60, cfg.reread_batch)

    async def reconcile_open(self, older_than_s: float, batch: int, chat_id: int | None = None) -> int:
        """Re-read the messages of open deposits from Telegram, a batch of ids per chat (FT-З.10)."""
        assert self.services is not None
        pool = self.services.db.pool
        rows = await pool.fetch(
            "SELECT d.deposit_id, e.chat_id, array_agg(DISTINCT e.msg_id) AS ids FROM deposits d "
            "JOIN deposit_events e ON e.deposit_id = d.deposit_id AND e.chat_id = d.chat_id "
            "WHERE d.status = 'open' AND d.created_at < now() - make_interval(secs => $1) "
            "AND (d.reread_at IS NULL OR d.reread_at < now() - make_interval(secs => $1)) "
            "AND ($3::bigint IS NULL OR d.chat_id = $3) AND e.msg_id IS NOT NULL "
            "GROUP BY d.deposit_id, e.chat_id ORDER BY min(d.created_at) LIMIT $2", older_than_s, batch, chat_id)
        by_chat: dict[int, list[int]] = {}
        done: dict[int, list[str]] = {}
        for r in rows:
            by_chat.setdefault(r["chat_id"], []).extend(r["ids"])
            done.setdefault(r["chat_id"], []).append(r["deposit_id"])
        reread = 0
        for chat, ids in by_chat.items():
            if await self.services.reread(chat, ids):
                reread += len(ids)
                await pool.execute("UPDATE deposits SET reread_at = now() WHERE deposit_id = ANY($1::text[])",
                                   done[chat])
        return reread

    async def reconcile(self, chat_id: int) -> bool:
        """After "difference too long": re-read every open deposit of the kassa (FT-2.7)."""
        if self.services is None:
            return False
        await self.reconcile_open(0, 10_000, chat_id)
        return True

    # ---- /trace and reports ------------------------------------------------------------------------------
    def trace_resolver(self) -> TraceResolver | None:
        async def by_deposit(arg: str) -> list[str]:
            assert self.services is not None
            rows = await self.services.db.reports.fetch(
                "SELECT DISTINCT trace_id::text, min(at) AS at FROM deposit_events WHERE deposit_id = $1 "
                "GROUP BY trace_id ORDER BY at", arg)
            return [r["trace_id"] for r in rows]
        return by_deposit

    async def _summ(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if not event.external:
            return
        assert self.services is not None
        if not can_run(self.services.runtime(), rec.sender_id, rec.chat_id, "summ"):
            ctx.reply("Недостаточно прав для /summ.", rule="command.summ", kind="command_reply")
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        args = [str(a) for a in rec.fields.get("args") or []]
        try:
            text = await self.summary(args, default_chat=rec.chat_id)
        except ValueError as exc:
            text = str(exc)
        ctx.reply(text, rule="command.summ", kind="command_reply")

    async def summary(self, args: list[str], default_chat: int | None = None) -> str:
        """/summ [days] [all | chat_id] [by=cashier|method]: statistics from the daily aggregates (FT-З.8)."""
        assert self.services is not None
        days, chat, by = 1, default_chat, ""
        for a in args:
            if a.isdigit() and int(a) < 400:
                days = int(a)
            elif a == "all":
                chat = None
            elif a.lstrip("-").isdigit():
                chat = int(a)
            elif a in ("by=cashier", "by=method"):
                by = a.split("=")[1]
            else:
                raise ValueError("Формат: /summ [дни] [all | chat_id] [by=cashier | by=method]")
        tz = self._tz()
        group = {"cashier": "cashier_id::text", "method": "method"}.get(by, "''")
        rows = await self.services.db.reports.fetch(
            f"SELECT {group} AS g, sum(created) AS created, sum(success) AS success, sum(rejected) AS rejected, "
            "sum(cancelled) AS cancelled, sum(stuck) AS stuck, sum(amount_sum) AS amount_sum, "
            "sum(requisites_n) AS req_n, sum(requisites_sec) AS req_s, sum(final_n) AS fin_n, sum(final_sec) AS fin_s "
            "FROM deposit_daily WHERE day > (now() AT TIME ZONE $1)::date - $2::int "
            "AND ($3::bigint IS NULL OR chat_id = $3) GROUP BY 1 ORDER BY 2 DESC LIMIT 40",
            tz, days, chat)
        where = "все кассы" if chat is None else f"касса {chat}"
        lines = [f"Заявки за {days} дн. ({where}, сутки по {tz}):"]
        for r in rows:
            n = r["created"] or 0
            if not n:
                continue
            label = f"[{r['g']}] " if by else ""
            avg = (r["amount_sum"] or 0) / n
            req = (r["req_s"] / r["req_n"]) if r["req_n"] else None
            fin = (r["fin_s"] / r["fin_n"]) if r["fin_n"] else None
            lines.append(
                f"{label}{n} заявок, средняя сумма {avg:.2f}; успешных {r['success'] / n:.0%}, "
                f"отклонённых {r['rejected'] / n:.0%}, отменённых {r['cancelled'] / n:.0%}, зависших {r['stuck']}; "
                f"реквизиты за {_dur(req)}, до финала {_dur(fin)}")
        if len(lines) == 1:
            lines.append("Заявок нет.")
        off = await self.services.db.reports.fetch(
            "SELECT chat_id, name FROM payment_methods WHERE NOT enabled AND ($1::bigint IS NULL OR chat_id = $1) "
            "ORDER BY chat_id, name LIMIT 20", chat)
        if off:
            lines.append("Выключенные методы: " + ", ".join(f"{r['name']} ({r['chat_id']})" for r in off))
        return "\n".join(lines)


async def _by_idents(pool: asyncpg.Pool, params: dict[str, Any]) -> list[Any]:
    rows: list[Any] = await pool.fetch(
        "SELECT deposit_id, chat_id, amount, status, created_at FROM deposits "
        "WHERE (tgid = ANY($1::text[]) OR mel_id = ANY($2::text[])) AND created_at >= $3 ORDER BY created_at DESC",
        params["tgids"], params["mel_ids"], params["since"])
    return rows


def _s(value: Any) -> str | None:
    return None if value is None or value == "" else str(value).strip()


def _dur(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    return f"{seconds:.0f} с" if seconds < 120 else f"{seconds / 60:.1f} мин"
