"""Core module "reactions": rules of reaction, commands as rules, dialogs, timers, dry-run (stage 3).

Every external action becomes an outbound task in the SAME transaction as the processing mark
and the dialog state (FT-8.3, FT-8.9), with the key "event + rule + step": a repeat after a
crash finds the task and sends nothing twice. Safety first (NFT-12): automatic answers of a
rule marked `autoreply` stop for N minutes after a staff member wrote into the dialog, obey
per-dialog and per-account limits and stop on a restricted account.
"""

from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime, timedelta
from fnmatch import fnmatchcase
from typing import Any, ClassVar

import asyncpg

from autocash.actions.send import KIND_PRIORITY, OutboundDraft, insert_outbound
from autocash.core.commands.rights import can_run, deny
from autocash.core.config.schema import CommandArgCfg, ReactionRuleCfg
from autocash.core.reactions.engine import Draft, Effects, Runner, log_row
from autocash.core.record import MessageRecord
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, Event, Module

_IDENT = re.compile(r"^(?:(?:tg|mel):)?[\w-]+$")
_LOG = ("INSERT INTO rule_log (trace_id, module, rule, rule_version, config_version, input, actions, tasks) "
        "VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb)")
_TIMER = ("INSERT INTO timers (dedup_key, timer, fire_at, payload, trace_id, rule) "
          "VALUES ($1, $2, now() + make_interval(secs => $3::float8), $4::jsonb, $5::uuid, $6) "
          "ON CONFLICT (dedup_key) DO NOTHING")
_SIMULATION = (
    "INSERT INTO outbound_tasks (trace_id, parent_trace_id, dedup_key, module, rule, rule_version, kind, priority, "
    "chat_id, reply_to, text, random_id, stage, status, finished_at, config_version, dialog_account_id) "
    "VALUES (gen_random_uuid(), $1::uuid, $2, 'reactions', $3, $4, $5, $6, $7, $8, $9, 0, 'created', 'simulation', "
    "now(), $10, $11) ON CONFLICT (dedup_key) DO NOTHING")


class ArgError(ValueError):
    pass


def parse_args(spec: list[CommandArgCfg], raw: list[str], command: str) -> dict[str, Any]:
    """Arguments are checked by type (FT-12.3)."""
    out: dict[str, Any] = {}
    usage = f"/{command} " + " ".join(f"<{a.name}>" if not a.optional else f"[{a.name}]" for a in spec)
    i = 0
    for a in spec:
        if a.type == "text":
            value = " ".join(raw[i:]).strip()
            i = len(raw)
        else:
            value = raw[i] if i < len(raw) else ""
            i += 1
        if not value:
            if a.optional:
                out[a.name] = None
                continue
            raise ArgError(f"Не хватает аргумента {a.name}. Формат: {usage}")
        if a.type == "int" and not value.isdigit():
            raise ArgError(f"{a.name} должен быть числом. Формат: {usage}")
        if a.type == "ident" and not _IDENT.match(value):
            raise ArgError(f"{a.name}: TGID, MelID или tg:/mel:<номер>. Формат: {usage}")
        out[a.name] = int(value) if a.type == "int" else value
    return out


def event_data(rec: MessageRecord) -> dict[str, Any]:
    return {"event": {
        "type": rec.msg_type, "fields": dict(rec.fields), "text": rec.text, "chat_id": rec.chat_id,
        "msg_id": rec.msg_id, "sender_id": rec.sender_id, "sender_role": rec.sender_role, "origin": rec.origin,
        "source": rec.source, "account_id": rec.account_id, "trace_id": rec.trace_id,
    }, "type": rec.msg_type, "fields": dict(rec.fields), "r": {}}


class ReactionsModule(Module):
    name: ClassVar[str] = "reactions"
    subscriptions: ClassVar[list[str]] = ["*", "unknown"]  # answers to questions are usually untyped text
    concurrency: ClassVar[int] = 8
    timeout_s: ClassVar[float] = 10.0
    periodic_s: ClassVar[float | None] = 1.0  # timers of `schedule`

    def __init__(self) -> None:
        self._awaiting: dict[str, float] = {}  # "chat:user" -> state_until (epoch); a cache of open questions
        self._loaded = False

    # ---- helpers -------------------------------------------------------------------------------------
    def _rt(self) -> Any:
        assert self.services is not None
        return self.services.runtime()

    def _notify(self, text: str) -> None:
        assert self.services is not None
        self.services.notify(text)

    def _runner(self, rule: ReactionRuleCfg, data: dict[str, Any], *, external: bool) -> Runner:
        assert self.services is not None
        svc = self.services.runtime().config.service_chats
        simulate = rule.mode == "dry_run"
        return Runner(rule, data, reads=self.services.db.pool, reports=self.services.db.reports,
                      external=external, notify_chats={"logs": svc.logs, "debug": svc.debug}, simulate=simulate,
                      ask_timeout_s=self.services.runtime().config.reactions.ask_timeout_s)

    async def _load_awaiting(self) -> None:
        if self._loaded or self.services is None:
            return
        rows = await self.services.db.pool.fetch(
            "SELECT entity_key, extract(epoch FROM state_until) AS until FROM entities "
            "WHERE entity_type = 'dialog' AND state = 'awaiting' AND state_until > now()")
        self._awaiting = {r["entity_key"]: float(r["until"]) for r in rows}
        self._loaded = True

    def wants(self, rec: MessageRecord) -> bool:
        """An untyped message is wanted only as the answer to an open question."""
        if rec.msg_type != "unknown":
            return True
        return self._awaiting.get(f"{rec.chat_id}:{rec.sender_id}", 0) > time.time()

    # ---- events --------------------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if not event.external:
            return  # reactions fire for senders outside the pool (FT-2.10)
        await self._load_awaiting()
        rt = self._rt()
        key = f"{rec.chat_id}:{rec.sender_id}"
        if self._awaiting.get(key, 0) > time.time() and rec.event == "new" and await self._answer(rec, ctx, key):
            return
        msg_type = rec.msg_type or ""
        if msg_type == "command.dryrun":
            await self._dryrun_command(rec, ctx)
            return
        command = msg_type.removeprefix("command.") if msg_type.startswith("command.") else None
        rules = sorted((r for r in rt.config.reactions.rules if r.enabled), key=lambda r: (r.priority, r.id))
        if command is not None:
            rules = [r for r in rules if r.when.command == command]
            if rules and not can_run(rt, rec.sender_id, rec.chat_id, command):
                deny(self._notify, rt, rec.sender_id, rec.chat_id, command)  # the chat gets nothing (D-RM-8)
                ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
                return
        else:
            rules = [r for r in rules if r.when.type and any(fnmatchcase(msg_type, m) for m in r.when.type)]
        for rule in rules:
            data = event_data(rec)
            if command is not None:
                try:
                    data["args"] = parse_args(rule.when.args, [str(a) for a in rec.fields.get("args") or []], command)
                except ArgError as exc:
                    ctx.reply(str(exc), rule=f"{rule.id}.usage", kind="command_reply")
                    continue
                data["command"] = command
            await self._fire(rule, data, rec, ctx)

    async def _answer(self, rec: MessageRecord, ctx: Context, key: str) -> bool:
        """The awaited answer to `ask`: the rule runs again with the answer as its argument."""
        assert self.services is not None
        row = await self.services.db.pool.fetchrow(
            "SELECT attrs FROM entities WHERE entity_type = 'dialog' AND entity_key = $1 AND state = 'awaiting' "
            "AND state_until > now()", key)
        self._awaiting.pop(key, None)
        if row is None:
            return False
        attrs = json.loads(row["attrs"]) if isinstance(row["attrs"], str) else dict(row["attrs"])
        rule = next((r for r in self._rt().config.reactions.rules if r.id == attrs.get("rule") and r.enabled), None)
        ctx.execute("UPDATE entities SET state = 'answered', state_until = NULL, timeout_state = NULL, "
                    "updated_at = now() WHERE entity_type = 'dialog' AND entity_key = $1 AND state = 'awaiting'", key)
        if rule is None:
            return True
        data = event_data(rec)
        data["args"] = dict(attrs.get("args") or {})
        if attrs.get("arg"):
            data["args"][attrs["arg"]] = rec.text.strip()
        data["dialog"] = attrs
        if rule.when.command:
            data["command"] = rule.when.command
        await self._fire(rule, data, rec, ctx)
        return True

    async def _blocked(self, rule: ReactionRuleCfg, rec: MessageRecord) -> str | None:
        """Why an automatic answer must not go now (FT-8.10, NFT-12), or None."""
        assert self.services is not None
        cfg = self._rt().config.reactions
        pool = self.services.db.pool
        if self.services.account_state(rec.account_id) == "restricted":
            return "account restricted"
        human = await pool.fetchval(
            "SELECT 1 FROM messages WHERE chat_id = $1 AND origin = 'human' "
            "AND msg_date > now() - make_interval(mins => $2::int) LIMIT 1", rec.chat_id, int(cfg.autoreply_pause_min))
        if human:
            return f"a staff member wrote in the last {cfg.autoreply_pause_min:g} min"
        dialog = await pool.fetchval(
            "SELECT count(*) FROM outbound_tasks WHERE module = 'reactions' AND chat_id = $1 "
            "AND created_at > now() - interval '1 hour' AND status <> 'simulation'", rec.chat_id)
        if dialog >= cfg.per_dialog_per_hour:
            return "limit per dialog"
        account = await pool.fetchval(
            "SELECT count(*) FROM outbound_tasks WHERE module = 'reactions' AND dialog_account_id = $1 "
            "AND created_at > now() - interval '1 hour' AND status <> 'simulation'", rec.account_id)
        if account >= cfg.per_account_per_hour:
            return "limit per account"
        return None

    async def _fire(self, rule: ReactionRuleCfg, data: dict[str, Any], rec: MessageRecord, ctx: Context) -> None:
        runner = self._runner(rule, data, external=rec.source != "backfill")
        if not runner.matches():
            return
        if rule.autoreply and rule.mode == "live":
            why = await self._blocked(rule, rec)
            if why is not None:
                ctx.execute(_LOG, *log_row(rule, rec.trace_id, ctx.config_version, data,
                                           Effects(log=[{"skipped": why}]), []))
                return
        effects = await runner.run()
        keys = self.apply(rule, effects, ctx, rec.trace_id)
        if rule.mode == "live":
            for key, wait in effects.asked:
                self._awaiting[key] = time.time() + wait
        ctx.execute(_LOG, *log_row(rule, rec.trace_id, ctx.config_version, data, effects, keys))

    def apply(self, rule: ReactionRuleCfg, effects: Effects, ctx: Context, trace_id: str) -> list[str]:
        """Effects become writes of the event's transaction; dry-run writes only simulations (FT-8.6)."""
        keys = []
        for d in effects.drafts:
            key = f"{trace_id}:reactions:{rule.id}:{d.step}"
            keys.append(key)
            if rule.mode == "dry_run":
                ctx.execute(_SIMULATION, trace_id, key, rule.id, str(rule.version), d.kind,
                            KIND_PRIORITY.get(d.kind, 2), d.chat_id, d.reply_to, d.text, ctx.config_version,
                            d.via_account)
            else:
                ctx.drafts.append(OutboundDraft(
                    chat_id=d.chat_id, text=d.text, rule=f"{rule.id}:{d.step}", kind=d.kind, reply_to=d.reply_to,
                    rule_version=str(rule.version), dedup_key=key, via_account=d.via_account))
        if rule.mode == "dry_run":
            return keys
        for sql, args in effects.sql:
            ctx.execute(sql, *args)
        for t in effects.timers:
            ctx.execute(_TIMER, f"{trace_id}:{rule.id}:{t['timer']}:{t['step']}", t["timer"], t["in_s"],
                        json.dumps(t["payload"], ensure_ascii=False, default=str), trace_id, rule.id)
        return keys

    # ---- timers (`schedule`) --------------------------------------------------------------------------
    async def periodic(self) -> None:
        assert self.services is not None
        rt = self._rt()
        pool = self.services.db.pool
        ids: list[int] = []
        async with pool.acquire() as conn, conn.transaction():
            due = await conn.fetch("SELECT * FROM timers WHERE fired_at IS NULL AND fire_at <= now() "
                                   "ORDER BY fire_at LIMIT 50 FOR UPDATE SKIP LOCKED")
            for t in due:
                payload = json.loads(t["payload"]) if isinstance(t["payload"], str) else dict(t["payload"])
                for rule in [r for r in rt.config.reactions.rules if r.enabled and r.when.timer == t["timer"]]:
                    data = {"timer": t["timer"], "payload": payload, "event": payload.get("event", {}), "r": {}}
                    runner = self._runner(rule, data, external=True)
                    if not runner.matches():
                        continue
                    effects = await runner.run()
                    ids += await self._apply_direct(conn, rule, effects, f"timer:{t['id']}", t["trace_id"],
                                                    rt.version)
                    await conn.execute(_LOG, *log_row(rule, str(t["trace_id"]) if t["trace_id"] else None,
                                                      rt.version, data, effects, []))
                await conn.execute("UPDATE timers SET fired_at = now() WHERE id = $1", t["id"])
        if ids:
            self.services.enqueue(ids)

    async def _apply_direct(self, conn: asyncpg.Connection, rule: ReactionRuleCfg, effects: Effects, base: str,
                            parent: Any, version: str) -> list[int]:
        ttl = self._rt().config.output.ttl_s
        ids: list[int] = []
        for d in effects.drafts:
            draft = OutboundDraft(chat_id=d.chat_id, text=d.text, rule=f"{rule.id}:{d.step}", kind=d.kind,
                                  reply_to=d.reply_to, rule_version=str(rule.version),
                                  dedup_key=f"{base}:{rule.id}:{d.step}", via_account=d.via_account)
            task_id = await insert_outbound(conn, draft, parent_trace_id=str(parent) if parent else None,
                                            module=self.name, ttl_s=ttl.get(d.kind, 3600.0), config_version=version)
            if task_id is not None:
                ids.append(task_id)
        for sql, args in effects.sql:
            await conn.execute(sql, *args)
        for t in effects.timers:
            await conn.execute(_TIMER, f"{base}:{rule.id}:{t['timer']}:{t['step']}", t["timer"], t["in_s"],
                               json.dumps(t["payload"], default=str), str(parent) if parent else None, rule.id)
        return ids

    # ---- dry-run over history (FT-8.6) ---------------------------------------------------------------------
    async def _dryrun_command(self, rec: MessageRecord, ctx: Context) -> None:
        if not can_run(self._rt(), rec.sender_id, rec.chat_id, "dryrun"):
            deny(self._notify, self._rt(), rec.sender_id, rec.chat_id, "dryrun")
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        args = [str(a) for a in rec.fields.get("args") or []]
        if not args:
            ctx.reply("Формат: /dryrun <id правила> [часы]", rule="command.dryrun", kind="command_reply")
            return
        hours = float(args[1]) if len(args) > 1 and args[1].replace(".", "", 1).isdigit() else 24.0
        ctx.reply(await self.replay(args[0], hours), rule="command.dryrun", kind="command_reply")

    async def replay(self, rule_id: str, hours: float, limit: int = 5000) -> str:
        """What the rule would have done over the stored history: nothing is written or sent."""
        assert self.services is not None
        rule = next((r for r in self._rt().config.reactions.rules if r.id == rule_id), None)
        if rule is None:
            return f"Правило {rule_id} не найдено."
        types = [f"command.{rule.when.command}"] if rule.when.command else rule.when.type
        if not types:
            return f"Правило {rule_id} срабатывает по таймеру: прогон по истории неприменим."
        since = datetime.now(UTC) - timedelta(hours=hours)
        rows = await self.services.db.reports.fetch(
            "SELECT * FROM messages WHERE msg_date >= $1 AND sorted_at IS NOT NULL AND origin = 'external' "
            "AND policy = 'full' ORDER BY msg_date LIMIT $2", since, limit)
        seen = fired = 0
        sends: list[Draft] = []
        simulated = rule.model_copy(update={"mode": "dry_run"})
        for row in rows:
            rec = MessageRecord.from_row(row)
            if not any(fnmatchcase(rec.msg_type or "", m) for m in types):
                continue
            seen += 1
            data = event_data(rec)
            if rule.when.command:
                try:
                    data["args"] = parse_args(rule.when.args, [str(a) for a in rec.fields.get("args") or []],
                                              rule.when.command)
                except ArgError:
                    continue
                data["command"] = rule.when.command
            runner = Runner(simulated, data, reads=None, reports=None, external=True,
                            notify_chats={}, simulate=True)
            if not runner.matches():
                continue
            fired += 1
            sends += (await runner.run()).drafts
        lines = [f"Прогон правила {rule_id} (v{rule.version}) за {hours:g} ч: подходящих событий {seen}, "
                 f"сработало бы {fired}, отправилось бы сообщений {len(sends)}."]
        lines += [f"• в {d.chat_id}: {d.text[:120]}" for d in sends[:10]]
        return "\n".join(lines)
