"""Reaction rules: when -> if -> then (FT-8.4, 8.5).

`then` is a chain of actions; the result of an action is available to the next ones as
`r.<as>`, and `if` branches on results. Parameters are values, Jinja2 templates (sandboxed)
or JSONLogic expressions. The engine does not write anything itself: it collects Effects
(outbound drafts, SQL, timers, log lines) that the caller applies in ONE transaction with the
event's processing mark (FT-8.3), or only reports them (dry-run, FT-8.6).

Actions:
    internal  render, set_state, upsert, lookup, link, report, schedule-less ones, if, call
    external  reply, send, notify, ask, schedule
External actions are not executed for history (source=backfill, FT-8.8): they are logged
as skipped.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from jinja2 import TemplateError
from jinja2.sandbox import ImmutableSandboxedEnvironment

from autocash.actions import entities
from autocash.core.config.schema import ReactionRuleCfg
from autocash.core.reactions import jsonlogic

EXTERNAL = {"reply", "send", "notify", "ask", "schedule"}
INTERNAL = {"render", "set_state", "upsert", "lookup", "link", "report", "if", "call", "stop"}
_ENV = ImmutableSandboxedEnvironment(autoescape=False, trim_blocks=True, lstrip_blocks=True)

# Actions added by modules (P-1: the core knows no domain): name -> async fn(params, data) -> result.
ModuleAction = Callable[[dict[str, Any], dict[str, Any]], Awaitable[Any]]
MODULE_ACTIONS: dict[str, ModuleAction] = {}


def register_action(name: str, fn: ModuleAction) -> None:
    MODULE_ACTIONS[name] = fn


class RuleConfigError(ValueError):
    pass


# ---- static validation at config load (FT-13.2) --------------------------------------------------
def _check_params(value: Any, where: str) -> None:
    if isinstance(value, str) and ("{{" in value or "{%" in value):
        try:
            _ENV.parse(value)
        except TemplateError as exc:
            raise RuleConfigError(f"{where}: template error: {exc}") from exc
    elif isinstance(value, dict):
        if len(value) == 1 and next(iter(value)) not in ("action",) and _is_logic(value):
            jsonlogic.validate(value)
        else:
            for k, v in value.items():
                _check_params(v, f"{where}.{k}")
    elif isinstance(value, list):
        for i, v in enumerate(value):
            _check_params(v, f"{where}[{i}]")


def _is_logic(value: dict[str, Any]) -> bool:
    op = next(iter(value))
    try:
        jsonlogic.validate({op: []})
    except jsonlogic.LogicError:
        return False
    return True


def _check_actions(actions: list[dict[str, Any]], where: str) -> None:
    for i, act in enumerate(actions):
        name = act.get("action")
        here = f"{where}[{i}]"
        if not isinstance(name, str):
            raise RuleConfigError(f"{here}: every step needs `action`")
        if name not in EXTERNAL | INTERNAL:
            raise RuleConfigError(f"{here}: unknown action {name!r}")
        if name == "if":
            if "cond" not in act:
                raise RuleConfigError(f"{here}: `if` needs `cond`")
            jsonlogic.validate(act["cond"])
            _check_actions(act.get("then", []), f"{here}.then")
            _check_actions(act.get("else", []), f"{here}.else")
            continue
        if name == "call" and not act.get("name"):
            raise RuleConfigError(f"{here}: `call` needs `name`")
        if name in ("reply", "send", "notify", "ask", "render") and not (act.get("text") or act.get("template")):
            raise RuleConfigError(f"{here}: `{name}` needs `text`")
        for key, value in act.items():
            if key not in ("action", "then", "else"):
                _check_params(value, f"{here}.{key}")


def validate_rule(rule: ReactionRuleCfg) -> None:
    where = f"reactions.{rule.id}"
    if rule.when.command is None and not rule.when.type and rule.when.timer is None:
        raise RuleConfigError(f"{where}: `when` needs type, command or timer")
    try:
        jsonlogic.validate(rule.if_)
    except jsonlogic.LogicError as exc:
        raise RuleConfigError(f"{where}.if: {exc}") from exc
    try:
        _check_actions(rule.then, f"{where}.then")
    except jsonlogic.LogicError as exc:
        raise RuleConfigError(f"{where}: {exc}") from exc


# ---- running a rule -------------------------------------------------------------------------------
@dataclass(slots=True)
class Draft:
    chat_id: int
    text: str
    kind: str
    reply_to: int | None
    via_account: int | None
    step: str  # unique inside the rule: makes the "event + rule" key of every message unique


@dataclass(slots=True)
class Effects:
    drafts: list[Draft] = field(default_factory=list)
    sql: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    timers: list[dict[str, Any]] = field(default_factory=list)
    log: list[dict[str, Any]] = field(default_factory=list)
    asked: list[tuple[str, float]] = field(default_factory=list)  # dialog key, seconds to wait
    final: tuple[str, str] | None = None

    def execute(self, sql: str, *args: Any) -> None:  # the `Writes` protocol of entity actions
        self.sql.append((sql, args))


class Stop(Exception):  # control flow of the action `stop`
    pass


class Runner:
    """Runs one rule over one input. `reads` is a DB handle for lookup/report; None in pure dry-run."""

    def __init__(self, rule: ReactionRuleCfg, data: dict[str, Any], *, reads: Any, reports: Any,
                 external: bool, notify_chats: dict[str, int | None], simulate: bool = False,
                 ask_timeout_s: float = 600) -> None:
        self.rule = rule
        self.data = data
        self.data.setdefault("r", {})
        self.reads = reads
        self.reports = reports
        self.external = external  # False for history: external actions are skipped (FT-8.8)
        self.notify_chats = notify_chats
        self.simulate = simulate  # dry-run: actions of modules are not executed, only planned
        self.ask_timeout_s = ask_timeout_s
        self.effects = Effects()
        self._step = 0

    def value(self, raw: Any) -> Any:
        if isinstance(raw, str) and ("{{" in raw or "{%" in raw):
            return _ENV.from_string(raw).render(**self.data)
        if isinstance(raw, dict) and len(raw) == 1 and _is_logic(raw):
            return jsonlogic.apply(raw, self.data)
        if isinstance(raw, dict):
            return {k: self.value(v) for k, v in raw.items()}
        if isinstance(raw, list):
            return [self.value(v) for v in raw]
        return raw

    def matches(self) -> bool:
        return self.rule.if_ is None or jsonlogic.truthy(jsonlogic.apply(self.rule.if_, self.data))

    async def run(self) -> Effects:
        try:
            await self._chain(self.rule.then)
        except Stop:
            pass
        return self.effects

    async def _chain(self, actions: list[dict[str, Any]]) -> None:
        for act in actions:
            await self._one(act)

    async def _one(self, act: dict[str, Any]) -> None:
        name = act["action"]
        self._step += 1
        step = f"{self._step}"
        if name == "if":
            branch = act.get("then", []) if jsonlogic.truthy(jsonlogic.apply(act["cond"], self.data)) \
                else act.get("else", [])
            self.effects.log.append({"action": "if", "branch": "then" if branch is act.get("then") else "else"})
            await self._chain(branch)
            return
        if name == "stop":
            self.effects.log.append({"action": "stop"})
            raise Stop()
        p = {k: self.value(v) for k, v in act.items() if k not in ("action", "as")}
        if name in EXTERNAL and not self.external:
            self.effects.log.append({"action": name, "skipped": "history (FT-8.8)"})
            return
        result = await self._do(name, p, step)
        if act.get("as"):
            self.data["r"][act["as"]] = result
        self.effects.log.append({"action": name, **{k: v for k, v in p.items() if k != "text"},
                                 **({"text": str(p["text"])[:200]} if "text" in p else {})})

    async def _do(self, name: str, p: dict[str, Any], step: str) -> Any:
        ev = self.data.get("event", {})
        if name == "render":
            return str(p.get("text") or p.get("template") or "")
        if name in ("reply", "send", "notify", "ask"):
            text = str(p["text"])
            if name == "notify":
                target = p.get("to", "logs")
                chat = self.notify_chats.get(str(target)) if not str(target).lstrip("-").isdigit() else int(target)
                if chat is None:
                    return None
                self.effects.drafts.append(Draft(chat, text, "notify", None, None, step))
                return chat
            if name == "send":
                chat = int(p["chat_id"])
                self.effects.drafts.append(Draft(chat, text, str(p.get("kind", "notify")), None,
                                                 ev.get("account_id") if chat > 0 else None, step))
                return chat
            chat = int(ev["chat_id"])
            kind = str(p.get("kind") or ("command_reply" if self.data.get("command") else "client_reply"))
            self.effects.drafts.append(Draft(chat, text, kind, ev.get("msg_id"),
                                             ev.get("account_id") if chat > 0 else None, step))
            if name == "ask":
                # The dialog state is written in the same transaction as the question (FT-8.9).
                key = f"{chat}:{ev.get('sender_id')}"
                wait = float(p.get("timeout_s") or self.ask_timeout_s)
                entities.set_state(self.effects, "dialog", key, "awaiting", timeout_s=wait, timeout_state="expired")
                self.effects.asked.append((key, wait))
                entities.upsert(self.effects, "dialog", key, attrs={
                    "rule": self.rule.id, "arg": p.get("arg"), "args": self.data.get("args", {}),
                    "account_id": ev.get("account_id"), "question": text})
            return chat
        if name == "schedule":
            self.effects.timers.append({"timer": str(p["timer"]), "in_s": float(p.get("in_s", 0)),
                                        "payload": p.get("payload", {}), "step": step})
            return None
        if name == "set_state":
            entities.set_state(self.effects, str(p["entity_type"]), str(p["key"]), str(p["state"]),
                               timeout_s=p.get("timeout_s"), timeout_state=p.get("timeout_state"))
            return p["state"]
        if name == "upsert":
            entities.upsert(self.effects, str(p["entity_type"]), str(p["key"]), state=p.get("state"),
                            attrs=p.get("attrs") or {})
            return p["key"]
        if name == "link":
            src, dst = (str(p["from_type"]), str(p["from_key"])), (str(p["to_type"]), str(p["to_key"]))
            entities.link(self.effects, src, dst, str(p.get("relation", "")))
            return True
        if name == "lookup":
            if self.reads is None:
                return None
            return await entities.lookup(self.reads, str(p["entity_type"]), str(p["key"]))
        if name == "report":
            if self.reports is None:
                return []
            rows = await entities.report(self.reports, str(p["name"]), **(p.get("params") or {}))
            return [dict(r) for r in rows]
        if name == "call":
            if self.simulate:
                return {"simulated": True}
            fn = MODULE_ACTIONS.get(str(p["name"]))
            if fn is None:
                raise RuleConfigError(f"action `call {p['name']}` is not registered by any module")
            return await fn(p, self.data)
        raise RuleConfigError(f"unknown action {name}")


def log_row(rule: ReactionRuleCfg, trace_id: str | None, config_version: str, data: dict[str, Any],
            effects: Effects, keys: list[str]) -> tuple[Any, ...]:
    """A row of rule_log (FT-8.7): rule and version, event, input, actions, tasks."""
    inp = {k: data.get(k) for k in ("event", "args", "dialog")}
    return (trace_id, "reactions", rule.id, str(rule.version), config_version,
            json.dumps(inp, ensure_ascii=False, default=str), json.dumps(effects.log, ensure_ascii=False, default=str),
            json.dumps(keys))
