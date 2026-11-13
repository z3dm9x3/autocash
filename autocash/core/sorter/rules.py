"""Sorting rules: assign a message type and named fields (FT-6.1..6.3). Pure functions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import re2

from autocash.core.config.schema import RuleCfg
from autocash.core.extractor.normalize import fold

UNKNOWN = "unknown"


class RuleError(ValueError):
    pass


@dataclass(slots=True)
class CompiledRule:
    cfg: RuleCfg
    headers: frozenset[str]
    header_rx: Any
    text_rx: Any
    words_all: frozenset[str]
    words_any: frozenset[str]
    labels_all: frozenset[str]
    labels_any: frozenset[str]


def compile_rule(cfg: RuleCfg) -> CompiledRule:
    def rx(pattern: str | None, name: str) -> Any:
        if pattern is None:
            return None
        try:
            return re2.compile(pattern)
        except Exception as exc:
            raise RuleError(f"rule {cfg.id}: invalid {name}: {exc}") from exc

    w = cfg.when
    return CompiledRule(
        cfg=cfg,
        headers=frozenset(fold(h) for h in w.header),
        header_rx=rx(w.header_regex, "header_regex"),
        text_rx=rx(w.text_regex, "text_regex"),
        words_all=frozenset(fold(x) for x in w.words_all),
        words_any=frozenset(fold(x) for x in w.words_any),
        labels_all=frozenset(fold(x) for x in w.labels_all),
        labels_any=frozenset(fold(x) for x in w.labels_any),
    )


@dataclass(slots=True)
class MsgView:
    """Indexed view over extractor features, built once per message."""

    text: str
    event: str
    origin: str
    source_roles: frozenset[str]
    sender_role: str
    header: str | None = None
    pairs: dict[str, dict[str, Any]] = field(default_factory=dict)
    words: set[str] = field(default_factory=set)
    markers: set[str] = field(default_factory=set)
    kinds: set[str] = field(default_factory=set)
    command: dict[str, Any] | None = None


def build_view(
    *,
    text: str,
    features: list[dict[str, Any]],
    event: str,
    origin: str,
    source_roles: frozenset[str],
    sender_role: str,
    can_command: bool,
) -> MsgView:
    v = MsgView(text=text, event=event, origin=origin, source_roles=source_roles, sender_role=sender_role)
    for f in features:
        kind = f["k"]
        v.kinds.add(kind)
        if kind == "header":
            v.header = f["v"]
        elif kind == "pair":
            v.pairs.setdefault(fold(f["l"]), f)
        elif kind == "word":
            v.words.add(f["v"])
        elif kind == "marker":
            v.markers.add(f["v"])
        elif kind == "command" and can_command:
            v.command = {"name": f["v"], "args": f.get("args", [])}
    if not can_command:
        v.kinds.discard("command")
    return v


def matches(rule: CompiledRule, v: MsgView) -> bool:
    w = rule.cfg.when
    if w.source_role and not v.source_roles.intersection(w.source_role):
        return False
    if w.sender_role and v.sender_role not in w.sender_role:
        return False
    if w.origin and v.origin not in w.origin:
        return False
    if w.event and v.event not in w.event:
        return False
    if rule.headers and (v.header is None or fold(v.header) not in rule.headers):
        return False
    if rule.header_rx is not None and (v.header is None or not rule.header_rx.search(v.header)):
        return False
    if rule.words_all and not rule.words_all <= v.words:
        return False
    if rule.words_any and not rule.words_any & v.words:
        return False
    if rule.labels_all and not rule.labels_all <= v.pairs.keys():
        return False
    if rule.labels_any and not rule.labels_any & v.pairs.keys():
        return False
    if w.markers_any and not v.markers.intersection(w.markers_any):
        return False
    if w.has_all and not v.kinds.issuperset(w.has_all):
        return False
    if w.command and (v.command is None or v.command["name"] not in w.command):
        return False
    return not (rule.text_rx is not None and not rule.text_rx.search(v.text))


def _field_value(source: str, v: MsgView) -> Any:
    if source == "$header":
        return v.header
    if source == "$command":
        return v.command["name"] if v.command else None
    if source.startswith("$args"):
        if not v.command:
            return None
        args = v.command["args"]
        _, _, idx = source.partition(".")
        if not idx:
            return args
        return args[int(idx)] if idx.isdigit() and int(idx) < len(args) else None
    label, _, sub = source.partition(".")
    pair = v.pairs.get(fold(label))
    if pair is None:
        return None
    if sub:
        return pair.get("sub", {}).get(sub)
    return pair["v"]


@dataclass(slots=True)
class SortResult:
    msg_type: str
    fields: dict[str, Any]
    order_key: str
    priority: int
    text_ttl_days: int | None
    rule_id: str | None
    lane: int  # 0 commands, 1 live, 2 backfill


def classify(rules: list[CompiledRule], v: MsgView, *, chat_id: int, source: str, source_priority: int) -> SortResult:
    lane = 2 if source == "backfill" else (0 if v.command else 1)
    chat_key = f"chat={chat_id}"
    for rule in rules:
        if not rule.cfg.enabled or not matches(rule, v):
            continue
        fields = {name: val for name, src in rule.cfg.fields.items() if (val := _field_value(src, v)) is not None}
        parts = [f"{k}={fields[k]}" for k in rule.cfg.order_key if k in fields]
        key = ";".join(parts) if rule.cfg.order_key and len(parts) == len(rule.cfg.order_key) else chat_key
        return SortResult(rule.cfg.type, fields, key, rule.cfg.priority, rule.cfg.text_ttl_days, rule.cfg.id, lane)
    if v.command:
        fields = {"command": v.command["name"], "args": v.command["args"]}
        return SortResult(f"command.{v.command['name']}", fields, chat_key, source_priority, None, None, lane)
    return SortResult(UNKNOWN, {}, chat_key, source_priority, None, None, lane)
