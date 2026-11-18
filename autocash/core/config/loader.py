"""Loading, validation, versioning and hot reload of the configuration (FT-13.1..13.4)."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from autocash.core.config.schema import Config, Policy, RuleCfg, SourceCfg
from autocash.core.directory import OTHER, Directory
from autocash.core.extractor.extractor import CompiledManifest, ManifestError, compile_manifest
from autocash.core.sorter.rules import CompiledRule, RuleError, compile_rule

log = logging.getLogger(__name__)


class ConfigError(ValueError):
    pass


@dataclass(slots=True)
class SourceRuntime:
    cfg: SourceCfg
    policy: Policy
    roles: frozenset[str]
    rules: list[CompiledRule]
    known: bool = True


# Marked ids of supergroups and channels are -100<id> (Telethon): -1003845234572634 -> 3845234572634.
_MARK_LEN = 13  # "100" + at least 10 digits; ids of basic groups are shorter


def bare_group_id(chat_id: int) -> int:
    """A group id as it is written in `groups`: without the minus and without the "100" of supergroups."""
    digits = str(abs(chat_id))
    if digits.startswith("100") and len(digits) >= _MARK_LEN:
        return int(digits[3:])
    return int(digits)


@dataclass(slots=True)
class Runtime:
    """An immutable, fully validated snapshot of the configuration."""

    version: str
    config: Config
    manifest: CompiledManifest
    sources: dict[int, SourceRuntime]
    unknown_policy: Policy
    private_rules: list[CompiledRule] = field(default_factory=list)
    # Users, groups, roles and rights (D-RM-7): shared by every Runtime of one ConfigStore.
    directory: Directory = field(default_factory=Directory)
    _groups: dict[int, SourceRuntime] = field(default_factory=dict)
    _groups_version: int = -1

    def group_role(self, chat_id: int) -> str | None:
        """The role given to a group by Role Manager, None when it has none of its own."""
        return self.directory.group_role(bare_group_id(chat_id)) if chat_id < 0 else None

    def source(self, chat_id: int) -> SourceRuntime:
        if chat_id < 0:
            if self._groups_version != self.directory.version:
                self._groups.clear()  # a role changed: build the sources of groups again
                self._groups_version = self.directory.version
            cached = self._groups.get(chat_id)
            if cached is None:
                cached = self._groups[chat_id] = self._group_source(chat_id)
            return cached
        src = self.sources.get(chat_id)
        if src is not None:
            return src
        return SourceRuntime(SourceCfg(chat_id=chat_id, roles=["dm"]),
                             self.config.defaults.private_chat_policy, frozenset({"dm"}), self.private_rules)

    def _group_source(self, chat_id: int) -> SourceRuntime:
        service = self.config.service_chats
        src = self.sources.get(chat_id)
        if chat_id in (service.logs, service.debug):
            # The system's own log groups are never processed (FT-2.4), whatever role they are given.
            return src or SourceRuntime(SourceCfg(chat_id=chat_id), "ignore", frozenset(), [])
        role = self.group_role(chat_id)
        directory = self.directory
        if src is not None:
            if role is None:
                return src
            # The role given by Role Manager wins over the roles written in `sources`.
            return SourceRuntime(src.cfg, directory.policy(role), frozenset({role}), src.rules)
        if role is not None:
            return SourceRuntime(SourceCfg(chat_id=chat_id, roles=[role]), directory.policy(role),
                                 frozenset({role}), [])
        # A group without a role: "other" - known to exist, not read (D-RM-2).
        return SourceRuntime(SourceCfg(chat_id=chat_id), directory.policy(OTHER), frozenset({OTHER}),
                             [], known=False)

    def sender_role(self, src: SourceRuntime, sender_id: int | None, anonymous: bool) -> str:
        if anonymous or sender_id is None:
            return "chat"
        role = src.cfg.senders.get(sender_id)
        if role:
            return role
        role = self.directory.users.get(sender_id)
        return role if role is not None else "user"


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def read_raw(path: Path) -> dict[str, Any]:
    """A single YAML file or a directory of *.yaml merged in name order."""
    files = sorted(path.glob("*.yaml")) if path.is_dir() else [path]
    if not files:
        raise ConfigError(f"no config files in {path}")
    raw: dict[str, Any] = {}
    for file in files:
        try:
            data = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ConfigError(f"{file.name}: {exc}") from exc
        if not isinstance(data, dict):
            raise ConfigError(f"{file.name}: top level must be a mapping")
        raw = _deep_merge(raw, data)
    return raw


def build_runtime(raw: dict[str, Any]) -> Runtime:
    try:
        cfg = Config.model_validate(raw)
        manifest = compile_manifest(cfg.manifest)
        sources: dict[int, SourceRuntime] = {}
        for src in cfg.sources:
            sources[src.chat_id] = SourceRuntime(src, _policy(cfg, src), frozenset(src.roles), _rules(cfg, src))
        private_rules = _rules(cfg, SourceCfg(chat_id=1, template=cfg.defaults.private_template))
        # Reaction rules are checked before they apply: conditions, templates, actions (FT-13.2).
        from autocash.core.reactions.engine import RuleConfigError, validate_rule

        for rule in cfg.reactions.rules:
            try:
                validate_rule(rule)
            except RuleConfigError as exc:
                raise RuleError(str(exc)) from exc
    except (ValidationError, ManifestError, RuleError) as exc:
        raise ConfigError(str(exc)) from exc
    canonical = json.dumps(cfg.model_dump(mode="json"), sort_keys=True, ensure_ascii=False)
    version = hashlib.sha256(canonical.encode()).hexdigest()[:12]
    return Runtime(version, cfg, manifest, sources, cfg.defaults.unknown_chat_policy, private_rules)


def _policy(cfg: Config, src: SourceCfg) -> Policy:
    if src.policy is not None:
        return src.policy
    for role in src.roles:
        if role in cfg.defaults.policy_by_role:
            return cfg.defaults.policy_by_role[role]
    if cfg.service_chats.logs == src.chat_id or cfg.service_chats.debug == src.chat_id:
        return "ignore"
    return "full"


def _rules(cfg: Config, src: SourceCfg) -> list[CompiledRule]:
    rules: list[RuleCfg] = []
    if src.template is not None:
        for rule in cfg.templates[src.template].rules:
            if rule.id in src.disabled_rules:
                continue
            override = src.rule_overrides.get(rule.id)
            if override:
                rule = RuleCfg.model_validate(_deep_merge(rule.model_dump(), override))
            rules.append(rule)
        template_ids = {r.id for r in cfg.templates[src.template].rules}
        unknown = (set(src.disabled_rules) | set(src.rule_overrides)) - template_ids
        if unknown:
            raise RuleError(f"source {src.chat_id}: overrides for unknown rules {sorted(unknown)}")
    rules.extend(src.extra_rules)
    return [compile_rule(r) for r in rules]


class ConfigStore:
    """Holds the current Runtime. A failed reload keeps the previous version (FT-13.2)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.directory = Directory()  # filled by Role Manager, survives reloads of the configuration
        self.current: Runtime = build_runtime(read_raw(path))
        self.current.directory = self.directory
        self._listeners: list[Callable[[Runtime, Runtime], None]] = []
        self.on_error: Callable[[str], None] | None = None

    def subscribe(self, listener: Callable[[Runtime, Runtime], None]) -> None:
        self._listeners.append(listener)

    def reload(self) -> tuple[bool, str]:
        try:
            new = build_runtime(read_raw(self.path))
        except ConfigError as exc:
            message = f"Конфигурация не применена, действует версия {self.current.version}: {exc}"
            log.error(message)
            if self.on_error:
                self.on_error(message)
            return False, str(exc)
        new.directory = self.directory
        old, self.current = self.current, new
        if old.version != new.version:
            for listener in self._listeners:
                listener(old, new)
        return True, new.version
