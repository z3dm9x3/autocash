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


@dataclass(slots=True)
class Runtime:
    """An immutable, fully validated snapshot of the configuration."""

    version: str
    config: Config
    manifest: CompiledManifest
    sources: dict[int, SourceRuntime]
    unknown_policy: Policy
    private_rules: list[CompiledRule] = field(default_factory=list)

    def source(self, chat_id: int) -> SourceRuntime:
        src = self.sources.get(chat_id)
        if src is not None:
            return src
        if chat_id > 0:
            # A private dialog: one shared policy for all of them, no "new chat" notification.
            return SourceRuntime(SourceCfg(chat_id=chat_id, roles=["dm"]),
                                 self.config.defaults.private_chat_policy, frozenset({"dm"}), self.private_rules)
        service = self.config.service_chats
        if chat_id in (service.logs, service.debug):
            # The system's own log groups are never processed (FT-2.4).
            return SourceRuntime(SourceCfg(chat_id=chat_id), "ignore", frozenset(), [])
        return SourceRuntime(SourceCfg(chat_id=chat_id), self.unknown_policy, frozenset(), [], known=False)

    def sender_role(self, src: SourceRuntime, sender_id: int | None, anonymous: bool) -> str:
        if anonymous or sender_id is None:
            return "chat"
        role = src.cfg.senders.get(sender_id)
        if role:
            return role
        roles = self.config.users.get(sender_id)
        return roles[0] if roles else "user"


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
        self.current: Runtime = build_runtime(read_raw(path))
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
        old, self.current = self.current, new
        if old.version != new.version:
            for listener in self._listeners:
                listener(old, new)
        return True, new.version
