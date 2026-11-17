"""Config examples pass the schema (Definition of Done); the core depends on the client interface."""

from __future__ import annotations

import re
from pathlib import Path

from autocash.cli.run import build_modules
from autocash.core.config.loader import build_runtime, read_raw
from autocash.core.config.schema import Config

ROOT = Path(__file__).resolve().parents[2]


def test_config_example_passes_the_schema() -> None:
    raw = read_raw(ROOT / "config.example")
    runtime = build_runtime(raw)
    assert runtime.source(-1001000000001).policy == "full" and runtime.source(-1001000000001).rules
    assert runtime.source(-1009000000001).policy == "ignore"  # the system's own log group (FT-2.4)
    # A group without a role is "other": known to exist, not read (D-RM-2).
    assert runtime.source(-123).policy == "presence" and not runtime.source(-123).known
    assert runtime.source(-123).roles == frozenset({"other"})
    # The example spells out the defaults, so it must not drift away from the schema.
    explicit = Config.model_validate(raw)
    defaults = Config()
    for section in ("buffer", "uploader", "writer", "queue", "output", "retention", "sorter", "coverage", "poll"):
        assert getattr(explicit, section) == getattr(defaults, section), section
    assert explicit.pool.model_copy(update={"sessions_dir": None}) == defaults.pool
    assert explicit.stage_norms_s == defaults.stage_norms_s


def test_demo_module_runs_only_when_named_in_the_config() -> None:
    assert build_modules({}) == []
    assert [m.name for m in build_modules({"demo": object()})] == ["demo"]


def test_only_the_adapter_imports_telethon() -> None:
    """Rule 2: core code depends on the client abstraction, not on Telethon."""
    offenders = []
    for path in (ROOT / "autocash" / "core").rglob("*.py"):
        if path.name == "telethon_client.py":
            continue
        if re.search(r"^\s*(from|import)\s+telethon\b", path.read_text(encoding="utf-8"), re.MULTILINE):
            offenders.append(path.relative_to(ROOT).as_posix())
    for path in (ROOT / "autocash" / "modules").rglob("*.py"):
        if re.search(r"^\s*(from|import)\s+telethon\b", path.read_text(encoding="utf-8"), re.MULTILINE):
            offenders.append(path.relative_to(ROOT).as_posix())
    assert offenders == []


def test_core_knows_no_domain_entities() -> None:
    """P-1: not a single `if deposit` in the core."""
    offenders = []
    for path in (ROOT / "autocash" / "core").rglob("*.py"):
        if re.search(r"deposit|депозит|заявк|промокод|тикет", path.read_text(encoding="utf-8"), re.IGNORECASE):
            offenders.append(path.relative_to(ROOT).as_posix())
    assert offenders == []
