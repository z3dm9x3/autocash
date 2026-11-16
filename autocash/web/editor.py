"""Changes of the configuration made through the admin (FT-13.5, item 1; D-5-2).

The admin edits the same YAML files the operator edits by hand. A change is applied to the
merged configuration, written back into the file that holds the changed top-level key, and
checked by the same `build_runtime` before anything is written: an invalid change is refused
with the error and the files stay as they were. After writing, the running service reloads the
files exactly as `/reload` does, so the version is the same as for a hand edit of the same
content. Every change is recorded with its author and the YAML diff, and committed to git when
the config directory is in a repository.
"""

from __future__ import annotations

import asyncio
import copy
import difflib
import logging
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from autocash.core.config.loader import ConfigError, _deep_merge, build_runtime

if TYPE_CHECKING:
    from autocash.core.app import App

log = logging.getLogger(__name__)

Mutation = Callable[[dict[str, Any]], None]


class EditError(ValueError):
    pass


@dataclass(slots=True)
class EditResult:
    version_before: str
    version_after: str
    files: list[str]
    diff: str
    git_commit: str | None


def dump(data: Any) -> str:
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)


class ConfigEditor:
    def __init__(self, app: App) -> None:
        self.app = app
        self._lock = asyncio.Lock()  # one change at a time: up to ten admins work at once

    # ---- files -------------------------------------------------------------------------------
    @property
    def path(self) -> Path:
        return self.app.store.path

    def files(self) -> list[Path]:
        return sorted(self.path.glob("*.yaml")) if self.path.is_dir() else [self.path]

    @staticmethod
    def _read(file: Path) -> dict[str, Any]:
        data = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise EditError(f"{file.name}: top level must be a mapping")
        return data

    def merged(self) -> dict[str, Any]:
        raw: dict[str, Any] = {}
        for file in self.files():
            raw = _deep_merge(raw, self._read(file))
        return raw

    def owner(self, key: str, contents: dict[Path, dict[str, Any]]) -> Path:
        """The file that holds a top-level key; a new key goes into the last file."""
        holders = [f for f, data in contents.items() if key in data]
        if len(holders) > 1:
            names = ", ".join(f.name for f in holders)
            raise EditError(f"раздел {key} задан в нескольких файлах ({names}): измените его в файлах вручную")
        return holders[0] if holders else list(contents)[-1]

    # ---- a change ------------------------------------------------------------------------------
    async def apply(self, author: int, summary: str, mutate: Mutation, base_version: str | None = None) -> EditResult:
        async with self._lock:
            before = self.app.store.current.version
            if base_version and base_version != before:
                raise EditError(f"конфигурация уже изменилась (действует версия {before}): обновите страницу "
                                "и повторите изменение")
            contents = {f: self._read(f) for f in self.files()}
            merged: dict[str, Any] = {}
            for data in contents.values():
                merged = _deep_merge(merged, data)
            changed = copy.deepcopy(merged)
            mutate(changed)
            keys = [k for k in set(merged) | set(changed) if merged.get(k) != changed.get(k)]
            if not keys:
                raise EditError("изменений нет")
            new_contents = {f: dict(d) for f, d in contents.items()}
            for key in keys:
                owner = self.owner(key, contents)
                if key in changed:
                    new_contents[owner][key] = changed[key]
                else:
                    new_contents[owner].pop(key, None)
            check: dict[str, Any] = {}
            for data in new_contents.values():
                check = _deep_merge(check, data)
            try:
                expected = build_runtime(check).version  # the same check as a hand edit and /reload
            except ConfigError as exc:
                raise EditError(f"изменение отклонено, действует версия {before}: {exc}") from exc
            touched = [f for f in contents if new_contents[f] != contents[f]]
            old_text = {f: f.read_text(encoding="utf-8") for f in touched}
            new_text = {f: dump(new_contents[f]) for f in touched}
            for f in touched:
                _write_atomic(f, new_text[f])
            ok, detail = self.app.reload_config()
            if not ok or self.app.store.current.version != expected:
                for f in touched:
                    _write_atomic(f, old_text[f])
                self.app.reload_config()
                raise EditError(f"изменение не применено: {detail}")
            after = self.app.store.current.version
            diff = "".join(
                "".join(difflib.unified_diff(old_text[f].splitlines(True), new_text[f].splitlines(True),
                                             f"a/{f.name}", f"b/{f.name}"))
                for f in touched)
            commit = None
            if self.app.store.current.config.web.git_commit:
                commit = await asyncio.to_thread(_git_commit, touched, f"admin: {summary} (автор {author})")
            await self.app.db.pool.execute(
                "INSERT INTO config_changes (author, version_before, version_after, summary, files, diff, git_commit) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                author, before, after, summary, [f.name for f in touched], diff, commit)
            return EditResult(before, after, [f.name for f in touched], diff, commit)

    async def set_section(self, author: int, key: str, text: str, base_version: str | None) -> EditResult:
        try:
            value = yaml.safe_load(text) if text.strip() else None
        except yaml.YAMLError as exc:
            raise EditError(f"ошибка YAML: {exc}") from exc

        def mutate(raw: dict[str, Any]) -> None:
            if value is None:
                raw.pop(key, None)
            else:
                raw[key] = value

        return await self.apply(author, f"раздел {key}", mutate, base_version)


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _git_commit(files: list[Path], message: str) -> str | None:
    """Commit the changed files when they are in a git work tree; a failure only goes to the log."""
    cwd = files[0].parent
    try:
        inside = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], cwd=cwd, capture_output=True,
                                text=True, timeout=10)
        if inside.returncode != 0 or inside.stdout.strip() != "true":
            return None
        subprocess.run(["git", "add", "--", *[str(f) for f in files]], cwd=cwd, check=True, capture_output=True,
                       timeout=20)
        subprocess.run(["git", "commit", "-m", message, "--", *[str(f) for f in files]], cwd=cwd, check=True,
                       capture_output=True, timeout=30)
        head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=cwd, capture_output=True, text=True,
                              timeout=10)
        return head.stdout.strip() or None
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("config change is not committed to git: %r", exc)
        return None
