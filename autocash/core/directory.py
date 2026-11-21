"""The directory of users, groups, roles and rights, kept in memory (D-RM-7).

The source of truth is PostgreSQL: tables of the module Role Manager (`rm_*`). The core does not
read them; it asks this object. RM fills it at start and on every change, and the object keeps a
local snapshot file, so the gateway knows which groups to read and the commands know who may run
them while PostgreSQL is down (invariant 1, NFT-18).

Without RM (tests of the core) the directory holds the built-in roles and the users seeded from
the configuration (`users`, deprecated: RM imports them into `rm_users`).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from autocash.core.config.schema import Policy

log = logging.getLogger(__name__)

ANY = "*"
GUEST = "guest"
DEV = "dev"  # may run every command, always: a rule of the code, not a row of a table
OTHER = "other"


@dataclass(frozen=True, slots=True)
class RoleDef:
    code: str
    scope: str  # "user" or "group"
    title: str
    policy: Policy | None = None  # groups: full / meta / presence / ignore


BUILTIN_ROLES: tuple[RoleDef, ...] = (
    RoleDef("guest", "user", "Guest"),
    RoleDef("player", "user", "Player"),
    RoleDef("casher", "user", "Casher"),
    RoleDef("support", "user", "Support"),
    RoleDef("manager", "user", "Manager"),
    RoleDef("admin", "user", "Admin"),
    RoleDef("dev", "user", "Dev"),
    RoleDef("other", "group", "Other", "presence"),
    RoleDef("tech", "group", "Tech", "full"),
    RoleDef("dop", "group", "Dop", "full"),
    RoleDef("ww", "group", "WW", "full"),
)
BUILTIN_PERMISSIONS: dict[str, set[str]] = {"admin": {ANY}}


class Directory:
    def __init__(self) -> None:
        self.roles: dict[str, RoleDef] = {r.code: r for r in BUILTIN_ROLES}
        self.permissions: dict[str, set[str]] = {k: set(v) for k, v in BUILTIN_PERMISSIONS.items()}
        self.users: dict[int, str] = {}  # tg_id -> role
        self.groups: dict[int, str] = {}  # bare group id (no minus, no "100") -> role
        self.version = 0  # grows on every change: caches of group sources are dropped
        self.snapshot_path: Path | None = None
        self.loaded_from_db = False
        # Refused commands (rights.deny): when `logs` was last told about a user, attempts since then.
        self.denied_at: dict[int, float] = {}
        self.denied_count: dict[int, int] = {}

    # ---- reads -------------------------------------------------------------------------------
    def user_role(self, tg_id: int | None) -> str:
        return self.users.get(tg_id, GUEST) if tg_id is not None else GUEST

    def allowed(self, tg_id: int | None) -> set[str]:
        """Commands of a user; ANY in the result allows every command. Rights belong to users only."""
        if tg_id is None:
            return set()
        role = self.user_role(tg_id)
        if role == DEV:
            return {ANY}
        return set(self.permissions.get(role, set()))

    def group_role(self, bare_id: int) -> str | None:
        return self.groups.get(bare_id)

    def policy(self, group_role: str) -> Policy:
        role = self.roles.get(group_role)
        return role.policy if role is not None and role.policy else "full"

    def roles_of(self, scope: str) -> list[str]:
        return [r.code for r in self.roles.values() if r.scope == scope]

    # ---- changes -----------------------------------------------------------------------------
    def seed_users(self, users: dict[int, str]) -> None:
        """Users of the configuration that the directory does not know yet."""
        added = {tg: role for tg, role in users.items() if tg not in self.users}
        if added:
            self.users.update(added)
            self.version += 1

    def replace(self, roles: list[RoleDef], permissions: dict[str, set[str]], users: dict[int, str],
                groups: dict[int, str]) -> None:
        """The whole state read from PostgreSQL."""
        self.roles = {r.code: r for r in roles} or {r.code: r for r in BUILTIN_ROLES}
        self.permissions = permissions
        self.users = users
        self.groups = groups
        self.loaded_from_db = True
        self._changed()

    def set_user(self, tg_id: int, role: str) -> None:
        self.users[tg_id] = role
        self._changed()

    def set_users(self, users: dict[int, str]) -> None:
        """Many users at once (the history of orders adds thousands): one snapshot write."""
        if users:
            self.users.update(users)
            self._changed()

    def set_group(self, bare_id: int, role: str | None) -> None:
        if role is None:
            self.groups.pop(bare_id, None)
        else:
            self.groups[bare_id] = role
        self._changed()

    def _changed(self) -> None:
        self.version += 1
        self.save()

    # ---- the local snapshot ------------------------------------------------------------------
    def save(self) -> None:
        if self.snapshot_path is None:
            return
        data = {
            "roles": [[r.code, r.scope, r.title, r.policy] for r in self.roles.values()],
            "permissions": {k: sorted(v) for k, v in self.permissions.items()},
            "users": {str(k): v for k, v in self.users.items()},
            "groups": {str(k): v for k, v in self.groups.items()},
        }
        tmp = self.snapshot_path.with_name(self.snapshot_path.name + ".tmp")
        try:
            self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self.snapshot_path)
        except OSError:
            log.exception("directory: the snapshot is not saved (%s)", self.snapshot_path)

    def load(self, path: Path) -> bool:
        """Read the snapshot (before PostgreSQL is reachable); False if there is none or it is broken."""
        self.snapshot_path = path
        if not path.exists():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self.roles = {c: RoleDef(c, s, t, cast(Policy | None, p)) for c, s, t, p in data["roles"]}
            self.permissions = {k: set(v) for k, v in data["permissions"].items()}
            self.users = {int(k): v for k, v in data["users"].items()}
            self.groups = {int(k): v for k, v in data["groups"].items()}
        except (OSError, ValueError, KeyError, TypeError):
            log.exception("directory: the snapshot %s is broken, the built-in roles are used", path)
            self.__init__()  # type: ignore[misc]
            self.snapshot_path = path
            return False
        self.version += 1
        return True
