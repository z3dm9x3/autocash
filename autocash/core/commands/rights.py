"""Rights to run commands (FT-12.4, D-RM-7, D-RM-8).

Rights belong to users only: the user's role (Role Manager, tables rm_users / rm_permissions,
kept in memory by the Directory) gives the commands; the role of the group does not matter.
The role Dev may run every command, always. The check works without PostgreSQL (NFT-18).

A refused command is not answered in the chat (D-RM-8): the refusal goes to the group `logs` and
to the log of the monitor, at most once per user in `DENIAL_WINDOW_S`, with the count of attempts.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from autocash.core.config.loader import Runtime
from autocash.core.directory import ANY

__all__ = ["ANY", "allowed_commands", "can_run", "deny", "user_roles"]

log = logging.getLogger(__name__)

DENIAL_WINDOW_S = 600.0


def user_roles(rt: Runtime, user_id: int | None, chat_id: int) -> set[str]:
    return set() if user_id is None else {rt.directory.user_role(user_id)}


def allowed_commands(rt: Runtime, user_id: int | None, chat_id: int) -> set[str]:
    """Commands this user may run; ANY in the result allows every command."""
    return rt.directory.allowed(user_id)


def can_run(rt: Runtime, user_id: int | None, chat_id: int, command: str) -> bool:
    allowed = allowed_commands(rt, user_id, chat_id)
    return ANY in allowed or command in allowed


def deny(notify: Callable[[str], None], rt: Runtime, user_id: int | None, chat_id: int, command: str) -> None:
    """Report a refused command; the caller ends the task with ACCESS_DENIED, the chat gets nothing.

    The notice to `logs` goes at most once per user in DENIAL_WINDOW_S; attempts between are counted.
    """
    directory = rt.directory
    role = directory.user_role(user_id)
    log.warning("ACCESS_DENIED: user %s (role %s), command /%s, chat %s", user_id, role, command, chat_id)
    key = user_id or 0
    directory.denied_count[key] = directory.denied_count.get(key, 0) + 1
    now = time.monotonic()
    last = directory.denied_at.get(key)
    if last is not None and now - last < DENIAL_WINDOW_S:
        return
    directory.denied_at[key] = now
    count, directory.denied_count[key] = directory.denied_count[key], 0
    more = f"; попыток с прошлого сообщения: {count}" if count > 1 else ""
    notify(f"Отказ в правах: пользователь {user_id} (роль {role}), команда /{command}, чат {chat_id}{more}")
