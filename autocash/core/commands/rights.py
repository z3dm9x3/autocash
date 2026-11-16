"""Roles and the rights matrix (FT-12.1, 12.2, 12.4).

A user's roles are the global ones (`users`) plus the role bound to the source the command
came from (`sources[].senders`). The matrix maps "user role x source role" to the commands
allowed; "*" stands for any source role or any command. Roles come from the configuration
only, so the check works without PostgreSQL (the emergency channel, NFT-18).
"""

from __future__ import annotations

from autocash.core.config.loader import Runtime

ANY = "*"


def user_roles(rt: Runtime, user_id: int | None, chat_id: int) -> set[str]:
    if user_id is None:
        return set()
    roles = set(rt.config.users.get(user_id, []))
    bound = rt.source(chat_id).cfg.senders.get(user_id)
    if bound:
        roles.add(bound)
    return roles


def allowed_commands(rt: Runtime, user_id: int | None, chat_id: int) -> set[str]:
    """Commands this user may run in this chat; ANY in the result allows every command."""
    matrix = rt.config.permissions
    source_roles = set(rt.source(chat_id).roles) | {ANY}
    out: set[str] = set()
    for role in user_roles(rt, user_id, chat_id):
        for source_role, commands in matrix.get(role, {}).items():
            if source_role in source_roles:
                out.update(commands)
    return out


def can_run(rt: Runtime, user_id: int | None, chat_id: int, command: str) -> bool:
    allowed = allowed_commands(rt, user_id, chat_id)
    return ANY in allowed or command in allowed
