"""RM: group ids as people type them.

Telegram writes a supergroup as -100<id> and a basic group as -<id>. RM accepts any of
3845234572634, 1003845234572634, -3845234572634, -1003845234572634 and stores the id without the
minus and without "100" (D-RM-3). A basic group and a supergroup with the same number at once is
taken as impossible (decision of the customer, 2026-11-17).
"""

from __future__ import annotations

import re

from autocash.core.config.loader import bare_group_id

_ID = re.compile(r"^-?\d{1,20}$")


class RMIdError(ValueError):
    pass


def candidates(text: str) -> set[int]:
    """The bare ids a typed group id may mean: the number itself and the number without "100"."""
    value = text.strip()
    if not _ID.fullmatch(value):
        raise RMIdError(f"ID группы — число, например 3845234572634 или -1003845234572634, а не {text!r}")
    digits = value.lstrip("-").lstrip("0") or "0"
    out = {int(digits)}
    if digits.startswith("100") and len(digits) > 3:
        out.add(int(digits[3:]))
    out.discard(0)
    if not out:
        raise RMIdError("ID группы не может быть нулём")
    return out


def resolve(text: str, known: set[int]) -> int:
    """The bare id of a known group (`known`: bare ids of groups the pool knows)."""
    found = candidates(text) & known
    if not found:
        raise RMIdError(f"группа {text.strip()} не найдена: аккаунты пула в ней не состоят и роли у неё нет")
    if len(found) > 1:
        options = " или ".join(str(i) for i in sorted(found))
        raise RMIdError(f"ID {text.strip()} подходит к двум группам ({options}): укажите точнее")
    return found.pop()


def bare(chat_id: int) -> int:
    return bare_group_id(chat_id)
