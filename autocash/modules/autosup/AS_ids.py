"""AS: the player's ID (MelID) in a message (AS_readme.md, D-AS-2).

Rules, from 1 211 real answers to "send your ID" and 6 410 real MelIDs (Cash Monitor Dev, 2026-11-18):
- an ID is 8-12 digits after removing the spaces: "1234567890", "123 456 789", "1 234 567 890";
  amounts are at most 6-7 digits (the largest deposit is 500 005), so they never pass;
- not an ID: a phone (+7..., 11 digits starting with 7 or 8, 10 digits starting with 9 - real
  10-digit MelIDs start with 1), a number glued to letters (file names of receipts), a number
  after "Заявка #" or "EPOS ID", a number next to "руб", "₽", "тыс", "сумма", "реквизит", "карт";
- prefixes "ID", "Id", "I'd", "айди", "ид" and labels "Номер аккаунта", "номер счёта" are fine;
- several different IDs in one message: a labelled one wins; otherwise nothing is guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# A run of digits, or digits grouped by thousands with spaces; not glued to letters, "+", "#", "@".
_NUMBER = re.compile(r"(?<![\w+#@$])(\d{1,3}(?:[  ]\d{3})+|\d+)(?![\w])")
_LABEL = re.compile(r"(\bi'?d\b|\bид\b|айди|аккаунт|сч[её]т|melbet|мелбет)", re.IGNORECASE)
_NOT_ID_BEFORE = re.compile(r"(заявк\w*\s*#|epos\s*id|реквизит|карт\w*|телефон\w*|сумм\w*|номер карты)\s*:?\s*$",
                            re.IGNORECASE)
_NOT_ID_AFTER = re.compile(r"^\s*(руб|₽|р\.|р\b|тыс|к\b|rub)", re.IGNORECASE)
MIN_LEN, MAX_LEN = 8, 12


@dataclass(frozen=True, slots=True)
class FoundId:
    mel_id: str  # digits only
    spaced: bool  # the user wrote it with spaces: Autosup writes it back normalized


def _is_phone(digits: str) -> bool:
    return (len(digits) == 11 and digits[0] in "78") or (len(digits) == 10 and digits[0] == "9")


def candidates(text: str) -> list[tuple[FoundId, bool]]:
    """Every ID-like number of the text, with "is it labelled" (ID, айди, Номер аккаунта ...)."""
    out: list[tuple[FoundId, bool]] = []
    for line in (text or "").splitlines():
        for m in _NUMBER.finditer(line):
            raw = m.group(1)
            digits = re.sub(r"\D", "", raw)
            if not MIN_LEN <= len(digits) <= MAX_LEN or _is_phone(digits):
                continue
            before, after = line[:m.start()], line[m.end():]
            if _NOT_ID_BEFORE.search(before[-30:]) or _NOT_ID_AFTER.search(after):
                continue
            out.append((FoundId(digits, raw != digits), bool(_LABEL.search(before[-40:]))))
    return out


def find_id(text: str) -> FoundId | None:
    """The one ID of the message, or None (no ID, or several different ones and none labelled)."""
    found = candidates(text)
    distinct = {f.mel_id for f, _ in found}
    if len(distinct) == 1:
        return found[0][0]
    labelled = {f.mel_id: f for f, lab in found if lab}
    if len(labelled) == 1:
        return next(iter(labelled.values()))
    return None
