"""TS: the triggers of C-ticket - what makes a ticket D (deposit) or WW (withdrawal) (TS_readme.md, D-TS-2).

Explicit words only, in any form:
- D: пополнил/пополнение/попалнил, депозит/дипозит, деп/депнул/задепал, закинул, на игру, чек/квитанция,
  оплатил, перевёл, платёж; a PDF; a forwarded message of the kassa bot about a deposit;
- WW: вывод/вывел/вывести, снял (not "снял видео"), выплата, "отправлена на модерацию", a bare withdrawal code.
Not triggers - they come in deposits and withdrawals alike: "завис", "сутки", "деньги не пришли", "на карту",
"зачисление", "реквизиты" (checked on 3 136 real appeals of Cash Monitor Dev, 2026-11-19).
The rules are configuration (P-2): `modules.ticket_system.config.rules`; these are the defaults.
Expressions run in re2 (A-7) on the normalized text: lower case, "ё" -> "е".
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import re2
from pydantic import BaseModel, Field

D, WW, Q = "D", "WW", "Q"
_LETTER = "а-яa-z0-9"  # what counts as a letter for the borders of a short word (re2 \b is ASCII only)


def _word(body: str) -> str:
    """A whole word: not glued to letters or digits on either side."""
    return f"(?:^|[^{_LETTER}])(?:{body})(?:$|[^{_LETTER}])"


class RuleCfg(BaseModel):
    name: str
    kind: str  # D | WW
    pattern: str | None = None  # re2, on the normalized text
    media: list[str] = Field(default_factory=list)  # media types that fire the rule (e.g. "pdf")
    unless: str | None = None  # re2: the rule does not fire if the same message matches this


DEFAULT_RULES: list[dict[str, Any]] = [
    # ---- deposit -------------------------------------------------------------------------------
    {"name": "d.popoln", "kind": D, "pattern": "п[оа]п[оа]лн[а-я]*"},  # пополнил, пополнение, попалнил
    {"name": "d.deposit", "kind": D, "pattern": "д[еи]п[оа]з[а-я]*",  # депозит, депозита, дипозит, депазит
     "unless": "вносили\\s+депозит"},  # the bot's withdrawal guide: "платформу, через которую вы вносили депозит"
    {"name": "d.dep", "kind": D,  # деп, депа, депнул, задепал - not депутат, департамент
     "pattern": _word("(?:за)?д[еэ]п(?:а|у|ом|е|ы|ов|нул|нула|нули|нуть|ну|нешь|ал|ала|али|ать|аю|аешь)?")},
    {"name": "d.zakinul", "kind": D, "pattern": "зак[иі]н[а-я]*|закид[а-я]*", "unless": "код"},  # закинул
    {"name": "d.na_igru", "kind": D, "pattern": "на\\s+игр(?:у|овой)"},  # (закинул) на игру
    {"name": "d.receipt", "kind": D, "pattern": _word("ч[еэ]к(?:а|у|ом|и|ов|е)?") + "|квитанц[а-я]*"},
    {"name": "d.paid", "kind": D, "pattern": "оплатил[а-я]*|оплачен[а-я]*"},
    {"name": "d.sent", "kind": D, "pattern": "перевел[а-я]*"},
    {"name": "d.payment", "kind": D, "pattern": "платеж[а-я]*"},
    {"name": "d.pdf", "kind": D, "media": ["pdf"]},
    {"name": "d.bot", "kind": D,  # forwarded messages of the kassa bot about a deposit
     "pattern": "переведите\\s+точную\\s+сумму|заявка\\s*#?\\s*[\\d ]+\\s*создана|epos\\s*id|"
                "реквизиты\\s+для\\s+оплаты|метод\\s+пополнения|ожидание\\s+реквизитов"},
    # ---- withdrawal ----------------------------------------------------------------------------
    {"name": "w.vyvod", "kind": WW, "pattern": "вывод[а-я]*|вывел[а-я]*|вывест[а-я]*|вывед[а-я]*"},
    {"name": "w.snyal", "kind": WW, "pattern": "снял[а-я]*|снять|сняти[а-я]*|сниму",
     "unless": "видео|скрин|запис"},  # "снял видео" is the deposit check, not a withdrawal
    {"name": "w.vyplata", "kind": WW, "pattern": "выплат[а-я]*"},
    {"name": "w.bot", "kind": WW, "pattern": "отправлена\\s+на\\s+модерацию"},
]

# A bare withdrawal code: 4 latin letters/digits, with a letter, not one of these words.
CODE_STOPWORDS = frozenset(w.lower() for w in (
    "EPOS", "Cash", "RAMZ", "USER", "Test", "1WIN", "call", "play", "apps", "Ozon", "Fast", "Bank", "Name", "will",
    "card", "have", "with", "made", "Team", "GOLD", "that", "this", "okay", "what", "from", "your", "pay1", "Sber",
    "Tink", "salam"))
_CODE = re.compile(r"[A-Za-z0-9]{4}")


@dataclass(frozen=True, slots=True)
class CompiledRule:
    name: str
    kind: str
    rx: Any | None
    media: frozenset[str]
    unless: Any | None


@dataclass(frozen=True, slots=True)
class Hit:
    rule: str  # its name
    kind: str  # D | WW
    fragment: str  # what fired it: the word of the text, "PDF", the code


def compile_rules(rules: Iterable[RuleCfg | dict[str, Any]]) -> list[CompiledRule]:
    out: list[CompiledRule] = []
    for raw in rules:
        r = raw if isinstance(raw, RuleCfg) else RuleCfg.model_validate(raw)
        if r.kind not in (D, WW):
            raise ValueError(f"TS rule {r.name}: kind must be D or WW, not {r.kind!r}")
        out.append(CompiledRule(r.name, r.kind, re2.compile(r.pattern) if r.pattern else None,
                                frozenset(m.lower() for m in r.media), re2.compile(r.unless) if r.unless else None))
    return out


def normalize(text: str) -> str:
    return (text or "").lower().replace("ё", "е")


def bare_code(text: str) -> str | None:
    """A message that is only a withdrawal code ("tfVQ", "5AKW"), maybe next to an ID ("1 758 918 313 / rywT").

    No Cyrillic in the message (a code comes alone, not inside a sentence); a token of letters only is a code
    when it is mixed-case ("tfVQ") or the whole message ("yzdm").
    """
    if re.search(r"[а-яёА-ЯЁ]", text or ""):
        return None
    words = [w for w in re.split(r"[\s/,;:|]+", (text or "").strip()) if w]
    if not 1 <= len(words) <= 5:
        return None
    for w in words:
        if not _CODE.fullmatch(w) or not re.search(r"[A-Za-z]", w) or w.lower() in CODE_STOPWORDS:
            continue
        mixed = bool(re.search(r"\d", w)) or (bool(re.search(r"[a-z]", w)) and bool(re.search(r"[A-Z]", w)))
        if mixed or len(words) == 1:
            return w
    return None


def hits(text: str, media_type: str | None, rules: list[CompiledRule]) -> list[Hit]:
    """Everything one message fires, in the order of the rules; a bare withdrawal code is the rule w.code."""
    norm = normalize(text)
    media = (media_type or "").lower()
    out: list[Hit] = []
    for rule in rules:
        if rule.unless is not None and rule.unless.search(norm):
            continue
        if media and media in rule.media:
            out.append(Hit(rule.name, rule.kind, media.upper()))
            continue
        found = rule.rx.search(norm) if rule.rx is not None and norm else None
        if found:
            out.append(Hit(rule.name, rule.kind, re.sub(rf"^[^{_LETTER}]+|[^{_LETTER}]+$", "", found.group(0))))
    code = bare_code(text)
    if code is not None:
        out.append(Hit("w.code", WW, code))
    return out


def decide(message_hits: list[Hit]) -> tuple[str, str] | None:
    """The type a message gives a ticket: (ticket, reason), or None when it fires nothing.

    Only D rules -> D; only WW -> WW; both kinds in one message -> Q (mixed): the player has to be asked.
    """
    kinds = {h.kind for h in message_hits}
    if not kinds:
        return None
    if kinds == {D}:
        return D, "triggers"
    if kinds == {WW}:
        return WW, "triggers"
    return Q, "mixed"
