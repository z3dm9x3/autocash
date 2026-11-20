"""TS: what C-ticket looks for in a player's message (TS_readme.md, knowledge/tickets/00_c_ticket.md).

Triggers - explicit words in any form:
- D: пополнил/пополнение/попалнил, депозит/дипозит, деп/депнул/задепал, закинул, на игру, чек/квитанция,
  оплатил, перевёл, платёж; a PDF; a forwarded message of the kassa bot about a deposit;
- WW: вывод/вывел/вывести, выплата, выигрыш, карта (customer: «однозначно выводы»), снял (not "снял видео",
  not "с меня сняли"), forwarded messages of the withdrawal bot.
Not triggers - they come in deposits and withdrawals alike: "завис", "сутки", "деньги не пришли", "зачисление",
"реквизиты" (checked on 3 136 real appeals of Cash Monitor Dev, 2026-11-19).

Withdrawal code candidates (D-TS-5): a whole token of exactly 4 latin letters/digits with at least one letter,
without 3 digits in a row, not an excluded word (EPOS, Read, Cash ...). A candidate is only a candidate: TS
checks it against the base of withdrawal codes (TS_codes); only a code found there makes the ticket WW.

The rules and exclusions are configuration (P-2): `modules.ticket_system.config.rules` / `.code_exclusions`;
these are the defaults. Expressions run in re2 (A-7) on the normalized text: lower case, "ё" -> "е".
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


_DEPOSIT_BOT_MODERATION = "время\\s+модерации\\s+может\\s+составлять"  # the deposit bot; the withdrawal one is short

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
                "реквизиты\\s+для\\s+оплаты|метод\\s+пополнения|ожидание\\s+реквизитов|" + _DEPOSIT_BOT_MODERATION},
    # ---- withdrawal ----------------------------------------------------------------------------
    {"name": "w.vyvod", "kind": WW, "pattern": "вывод[а-я]*|вывел[а-я]*|вывест[а-я]*|вывед[а-я]*"},
    {"name": "w.vyplata", "kind": WW, "pattern": "выплат[а-я]*"},
    {"name": "w.vyigrysh", "kind": WW, "pattern": "выигр[ыи]ш[а-я]*"},  # выигрыш, выигрыша
    {"name": "w.karta", "kind": WW,  # карта, карту, карты, картой, карточка - not картинка
     "pattern": _word("карт(?:а|у|ы|е|ой|ою|ам|ами|ах|очк[а-я]*)?")},
    {"name": "w.snyal", "kind": WW, "pattern": "снял[а-я]*|снять|сняти[а-я]*|сниму",
     "unless": "видео|скрин|запис|с\\s+меня\\s+сняли"},  # "снял видео" is the deposit check; "с меня сняли" a debit
    {"name": "w.bot", "kind": WW,  # forwarded messages of the withdrawal bot
     "pattern": "отправлена\\s+на\\s+модерацию|название\\s+(?:вашего\\s+)?банка", "unless": _DEPOSIT_BOT_MODERATION},
]

# 4-character latin words that are not withdrawal codes (customer: explicit exclusions; compared case-insensitively).
DEFAULT_CODE_EXCLUSIONS: list[str] = [
    "EPOS", "Read", "Cash", "Fast", "RAMZ", "USER", "Test", "1WIN", "call", "play", "apps", "Ozon", "Bank", "Name",
    "will", "card", "have", "with", "made", "Team", "GOLD", "that", "this", "okay", "what", "from", "your", "Sber",
    "Tink", "RWMG", "help", "info", "link", "mail", "html", "http", "free", "good", "nice", "year", "time",
    "amma", "edin", "etmi", "niye", "edir", "mene", "pulu", "saat", "kimi", "geri", "oldu", "edib", "eden",
]
# A whole token: not glued to letters, digits, "_", "@", "#", "/", "." (usernames, links, file names).
_CANDIDATE = re.compile(r"(?<![A-Za-z0-9_@#/.\-])([A-Za-z0-9]{4})(?![A-Za-z0-9_@/\-]|\.[A-Za-z0-9])")


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


def code_candidates(text: str, exclusions: Iterable[str] = DEFAULT_CODE_EXCLUSIONS) -> list[str]:
    """Tokens of the message that may be withdrawal codes, as written (case matters), in order, without repeats.

    Exactly 4 latin letters/digits, at least one letter, no 3 digits in a row ("998p" is not a code),
    not an excluded word. Letters only are fine ("yzdm").
    """
    excluded = {w.lower() for w in exclusions}
    out: list[str] = []
    for token in _CANDIDATE.findall(text or ""):
        if (re.search(r"[A-Za-z]", token) and not re.search(r"\d{3}", token)
                and token.lower() not in excluded and token not in out):
            out.append(token)
    return out


def hits(text: str, media_type: str | None, rules: list[CompiledRule]) -> list[Hit]:
    """The triggers one message fires, in the order of the rules."""
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
    return out


def decide(message_hits: list[Hit]) -> tuple[str, str] | None:
    """The type a message gives a ticket: (ticket, reason), or None when it fires nothing.

    Only D triggers -> D; only WW -> WW; signs of both in one message -> Q (mixed), definitely.
    """
    kinds = {h.kind for h in message_hits}
    if not kinds:
        return None
    if kinds == {D}:
        return D, "triggers"
    if kinds == {WW}:
        return WW, "triggers"
    return Q, "mixed"
