"""Text normalisation helpers: Telegram markup cleanup, hashing, word forms."""

from __future__ import annotations

import hashlib
import re
from functools import lru_cache

import pymorphy3

# These expressions are part of the code (not configuration), so stdlib `re` is fine (A-7).
_LINK = re.compile(r"\[([^\]\n]*)\]\(([^)\s]+)\)")
_PRE = re.compile(r"```(?:\w*\n)?(.*?)```", re.DOTALL)
_PAIRED = [
    re.compile(r"\*\*(.+?)\*\*", re.DOTALL),
    re.compile(r"(?<![\w@])__(.+?)__(?!\w)", re.DOTALL),
    re.compile(r"~~(.+?)~~", re.DOTALL),
    re.compile(r"\|\|(.+?)\|\|", re.DOTALL),
    re.compile(r"`([^`\n]+)`"),
]
_SILENT_SCHEMES = ("tel:", "mailto:", "tg://")


def clean_markup(text: str) -> tuple[str, list[str]]:
    """Strip Telegram markdown; return clean text and URLs hidden behind link text (FT-3.3)."""
    hidden: list[str] = []

    def _link(m: re.Match[str]) -> str:
        label, url = m.group(1), m.group(2)
        if not url.startswith(_SILENT_SCHEMES) and url != label:
            hidden.append(url)
        return label

    text = _LINK.sub(_link, text)
    text = _PRE.sub(lambda m: m.group(1), text)
    for rx in _PAIRED:
        text = rx.sub(lambda m: m.group(1), text)
    return text, hidden


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


_TRANSLIT_MULTI = [
    ("shch", "щ"), ("zh", "ж"), ("kh", "х"), ("ts", "ц"), ("ch", "ч"), ("sh", "ш"),
    ("yu", "ю"), ("ya", "я"), ("yo", "е"), ("ju", "ю"), ("ja", "я"),
]
_TRANSLIT_SINGLE = dict(zip("abvgdezijklmnoprstufhcyw", "абвгдезийклмнопрстуфхцыв", strict=True))
_TRANSLIT_SINGLE.update({"x": "кс", "q": "к"})

_morph: pymorphy3.MorphAnalyzer | None = None


def _analyzer() -> pymorphy3.MorphAnalyzer:
    global _morph
    if _morph is None:
        _morph = pymorphy3.MorphAnalyzer()
    return _morph


def translit(word: str) -> str:
    """Latin transliteration -> Cyrillic (best effort)."""
    out = word
    for lat, cyr in _TRANSLIT_MULTI:
        out = out.replace(lat, cyr)
    return "".join(_TRANSLIT_SINGLE.get(ch, ch) for ch in out)


def fold(text: str) -> str:
    """Case and ё folding used for labels, headers and words."""
    return text.strip().lower().replace("ё", "е")


@lru_cache(maxsize=50_000)
def normalize_word(word: str) -> tuple[str, ...]:
    """All normalised variants of a word: folded, transliterated, lemmatised (FT-3.4)."""
    base = fold(word)
    variants = [base]
    if base.isascii():
        cyr = translit(base)
        if cyr != base:
            variants.append(cyr)
    for v in list(variants):
        if not v.isascii():
            lemma = _analyzer().parse(v)[0].normal_form.replace("ё", "е")
            if lemma not in variants:
                variants.append(lemma)
    return tuple(variants)
