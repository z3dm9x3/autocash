"""Extractor: describes WHAT is in the text (forms). Meaning is assigned by the sorter (P-3)."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

import re2

from autocash.core.config.schema import ManifestCfg
from autocash.core.extractor.normalize import clean_markup, fold, normalize_word, text_hash

Feature = dict[str, Any]

_WORD = re.compile(r"[^\W\d_]+")
_DIGITS = re.compile(r"\d+")
_COMMAND = re.compile(r"^/([A-Za-z][A-Za-z0-9_]{0,31})(?:@\w+)?(?:\s+(.*))?$", re.DOTALL)


class ManifestError(ValueError):
    pass


def _compile(name: str, pattern: str) -> Any:
    # re2 guarantees linear time, so configured expressions cannot stall the loop (A-7).
    try:
        return re2.compile(pattern)
    except Exception as exc:  # re2.error has no stable public base class
        raise ManifestError(f"manifest: invalid regex {name!r}: {exc}") from exc


@dataclass(slots=True)
class CompiledManifest:
    version: str
    cfg: ManifestCfg
    amount: Any
    amount_int: Any
    phone: Any
    card: Any
    username: Any
    url: Any
    header: Any
    pair: Any
    patterns: list[tuple[str, Any]]
    words: dict[str, list[tuple[str, str]]]
    subfields: dict[str, Any]
    bare_commands: frozenset[str]


def compile_manifest(cfg: ManifestCfg) -> CompiledManifest:
    """Validate and compile a manifest; raises ManifestError (FT-3.8)."""
    version = hashlib.sha256(
        json.dumps(cfg.model_dump(), sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()[:12]
    words: dict[str, list[tuple[str, str]]] = {}
    for dict_name, entries in cfg.dictionaries.items():
        for entry in entries:
            if not _WORD.fullmatch(entry):
                raise ManifestError(f"dictionary {dict_name!r}: {entry!r} is not a single word")
            for variant in normalize_word(entry):
                words.setdefault(variant, []).append((dict_name, fold(entry)))
    subfields = {}
    for label, sub in cfg.subfields.items():
        rx = _compile(f"subfields.{label}", sub.regex)
        if not rx.groupindex:
            raise ManifestError(f"subfields.{label}: regex needs named groups")
        subfields[fold(label)] = rx
    return CompiledManifest(
        version=version,
        cfg=cfg,
        amount=_compile("amount_regex", cfg.amount_regex),
        amount_int=_compile("amount_int_regex", cfg.amount_int_regex),
        phone=_compile("phone_regex", cfg.phone_regex),
        card=_compile("card_regex", cfg.card_regex),
        username=_compile("username_regex", cfg.username_regex),
        url=_compile("url_regex", cfg.url_regex),
        header=_compile("header_regex", cfg.header_regex),
        pair=_compile("pair_regex", cfg.pair_regex),
        patterns=[(p.name, _compile(f"patterns.{p.name}", p.regex)) for p in cfg.patterns],
        words=words,
        subfields=subfields,
        bare_commands=frozenset(c.lower() for c in cfg.commands.bare),
    )


@dataclass(slots=True)
class Extraction:
    text: str
    text_hash: str
    features: list[Feature] = field(default_factory=list)
    command: dict[str, Any] | None = None


def _digit_bounded(text: str, start: int, end: int) -> bool:
    """re2 has no lookarounds: reject matches cut out of a longer digit run."""
    return not (start > 0 and text[start - 1].isdigit()) and not (end < len(text) and text[end].isdigit())


def parse_command(text: str, bare: frozenset[str]) -> dict[str, Any] | None:
    stripped = text.strip()
    m = _COMMAND.match(stripped)
    if m:
        raw = (m.group(2) or "").strip()
        return {"name": m.group(1).lower(), "args": raw.split(), "raw_args": raw, "slash": True}
    head, _, rest = stripped.partition(" ")
    if head.lower() in bare and "\n" not in stripped:
        return {"name": head.lower(), "args": rest.split(), "raw_args": rest.strip(), "slash": False}
    return None


def extract(raw_text: str, m: CompiledManifest) -> Extraction:
    text, hidden_urls = clean_markup(raw_text)
    feats: list[Feature] = []
    taken: list[tuple[int, int]] = []

    def free(start: int, end: int) -> bool:
        return all(end <= s or start >= e for s, e in taken)

    def add(kind: str, value: str, start: int, end: int, **extra: Any) -> None:
        feats.append({"k": kind, "v": value, "s": start, "e": end, **extra})

    header = m.header.search(text)
    if header:
        add("header", header.group(1).strip(), header.start(), header.end())

    lines: list[tuple[int, int, str]] = []
    for pm in m.pair.finditer(text):
        label, value = pm.group(1).strip(), pm.group(2)
        if not label or value.startswith("//"):
            continue
        pair: Feature = {"k": "pair", "l": label, "v": value, "s": pm.start(), "e": pm.end()}
        sub_rx = m.subfields.get(fold(label))
        if sub_rx is not None:
            sm = sub_rx.search(value)
            if sm:
                pair["sub"] = {k: v.strip() for k, v in sm.groupdict().items() if v is not None}
        feats.append(pair)
        lines.append((pm.start(), pm.end(), label))

    for kind, rx in (("card", m.card), ("phone", m.phone), ("amount", m.amount), ("amount_int", m.amount_int)):
        for fm in rx.finditer(text):
            s, e = fm.start(), fm.end()
            if _digit_bounded(text, s, e) and free(s, e):
                if kind == "amount_int" and e < len(text) - 1 and text[e] in ".," and text[e + 1].isdigit():
                    continue
                taken.append((s, e))
                add(kind, fm.group(0), s, e)
    for dm in _DIGITS.finditer(text):
        s, e = dm.start(), dm.end()
        if m.cfg.number_min_len <= e - s <= m.cfg.number_max_len and free(s, e):
            add("number", dm.group(0), s, e, n=e - s)
    for kind, rx in (("username", m.username), ("url", m.url), *m.patterns):
        for fm in rx.finditer(text):
            add(kind, fm.group(0), fm.start(), fm.end())
    for url in hidden_urls:
        add("url", url, -1, -1)
    for marker in m.cfg.markers:
        pos = text.find(marker)
        while pos != -1:
            add("marker", marker, pos, pos + len(marker))
            pos = text.find(marker, pos + len(marker))
    if m.words:
        for wm in _WORD.finditer(text):
            for variant in normalize_word(wm.group(0)):
                hits = m.words.get(variant)
                if hits:
                    for dict_name, canonical in hits:
                        add("word", canonical, wm.start(), wm.end(), d=dict_name)
                    break

    command = parse_command(text, m.bare_commands)
    if command:
        add("command", command["name"], 0, len(text), args=command["args"])

    for f in feats:
        if f["k"] != "pair" and "l" not in f:
            pos = f["s"]
            f["l"] = next((label for s, e, label in lines if s <= pos < e), None)
    feats.sort(key=lambda f: (f["s"], f["e"]))
    return Extraction(text=text, text_hash=text_hash(text), features=feats, command=command)
