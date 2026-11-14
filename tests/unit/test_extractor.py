"""Criterion 8 of stage 1a: markup cleanup and every form of feature from the TZ (FT-3.3..3.6).

Only the examples given in the TZ are used here; real message fixtures live in
tests/fixtures/messages/ (NFT-21).
"""

from __future__ import annotations

from typing import Any

import pytest

from autocash.core.config.schema import ManifestCfg
from autocash.core.extractor.extractor import CompiledManifest, ManifestError, compile_manifest, extract
from autocash.core.extractor.normalize import clean_markup

MANIFEST: dict[str, Any] = {
    "dictionaries": {"deposit": ["депозит"], "tree": ["ёлка"], "scam": ["скам"]},
    "subfields": {
        "Пользователь": {"regex": r"^(?P<name>\S+)\s+ID:\s*(?P<id>\d+)$"},
        "Успешных депозитов": {"regex": r"^(?P<marker>\S+)\s+(?P<count>\d+)\s+\(всего:\s*(?P<total>\d+)\)$"},
        "Метод оплаты": {"regex": r"^(?P<method>.*?)\s+(?P<min>\d[\d ]*\d)\s*-\s*(?P<max>\d[\d ]*\d)$"},
    },
    "commands": {"bare": ["scam", "unban"]},
}


@pytest.fixture(scope="module")
def manifest() -> CompiledManifest:
    return compile_manifest(ManifestCfg.model_validate(MANIFEST))


def feats(text: str, manifest: CompiledManifest, kind: str) -> list[dict[str, Any]]:
    return [f for f in extract(text, manifest).features if f["k"] == kind]


def values(text: str, manifest: CompiledManifest, kind: str) -> list[str]:
    return [f["v"] for f in feats(text, manifest, kind)]


# ---- FT-3.3: markup -------------------------------------------------------------------------
def test_tel_link_is_replaced_by_its_label(manifest: CompiledManifest) -> None:
    assert clean_markup("[1481598133](tel:1481598133)") == ("1481598133", [])
    ex = extract("Номер счёта: [1481598133](tel:1481598133)", manifest)
    assert ex.text == "Номер счёта: 1481598133"
    assert [f["v"] for f in ex.features if f["k"] == "pair"] == ["1481598133"]


def test_bold_code_and_hidden_links(manifest: CompiledManifest) -> None:
    text, hidden = clean_markup("**Сумма**: `3500.00` [чек](https://example.com/c/1) ~~старое~~ ||секрет||")
    assert text == "Сумма: 3500.00 чек старое секрет"
    assert hidden == ["https://example.com/c/1"]
    assert "https://example.com/c/1" in values("[чек](https://example.com/c/1)", manifest, "url")


def test_header_brackets_are_not_taken_for_a_link(manifest: CompiledManifest) -> None:
    assert extract("[ДЕПОЗИТ ТРЕУГОЛЬНИК]\nDeposit ID: 5", manifest).text == "[ДЕПОЗИТ ТРЕУГОЛЬНИК]\nDeposit ID: 5"


# ---- FT-3.4: forms --------------------------------------------------------------------------
@pytest.mark.parametrize("amount", ["5 001.34", "3500.00", "4 001,36"])
def test_amount_formats(manifest: CompiledManifest, amount: str) -> None:
    text = f"Сумма: {amount} руб"
    (f,) = feats(text, manifest, "amount")
    assert f["v"] == amount and text[f["s"]:f["e"]] == amount and f["l"] == "Сумма"
    assert values(text, manifest, "number") == []  # digits of the amount are not reported again


def test_integer_amount_with_group_separators(manifest: CompiledManifest) -> None:
    assert values("лимит 500 000 в сутки", manifest, "amount_int") == ["500 000"]


def test_numbers_by_length(manifest: CompiledManifest) -> None:
    fs = feats("ID 7287295969, счёт 1481598133, код 123", manifest, "number")
    assert [(f["v"], f["n"]) for f in fs] == [("7287295969", 10), ("1481598133", 10)]  # 123 is below min length


def test_phone_card_username_url(manifest: CompiledManifest) -> None:
    text = "тел +79161234567 карта 2200 1234 5678 9012 пиши @some_user или https://t.me/some_chat"
    assert values(text, manifest, "phone") == ["+79161234567"]
    assert values(text, manifest, "card") == ["2200 1234 5678 9012"]
    assert values(text, manifest, "username") == ["@some_user"]
    assert values(text, manifest, "url") == ["https://t.me/some_chat"]
    assert values(text, manifest, "number") == []  # phone and card digits are not repeated as numbers


def test_header_in_square_brackets(manifest: CompiledManifest) -> None:
    assert values("[ДЕПОЗИТ ПРЕДУСТАНОВЛЕННАЯ СУММА]\nDeposit ID: 7", manifest, "header") == [
        "ДЕПОЗИТ ПРЕДУСТАНОВЛЕННАЯ СУММА"]
    assert values("без заголовка [не в начале]", manifest, "header") == []


def test_label_value_pairs(manifest: CompiledManifest) -> None:
    fs = feats("[ДЕПОЗИТ]\nDeposit ID: 42\nНомер счёта: 1481598133", manifest, "pair")
    assert [(f["l"], f["v"]) for f in fs] == [("Deposit ID", "42"), ("Номер счёта", "1481598133")]


def test_dictionary_words_are_normalised(manifest: CompiledManifest) -> None:
    # case, ё -> е, initial form, transliteration
    assert values("ДЕПОЗИТ", manifest, "word") == ["депозит"]
    assert values("два депозита", manifest, "word") == ["депозит"]
    assert values("Елка и ёлки", manifest, "word") == ["елка", "елка"]
    assert values("depozit", manifest, "word") == ["депозит"]  # transliteration, not translation
    (f,) = feats("это SKAM", manifest, "word")
    assert f["v"] == "скам" and f["d"] == "scam"
    assert values("ничего подходящего", manifest, "word") == []


def test_markers(manifest: CompiledManifest) -> None:
    assert sorted(values("✅ принято, ❌ отказ, 🔴 52", manifest, "marker")) == ["✅", "❌", "🔴"]


def test_commands_are_marked(manifest: CompiledManifest) -> None:
    ex = extract("/change_dep_limits 3", manifest)
    assert ex.command == {"name": "change_dep_limits", "args": ["3"], "raw_args": "3", "slash": True}
    assert [(f["v"], f["args"]) for f in ex.features if f["k"] == "command"] == [("change_dep_limits", ["3"])]
    assert extract("/status@autocash_bot", manifest).command["name"] == "status"  # type: ignore[index]
    bare = extract("scam 1481598133", manifest).command
    assert bare is not None and bare["name"] == "scam" and bare["args"] == ["1481598133"] and not bare["slash"]
    assert extract("unban 1481598133", manifest).command["name"] == "unban"  # type: ignore[index]
    assert extract("scam это плохо\nвторая строка", manifest).command is None
    assert extract("обычный текст", manifest).command is None


# ---- FT-3.5: sub-fields ---------------------------------------------------------------------
def test_subfields_user(manifest: CompiledManifest) -> None:
    (f,) = feats("Пользователь: @x ID: 7287295969", manifest, "pair")
    assert f["l"] == "Пользователь" and f["sub"] == {"name": "@x", "id": "7287295969"}


def test_subfields_marker_and_two_numbers(manifest: CompiledManifest) -> None:
    (f,) = feats("Успешных депозитов: 🔴 52 (всего: 52)", manifest, "pair")
    assert f["sub"] == {"marker": "🔴", "count": "52", "total": "52"}


def test_subfields_method_and_limits(manifest: CompiledManifest) -> None:
    (f,) = feats("Метод оплаты: СБП Т-Банк 3 000 - 500 000", manifest, "pair")
    assert f["sub"] == {"method": "СБП Т-Банк", "min": "3 000", "max": "500 000"}


# ---- FT-3.6: position and the nearest label; no meaning ----------------------------------------
def test_every_feature_has_position_and_label(manifest: CompiledManifest) -> None:
    text = "[ДЕПОЗИТ]\nПользователь: @x ID: 7287295969\nНомер счёта: 1481598133"
    ex = extract(text, manifest)
    by_value = {f["v"]: f for f in ex.features if f["k"] == "number"}
    assert by_value["7287295969"]["l"] == "Пользователь" and by_value["1481598133"]["l"] == "Номер счёта"
    for f in ex.features:
        assert f["s"] <= f["e"]
        if f["k"] in ("number", "username", "header"):
            assert ex.text[f["s"]:f["e"]].strip("[] \n") == f["v"]
    # The extractor names forms only; meaning (TGID, MelID, Deposit ID) is the sorter's job (P-3).
    assert {f["k"] for f in ex.features} <= {"header", "pair", "number", "username", "word", "marker", "amount",
                                             "amount_int", "phone", "card", "url", "command"}
    assert not any(key in f for f in ex.features for key in ("tgid", "mel_id", "deposit_id"))


# ---- FT-3.8: validation ---------------------------------------------------------------------
@pytest.mark.parametrize("patch", [
    {"amount_regex": "("},
    {"subfields": {"X": {"regex": r"\d+"}}},  # no named groups
    {"patterns": [{"name": "bad", "regex": r"(?<=a)b"}]},  # lookbehind is not linear time: rejected by re2
    {"dictionaries": {"d": ["два слова"]}},
])
def test_invalid_manifest_is_rejected(patch: dict[str, Any]) -> None:
    with pytest.raises(ManifestError):
        compile_manifest(ManifestCfg.model_validate({**MANIFEST, **patch}))


def test_manifest_version_changes_with_content() -> None:
    a = compile_manifest(ManifestCfg.model_validate(MANIFEST))
    b = compile_manifest(ManifestCfg.model_validate({**MANIFEST, "number_min_len": 5}))
    assert a.version == compile_manifest(ManifestCfg.model_validate(MANIFEST)).version != b.version
