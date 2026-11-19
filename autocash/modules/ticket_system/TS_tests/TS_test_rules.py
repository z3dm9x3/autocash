"""TS: C-ticket rules on real phrases of players (Cash Monitor Dev, private dialogs, 2026-08..11)."""

from __future__ import annotations

import pytest

from autocash.modules.ticket_system.TS_rules import DEFAULT_RULES, WW, D, Q, bare_code, compile_rules, decide, hits

RULES = compile_rules(DEFAULT_RULES)


def ticket(text: str, media: str | None = None) -> str | None:
    """The type one message gives a ticket, None when it fires nothing."""
    decided = decide(hits(text, media, RULES))
    return decided[0] if decided else None


@pytest.mark.parametrize("text", [
    "Не пришёл депозит", "не пришел депозит", "Не дошло пополнение", "Почему еще нет пополнения",
    "Здравствуйте пополняла деньги не поступили.", "Пополнил на 500 а баланс пустой", "пополнила",
    "Здравствуйте, мой вопрос с депозитом в 500 рублей так и не был решен", "Что с моим депозитом?",
    "Дэп // Не приходит", "деп не зашел", "депнул 1000 и тишина", "задепал вчера", "сделал деп",
    "Закинул на игру 300", "закинула деньги", "Попалнил 1000", "дипозит не пришел", "пополнения нет уже сутки",
    "Я депозит оплатил, а поступления нету", "Я не могу предоставить ПДФ а уже оплатил что делать?",
    "Не успела отправить чек", "Чек прикрепила", "У меня платеж не прошёл", "Перевёл, жду",
    "Здравствуйте, я оплатил по выданным реквизитам.",
    "Переведите точную сумму\n430 рублей\n\nНажмите, чтобы скопировать!",
    "Заявка #1 275 260 создана.\nEPOS ID: 1349261\nНомер аккаунта: 1805366925",
])
def test_deposit_words_in_any_form_make_a_d_ticket(text: str) -> None:
    assert ticket(text) == D


def test_a_pdf_alone_is_a_deposit() -> None:
    assert ticket("", "pdf") == D
    assert ticket("", "document") is None and ticket("", "photo") is None


@pytest.mark.parametrize("text", [
    "Вывод не пришёл за 24 часа", "Здравствуйте, что с выводом?", "Я снял 1000 рублей", "выплата будет?",
    "Добрый день прошло 48 часов а вывод не поступил", "не могу вывести деньги", "Вывод с аккаунта 113 465 895 tfVQ",
    "tfVQ", "1 758 918 313 / rywT", "5AKW", "Вывод завис",
])
def test_withdrawal_words_and_bare_codes_make_a_ww_ticket(text: str) -> None:
    assert ticket(text) == WW


@pytest.mark.parametrize("text", [
    "Здравствуйте", "Привет", "?", "Ау", "Деньги не пришли", "Где деньги", "уже сутки жду", "Когда придут на карту",
    "Завис", "У меня всё зависло", "Здравствуйте, ваш вопрос ещё актуален?", "Скиньте рабочую ссылку на сайт",
    "Зачисление когда будет?", "Есть реквизиты на 500?", "сняла видео как просили", "Salam", "депутат", "департамент",
])
def test_no_explicit_trigger_fires_nothing(text: str) -> None:
    """"завис", "сутки", "не пришли", "на карту", "зачисление", "реквизиты" are no triggers."""
    assert ticket(text) is None


def test_both_kinds_in_one_message_are_q() -> None:
    assert decide(hits("пополнял через тг бота, как вывести деньги?", None, RULES)) == (Q, "mixed")
    assert decide(hits("Не приходит вывод", "pdf", RULES)) == (Q, "mixed")


def test_the_bot_withdrawal_guide_is_not_a_deposit() -> None:
    guide = ("📍Заходим👇\n📍1. Настройки!\n📍2. Вывести со счета!\n📍3. Наличные\n📍4.Сумму для Вывода!\n"
             "|ВЫБИРАЙТЕ ТУ ПЛАФТОРМУ ЧЕРЕЗ КОТОРУЮ ВЫ ВНОСИЛИ ДЕПОЗИТ|\n📍6. Получить Код!")
    assert ticket(guide) == WW


def test_the_fragment_is_what_fired() -> None:
    assert [h.fragment for h in hits("Не пришёл деп, пополнила вчера", None, RULES)] == ["пополнила", "деп"]
    assert [h.fragment for h in hits("", "pdf", RULES)] == ["PDF"]
    assert [(h.rule, h.fragment) for h in hits("tfVQ", None, RULES)] == [("w.code", "tfVQ")]


def test_bare_codes() -> None:
    assert bare_code("M9mp") == "M9mp" and bare_code("yzdm") == "yzdm" and bare_code("ID 1773590121 / 5kth") == "5kth"
    assert bare_code("EPOS") is None and bare_code("Cash") is None
    assert bare_code("код tfVQ пришёл") is None  # Cyrillic: a sentence, the word rules decide
    assert bare_code("7063832") is None  # an SMS code: digits only
    assert bare_code("hello there my friend") is None


def test_rules_from_the_configuration_replace_the_defaults() -> None:
    rules = compile_rules([{"name": "d.custom", "kind": "D", "pattern": "касса\\s+не\\s+приняла"}])
    assert [h.rule for h in hits("Касса не приняла", None, rules)] == ["d.custom"]
    with pytest.raises(ValueError):
        compile_rules([{"name": "x", "kind": "Z", "pattern": "x"}])
