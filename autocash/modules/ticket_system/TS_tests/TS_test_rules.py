"""TS: C-ticket rules on real phrases of players (Cash Monitor Dev, private dialogs, 2026-08..11) and on
real withdrawal codes of the [ВЫВОД] cards of the WW groups."""

from __future__ import annotations

import pytest

from autocash.modules.ticket_system.TS_rules import (
    DEFAULT_RULES,
    WW,
    D,
    Q,
    code_candidates,
    compile_rules,
    decide,
    hits,
)

RULES = compile_rules(DEFAULT_RULES)


def ticket(text: str, media: str | None = None) -> str | None:
    """The type one message gives a ticket by its triggers, None when it fires nothing."""
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
    "Ваша заявка отправлена на модерацию Время модерации может составлять до 30 минут",  # the deposit bot
])
def test_deposit_words_in_any_form_make_a_d_ticket(text: str) -> None:
    assert ticket(text) == D


def test_a_pdf_alone_is_a_deposit() -> None:
    assert ticket("", "pdf") == D
    assert ticket("", "document") is None and ticket("", "photo") is None


@pytest.mark.parametrize("text", [
    "Вывод не пришёл за 24 часа", "Здравствуйте, что с выводом?", "Я снял 1000 рублей", "выплата будет?",
    "Добрый день прошло 48 часов а вывод не поступил", "не могу вывести деньги", "Вывод с аккаунта 113 465 895 tfVQ",
    "Вывод завис", "мне на карту не пришли деньги", "Когда придут деньги на карту?", "Номер карты нужен?",
    "где мой выигрыш", "Выигрыш не пришёл", "на карточку ничего", "Ваша заявка отправлена на модерацию",
    "Введите название вашего Банка",
])
def test_withdrawal_words_make_a_ww_ticket(text: str) -> None:
    assert ticket(text) == WW


@pytest.mark.parametrize("text", [
    "Здравствуйте", "Привет", "?", "Ау", "Деньги не пришли", "Где деньги", "уже сутки жду", "Завис",
    "У меня всё зависло", "Здравствуйте, ваш вопрос ещё актуален?", "Скиньте рабочую ссылку на сайт",
    "Зачисление когда будет?", "Есть реквизиты на 500?", "сняла видео как просили", "Salam", "депутат",
    "департамент", "картинка не грузится", "tfVQ", "1 758 918 313 / rywT", "Я отправил деньги обратно, с меня сняли",
])
def test_no_trigger_fires_nothing(text: str) -> None:
    """A bare code is not a trigger: it counts only if the base of withdrawal codes knows it (D-TS-5)."""
    assert ticket(text) is None


@pytest.mark.parametrize("text", [
    "пополнял через тг бота, как вывести деньги?",
    "Пополняла баланс по номеру карты",  # signs of D and W: definitely Q
    "Случайно на карту 2 раза сделала перевод, для пополнения",
])
def test_signs_of_both_in_one_message_are_q(text: str) -> None:
    assert decide(hits(text, None, RULES)) == (Q, "mixed")


def test_a_pdf_with_a_withdrawal_word_is_q() -> None:
    assert decide(hits("Не приходит вывод", "pdf", RULES)) == (Q, "mixed")


def test_the_bot_withdrawal_guide_is_not_a_deposit() -> None:
    guide = ("📍Заходим👇\n📍1. Настройки!\n📍2. Вывести со счета!\n📍3. Наличные\n📍4.Сумму для Вывода!\n"
             "|ВЫБИРАЙТЕ ТУ ПЛАФТОРМУ ЧЕРЕЗ КОТОРУЮ ВЫ ВНОСИЛИ ДЕПОЗИТ|\n📍6. Получить Код!")
    assert ticket(guide) == WW


def test_the_fragment_is_what_fired() -> None:
    assert [h.fragment for h in hits("Не пришёл деп, пополнила вчера", None, RULES)] == ["пополнила", "деп"]
    assert [h.fragment for h in hits("", "pdf", RULES)] == ["PDF"]
    assert [h.fragment for h in hits("на карту не пришло", None, RULES)] == ["карту"]


# Real codes of [ВЫВОД] cards (WW groups, Cash Monitor Dev): mixed case, upper, lower, with digits.
REAL_CODES = ["9Bz7", "S2q3", "Q73Q", "y93N", "2Qcs", "hA3s", "UK2Q", "EyMg", "u62P", "91B4", "vygG", "Xn1T",
              "jjEm", "e2nT", "UfwN", "HCXY", "itpC", "5nEb", "Rcrw", "XqbD", "n8yp", "1WM4", "FzPs", "cLh8",
              "TJXC", "wyci", "jdaa", "Tszx"]


@pytest.mark.parametrize("code", REAL_CODES)
def test_real_codes_are_candidates(code: str) -> None:
    assert code_candidates(code) == [code]
    assert code_candidates(f"Здравствуйте, вывод {code} не пришёл") == [code]
    assert code_candidates(f"ID 1773590121 / {code}") == [code]


@pytest.mark.parametrize("text", [
    "EPOS ID: 1349261", "Read", "Cash", "Queen Cash", "Fast", "998p",  # excluded words; 3 digits in a row
    "1234", "7063832",  # digits only: an SMS code, not a withdrawal code
    "abcde", "abc", "@user1", "#ab12", "file.pdf1", "https://x.ru/ab12", "ab12cd", "привет",
])
def test_not_candidates(text: str) -> None:
    assert code_candidates(text) == []


def test_candidates_keep_the_case_the_order_and_no_repeats() -> None:
    assert code_candidates("M9mp / U7RY / M9mp") == ["M9mp", "U7RY"]
    assert code_candidates("код tfVQ", ["tfvq"]) == []  # the exclusions are case-insensitive


def test_rules_from_the_configuration_replace_the_defaults() -> None:
    rules = compile_rules([{"name": "d.custom", "kind": "D", "pattern": "касса\\s+не\\s+приняла"}])
    assert [h.rule for h in hits("Касса не приняла", None, rules)] == ["d.custom"]
    with pytest.raises(ValueError):
        compile_rules([{"name": "x", "kind": "Z", "pattern": "x"}])
