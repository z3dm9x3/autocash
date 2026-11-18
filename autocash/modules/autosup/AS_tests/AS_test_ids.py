"""AS: the player's ID in the shapes users really write (Cash Monitor Dev, 2026-11-18)."""

from __future__ import annotations

import pytest

from autocash.modules.autosup.AS_ids import FoundId, find_id


@pytest.mark.parametrize(("text", "expected"), [
    ("1234567890", FoundId("1234567890", False)),
    ("Id 1234567890", FoundId("1234567890", False)),
    ("I'd 1234567890", FoundId("1234567890", False)),
    ("123456789", FoundId("123456789", False)),
    ("12345678 айди", FoundId("12345678", False)),  # 224 real MelIDs have 8 digits
    ("123 456 789", FoundId("123456789", True)),
    ("ID 123 456 789", FoundId("123456789", True)),
    ("1 234 567 890", FoundId("1234567890", True)),
    ("Вывод с аккаунта 1 234 567 890", FoundId("1234567890", True)),
    ("мой ID 1234567890, сумма 5000", FoundId("1234567890", False)),
    ("1000000001", FoundId("1000000001", False)),  # zeros in a row are fine: real MelIDs have them
    # A forwarded request of the kassa bot: the account number is the ID, the other numbers are not.
    ("Заявка #1 234 567 создана.\nEPOS ID: 1234567\nНомер аккаунта: 1234567890", FoundId("1234567890", False)),
])
def test_ids(text: str, expected: FoundId) -> None:
    assert find_id(text) == expected


@pytest.mark.parametrize("text", [
    "Хорошо", "5478", "100 000", "Переведите точную сумму\n5 478 рублей", "Сумма 12345678 руб",
    "+79161234567", "89161234567", "79161234567", "9161234567",  # phones
    "Реквизиты: 2200 1234 5678 9012", "2200123456789012",  # cards
    "receipt-B1234567890G99.pdf", "ozonbank_document_12345678901234.pdf",  # file names
    "1234567890 или 1987654321",  # two different IDs, none labelled: nothing is guessed
])
def test_not_ids(text: str) -> None:
    assert find_id(text) is None
