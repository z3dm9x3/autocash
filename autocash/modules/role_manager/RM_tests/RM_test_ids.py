"""RM: every way of typing a group id leads to the same group."""

from __future__ import annotations

import pytest

from autocash.modules.role_manager.RM_ids import RMIdError, bare, candidates, resolve

GROUP = 3845234572634


@pytest.mark.parametrize("typed", ["3845234572634", "1003845234572634", "-3845234572634", "-1003845234572634"])
def test_four_forms_find_the_group(typed: str) -> None:
    assert resolve(typed, {GROUP, 777}) == GROUP


def test_bare_id_of_telegram_chat_ids() -> None:
    assert bare(-1003845234572634) == GROUP  # a supergroup
    assert bare(-1002581675423) == 2581675423  # a real-size supergroup id (10 digits)
    assert bare(-4456727448) == 4456727448  # a basic group
    assert bare(-1001234567) == 1001234567  # a short id that starts with 100 is not cut


def test_a_short_id_starting_with_100_is_found_both_ways() -> None:
    assert resolve("1001234567", {1001234567}) == 1001234567
    assert resolve("1001234567", {1234567}) == 1234567


def test_unknown_or_bad_ids_are_refused() -> None:
    with pytest.raises(RMIdError, match="не найдена"):
        resolve("123456", {GROUP})
    with pytest.raises(RMIdError, match="число"):
        candidates("-100abc")
    with pytest.raises(RMIdError, match="двум группам"):
        resolve("1001234567", {1001234567, 1234567})
