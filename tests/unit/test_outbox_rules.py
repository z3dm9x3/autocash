"""OBX_Antifloodwaiter and OBX_Router (D-OBX-2, D-OBX-3)."""

from __future__ import annotations

import time

from autocash.core.config.schema import AntifloodCfg
from autocash.core.outbox.OBX_antiflood import Antiflood
from autocash.core.outbox.OBX_router import choose

USER = AntifloodCfg(per_account_per_minute=10, per_chat_interval_s=2.0, group_per_minute=10, jitter_s=0.0)


def test_writes_of_an_account_are_spread_evenly_never_a_burst() -> None:
    af = Antiflood()
    assert af.delay(USER, 1, 100) == 0.0
    af.wrote(USER, 1, 100)
    # 10 per minute -> one write per 6 s, to any chat (500 tasks at once still go one by one).
    assert 5.9 < af.delay(USER, 1, 200) <= 6.0
    assert af.delay(USER, 2, 200) == 0.0  # other accounts are not held back


def test_ten_writes_in_a_minute_wait_for_the_window() -> None:
    af = Antiflood()
    now = time.monotonic()
    for i in range(10):
        af.wrote(None, 1, 100 + i, at=now - 50 + i)  # 10 writes in the last minute
    assert 9.0 < af.delay(USER, 1, 999) <= 10.0  # the oldest leaves the window in ~10 s


def test_one_chat_gets_one_write_per_two_seconds_and_a_group_ten_per_minute() -> None:
    cfg = AntifloodCfg(per_account_per_minute=0, per_chat_interval_s=2.0, group_per_minute=10)
    af = Antiflood()
    af.wrote(cfg, 1, 100)
    assert 1.9 < af.delay(cfg, 1, 100) <= 2.0 and af.delay(cfg, 1, 101) == 0.0
    now = time.monotonic()
    for i in range(10):
        af.wrote(cfg, 1, -500, at=now - 30 - i)
    assert 29.0 < af.delay(cfg, 1, -500) <= 30.0


def test_router_takes_the_least_loaded_account() -> None:
    af = Antiflood()
    now = time.monotonic()
    for account, writes in ((1, 3), (2, 2), (3, 9)):  # A sent 3, B 2, C 9 in the last minute
        for i in range(writes):
            af.wrote(None, account, -100, at=now - 20 - i)
    decision = choose([1, 2, 3], af, lambda a: 0)
    assert decision is not None and decision.account_id == 2 and decision.routed
    assert decision.load == {1: 3, 2: 2, 3: 9}
    queued = choose([1, 2, 3], af, lambda a: 5 if a == 2 else 0)
    assert queued is not None and queued.account_id == 1  # its queue counts too
    # On a tie the account with the sending duty of the chat goes first.
    tie = choose([1, 2, 3], Antiflood(), lambda a: 0, preferred=3)
    assert tie is not None and tie.account_id == 3
    single = choose([3], af, lambda a: 0)
    assert single is not None and not single.routed
    assert choose([], af, lambda a: 0) is None


def test_reads_have_their_own_budget_spread_evenly() -> None:
    cfg = AntifloodCfg(per_account_per_minute=20, per_chat_interval_s=2.0, group_per_minute=10, reads_per_minute=20)
    af = Antiflood()
    delays = [af.reserve_read(cfg, 1) for _ in range(3)]  # three reads asked at once
    assert delays[0] == 0.0 and 2.9 < delays[1] <= 3.0 and 5.9 < delays[2] <= 6.0  # one per 3 s
    assert af.reserve_read(cfg, 2) == 0.0  # every account has its own budget
    assert af.delay(cfg, 1, 100) == 0.0  # reads do not take the budget of writes


def test_the_fuse_halves_reads_and_writes() -> None:
    cfg = AntifloodCfg(per_account_per_minute=20, per_chat_interval_s=2.0, group_per_minute=10, reads_per_minute=20)
    af = Antiflood()
    af.reserve_read(cfg, 1, factor=0.5)
    assert 5.9 < af.reserve_read(cfg, 1, factor=0.5) <= 6.0  # 10 reads a minute
    af.wrote(cfg, 1, 100, factor=0.5)
    assert 5.9 < af.delay(cfg, 1, 200, factor=0.5) <= 7.0  # 10 writes a minute (+ jitter 0)


def test_the_router_of_reads_takes_the_least_loaded_reader() -> None:
    from autocash.core.outbox.OBX_router import choose_reader
    af = Antiflood()
    for _ in range(3):
        af.reserve_read(None, 1)
    af.reserve_read(None, 2)
    assert choose_reader([1, 2, 3], af) == 3
    assert choose_reader([1, 2], af, duty=1) == 2
    assert choose_reader([3, 4], af, duty=4) == 4  # a tie: the read duty
