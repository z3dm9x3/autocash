"""The Telethon adapter finds a private peer whose access hash is not cached (D-OBX-6)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from autocash.core.telegram.api import TelegramError
from autocash.core.telegram.telethon_client import TelethonClient


class FakeTelethon:
    """Knows a user only after the dialogs were read (as Telethon's entity cache)."""

    def __init__(self) -> None:
        self.cached: set[int] = set()
        self.dialog_reads = 0
        self.sent_to: list[Any] = []

    async def get_input_entity(self, chat_id: int) -> str:
        if chat_id not in self.cached:
            raise ValueError(f"Could not find the input entity for PeerUser(user_id={chat_id})")
        return f"peer:{chat_id}"

    async def iter_dialogs(self, limit: int) -> AsyncIterator[None]:
        self.dialog_reads += 1
        self.cached.add(1080307979)
        yield None

    async def pin_message(self, peer: Any, msg_id: int, **kw: Any) -> None:
        self.sent_to.append((peer, msg_id))


def adapter(fake: FakeTelethon, kind: str = "user") -> TelethonClient:
    client = TelethonClient.__new__(TelethonClient)
    client._client = fake
    client.account_id, client.kind = 8201927395, kind
    client.extra_call = None
    return client


async def test_an_unknown_private_peer_is_found_through_the_recent_dialogs() -> None:
    fake = FakeTelethon()
    client = adapter(fake)
    await client.pin_message(1080307979, 5)
    assert fake.sent_to == [("peer:1080307979", 5)] and fake.dialog_reads == 1
    await client.pin_message(1080307979, 6)  # now cached: no second read of the dialogs
    assert fake.dialog_reads == 1


async def test_the_read_of_the_dialogs_waits_for_the_read_budget() -> None:
    fake = FakeTelethon()
    client = adapter(fake)
    gated: list[str] = []

    async def gate(method: str) -> None:
        gated.append(method)

    client.extra_call = gate
    await client.pin_message(1080307979, 5)
    assert gated == ["getDialogs"]  # the extra call is counted as a read (D-OBX-7)


async def test_a_peer_that_is_nowhere_stays_an_error() -> None:
    fake = FakeTelethon()
    with pytest.raises(TelegramError) as err:
        await adapter(fake).pin_message(777, 5)
    assert err.value.code == "PEER_ID_INVALID" and fake.dialog_reads == 1
    with pytest.raises(TelegramError):  # groups are not looked up in the dialogs
        await adapter(fake).pin_message(-100123, 5)
    assert fake.dialog_reads == 1
