"""Membership map: which pool accounts are in which chat (FT-1.4).

Built from the dialog list of every account, kept current by join / leave events, by errors of
active calls and - cheaply - by the fact that an account received an update from a chat.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

import asyncpg

from autocash.core.storage.writer import BatchWriter
from autocash.core.telegram.api import DialogInfo

_UPSERT = """
INSERT INTO chat_membership (chat_id, account_id, can_send) VALUES ($1, $2, $3)
ON CONFLICT (chat_id, account_id) DO UPDATE SET can_send = EXCLUDED.can_send, updated_at = now()
"""


class Membership:
    def __init__(self, writer: BatchWriter, on_change: Callable[[], None]) -> None:
        self._writer = writer
        self._on_change = on_change
        self.chats: dict[int, dict[int, bool]] = {}  # chat -> account -> can send
        self.kinds: dict[int, str] = {}

    def members(self, chat_id: int) -> dict[int, bool]:
        return self.chats.get(chat_id, {})

    def chats_of(self, account_id: int) -> list[int]:
        return [chat for chat, accounts in self.chats.items() if account_id in accounts]

    def load(self, rows: Iterable[Any]) -> None:
        """Rows stored before a restart; they do not override what is already known."""
        for row in rows:
            self.chats.setdefault(row["chat_id"], {}).setdefault(row["account_id"], row["can_send"])

    def note(self, chat_id: int, account_id: int, kind: str | None = None) -> None:
        """An update from the chat arrived on the account: it is a member."""
        if kind and kind != "unknown":
            self.kinds[chat_id] = kind
        if account_id not in self.chats.get(chat_id, {}):
            self.set(chat_id, account_id, True, kind)

    def set(self, chat_id: int, account_id: int, can_send: bool = True, kind: str | None = None) -> None:
        if kind and kind != "unknown":
            self.kinds[chat_id] = kind
        accounts = self.chats.setdefault(chat_id, {})
        if accounts.get(account_id) == can_send:
            return
        accounts[account_id] = can_send
        self._writer.submit(lambda conn: conn.execute(_UPSERT, chat_id, account_id, can_send))
        self._on_change()

    def remove(self, chat_id: int, account_id: int) -> None:
        accounts = self.chats.get(chat_id)
        if not accounts or account_id not in accounts:
            return
        del accounts[account_id]
        if not accounts:
            del self.chats[chat_id]
        self._writer.submit(lambda conn: conn.execute(
            "DELETE FROM chat_membership WHERE chat_id = $1 AND account_id = $2", chat_id, account_id))
        self._on_change()

    def replace(self, account_id: int, dialogs: list[DialogInfo]) -> None:
        """The full dialog list of an account: links that are not in it are dropped."""
        fresh = {d.chat_id: d for d in dialogs}
        changed = False
        for chat_id in self.chats_of(account_id):
            if chat_id not in fresh:
                accounts = self.chats[chat_id]
                del accounts[account_id]
                if not accounts:
                    del self.chats[chat_id]
                changed = True
        for chat_id, dialog in fresh.items():
            self.kinds[chat_id] = dialog.kind
            accounts = self.chats.setdefault(chat_id, {})
            # A send ban learned from an error is kept: the dialog list does not always show it.
            can_send = dialog.can_send and accounts.get(account_id, True)
            if accounts.get(account_id) != can_send:
                accounts[account_id] = can_send
                changed = True
        rows = [(chat_id, account_id, accounts[account_id])
                for chat_id, accounts in self.chats.items() if account_id in accounts]

        async def write(conn: asyncpg.Connection) -> None:
            await conn.execute("DELETE FROM chat_membership WHERE account_id = $1", account_id)
            if rows:
                await conn.executemany(_UPSERT, rows)

        self._writer.submit(write)
        if changed:
            self._on_change()

    def drop_account(self, account_id: int) -> None:
        chats = self.chats_of(account_id)
        for chat_id in chats:
            accounts = self.chats[chat_id]
            del accounts[account_id]
            if not accounts:
                del self.chats[chat_id]
        self._writer.submit(lambda conn: conn.execute(
            "DELETE FROM chat_membership WHERE account_id = $1", account_id))
        if chats:
            self._on_change()
