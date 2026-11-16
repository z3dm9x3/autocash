from __future__ import annotations

import asyncio
import time
from typing import Any

from autocash.core.app import App
from tests.fake_telegram.server import FakeTelegram

ACC2, ACC3 = 112, 113


async def wait_for(predicate: Any, timeout: float = 15.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(f"{what} was not reached in {timeout} s")


def join_all(fake: FakeTelegram, *accounts: int) -> None:
    """Make the accounts members of every existing chat (before they connect)."""
    for chat in fake.chats.values():
        chat.members.update(accounts)


async def ready(app: App, timeout: float = 240.0) -> None:
    """The pipeline is up; a start that never finishes fails the test instead of hanging it."""
    try:
        await asyncio.wait_for(app.ready.wait(), timeout)
    except TimeoutError:
        raise TimeoutError(f"the pipeline did not start in {timeout} s (PostgreSQL, migrations?)") from None


async def started(app: App) -> App:
    """Start the app and wait until every account knows its chats and duties are spread."""
    await app.start()
    await ready(app)
    await wait_for(lambda: all(a.dialogs_loaded for a in app.pool.live()), what="dialogs")
    await wait_for(lambda: app.scheduler.rebalances > 0 and app.scheduler.rebalanced_at >= app.pool.changed_at,
                   what="duties")
    return app


def sent_by(fake: FakeTelegram, chat_id: int) -> list[tuple[int, str]]:
    return [(account, text) for account, chat, text, _ in fake.sent if chat == chat_id]
