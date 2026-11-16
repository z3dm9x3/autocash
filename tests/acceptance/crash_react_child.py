"""Child for the stage 3 crash test: a rule asks a question, the process dies at a crash point.

Usage: python -m tests.acceptance.crash_react_child <ask|recover> <config> <sqlite> <dsn> <state>
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from autocash.core.app import App
from autocash.core.reactions.module import ReactionsModule
from tests.acceptance.crash_load_child import PersistentFake
from tests.conftest import ACCOUNT, KASSA, PLAYER


async def main(mode: str, config: str, sqlite: str, dsn: str, state: str) -> None:
    fake = PersistentFake(Path(state))
    app = App(config_path=Path(config), sqlite_path=Path(sqlite), pg_dsn=dsn, client=fake.client(ACCOUNT),
              modules=[ReactionsModule()])
    await app.start()
    await asyncio.wait_for(app.ready.wait(), 120)
    print("STARTED", flush=True)
    if mode == "ask":
        fake.post(KASSA, PLAYER, "note ask me")
        await fake.drain()
    await app.idle(timeout=120)
    print("IDLE", flush=True)  # reached only when the crash point did not fire
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:6]))
