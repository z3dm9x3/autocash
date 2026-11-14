"""Child process for the kill -9 test: runs the pipeline until a crash point terminates it.

Usage: python -m tests.acceptance.crash_child <config.yaml> <buffer.sqlite> <pg_dsn> <count>

Every event is first committed to SQLite (= accepted), then the pipeline is released; the
crash point named in AUTOCASH_CRASHPOINT ends the process with os._exit, without any cleanup.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from autocash.core.app import App
from autocash.modules.demo.module import DemoModule
from tests.conftest import ACCOUNT, BOT, DEPOSIT_TEXT, KASSA, KASSA2, make_fake


async def main(config: str, sqlite: str, dsn: str, count: int) -> None:
    fake = make_fake()
    app = App(config_path=Path(config), sqlite_path=Path(sqlite), pg_dsn=dsn, client=fake.client(ACCOUNT),
              modules=[DemoModule()])
    gate = asyncio.Event()
    start_pipeline = app._start_pipeline

    async def gated() -> None:
        await gate.wait()
        await start_pipeline()

    app._start_pipeline = gated  # type: ignore[method-assign]
    await app.start()
    for i in range(count):
        fake.post(KASSA if i % 2 else KASSA2, BOT, DEPOSIT_TEXT.format(deposit_id=i))
    await fake.drain()
    assert app.gateway.accepted == count
    print(f"ACCEPTED {count}", flush=True)
    gate.set()
    await app.idle(timeout=60)
    print("IDLE", flush=True)  # reached only when the crash point never fired
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])))
