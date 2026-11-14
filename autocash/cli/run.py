"""Run the pipeline with one Telegram account (stage 1a; the pool arrives with stage 1b).

    python -m autocash.cli.run --account "<folder in accounts/>" [--config config] [--data data]

The PostgreSQL DSN is read from the environment variable AUTOCASH_PG_DSN and is never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path

from autocash.accounts import find_account
from autocash.core.app import App
from autocash.core.config.loader import ConfigError
from autocash.core.telegram.telethon_client import TelethonClient
from autocash.modules.api import Module
from autocash.modules.demo.module import DemoModule

log = logging.getLogger("autocash")
ROOT = Path(__file__).resolve().parents[2]


def build_modules(app_config_modules: dict[str, object]) -> list[Module]:
    # The demo module answers /ping in real chats, so it runs only when the config names it.
    return [DemoModule()] if "demo" in app_config_modules else []


async def run(account_name: str, config: Path, data: Path) -> int:
    dsn = os.environ.get("AUTOCASH_PG_DSN")
    if not dsn:
        log.error("AUTOCASH_PG_DSN is not set")
        return 2
    account = find_account(account_name)
    client = TelethonClient(account.session_path, account.api_id, account.api_hash,
                            bot_token=account.bot_token or None)
    try:
        app = App(config_path=config, sqlite_path=data / "buffer.sqlite", pg_dsn=dsn, client=client, modules=[])
    except ConfigError as exc:
        log.error("configuration is invalid: %s", exc)
        return 2
    for module in build_modules(dict(app.store.current.config.modules)):
        app.queues.register(module)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows has no loop signal handlers
            loop.add_signal_handler(sig, stop.set)

    await app.start()
    log.info("account %s is connected, intake is running", client.account_id)
    disconnected = asyncio.create_task(client.run_until_disconnected())
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({disconnected, stopping}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (disconnected, stopping):
            task.cancel()
        await app.stop()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Autocash: Telegram message processor")
    parser.add_argument("--account", required=True, help="account folder name in accounts/")
    parser.add_argument("--config", type=Path, default=ROOT / "config", help="YAML file or a directory of *.yaml")
    parser.add_argument("--data", type=Path, default=ROOT / "data", help="directory of the SQLite buffer")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(asyncio.run(run(args.account, args.config, args.data)))


if __name__ == "__main__":
    main()
