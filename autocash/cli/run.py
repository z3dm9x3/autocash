"""Run Autocash with the pool of Telegram accounts.

    python -m autocash.cli.run [--config config] [--data data]

Accounts come from the directory of encrypted session files named in the config
(`pool.sessions_dir`): a file appearing there adds an account, a file vanishing removes it.
Session files are made by `python -m autocash.cli.login`.

Environment (values are never printed):
    AUTOCASH_PG_DSN            PostgreSQL DSN
    AUTOCASH_SESSION_KEY       base64 key of the session files, or
    AUTOCASH_SESSION_KEY_FILE  path to a file with that key (e.g. a systemd credential)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path

from autocash.core.app import App
from autocash.core.config.loader import ConfigError
from autocash.core.pool.sessions import SessionData
from autocash.core.reactions.module import ReactionsModule
from autocash.core.telegram.api import TelegramClient
from autocash.core.telegram.telethon_client import TelethonClient
from autocash.modules.api import Module
from autocash.modules.autosup.AS_module import AutosupModule
from autocash.modules.demo.module import DemoModule
from autocash.modules.deposits.module import DepositsModule
from autocash.modules.orders.orders_module import OrdersModule
from autocash.modules.players.players_module import PlayersModule
from autocash.modules.quickcast2.QK2_module import QuickCast2Module
from autocash.modules.quickkast.QK_module import QuickKastModule
from autocash.modules.role_manager.RM_module import RoleManagerModule
from autocash.modules.ticket_system.TS_module import TicketSystemModule

log = logging.getLogger("autocash")
ROOT = Path(__file__).resolve().parents[2]


# Role Manager is the first module: the others act on groups by their roles.
AVAILABLE: dict[str, type[Module]] = {"role_manager": RoleManagerModule, "autosup": AutosupModule,
                                      "ticket_system": TicketSystemModule, "demo": DemoModule,
                                      "players": PlayersModule, "deposits": DepositsModule,
                                      "reactions": ReactionsModule, "orders": OrdersModule,
                                      "quickkast": QuickKastModule, "quickcast2": QuickCast2Module}


def build_modules(app_config_modules: dict[str, object]) -> list[Module]:
    # A module runs only when the configuration names it (the demo one answers /ping in real chats).
    return [cls() for name, cls in AVAILABLE.items() if name in app_config_modules]


def client_from_session(data: SessionData) -> TelegramClient:
    return TelethonClient.from_string(data.session, data.api_id, data.api_hash, data.bot_token)


async def run(config: Path, data: Path) -> int:
    dsn = os.environ.get("AUTOCASH_PG_DSN")
    if not dsn:
        log.error("AUTOCASH_PG_DSN is not set")
        return 2
    try:
        app = App(config_path=config, sqlite_path=data / "buffer.sqlite", pg_dsn=dsn, modules=[],
                  client_factory=client_from_session)
    except ConfigError as exc:
        log.error("configuration is invalid: %s", exc)
        return 2
    if not app.store.current.config.pool.sessions_dir:
        log.error("pool.sessions_dir is not set in the configuration: there are no accounts to run")
        return 2
    for module in build_modules(dict(app.store.current.config.modules)):
        app.add_module(module)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows has no loop signal handlers
            loop.add_signal_handler(sig, stop.set)

    await app.start()
    log.info("intake is running, accounts in the pool: %d", len(app.pool.live()))
    try:
        await stop.wait()
    finally:
        await app.stop()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Autocash: Telegram message processor")
    parser.add_argument("--config", type=Path, default=ROOT / "config", help="YAML file or a directory of *.yaml")
    parser.add_argument("--data", type=Path, default=ROOT / "data", help="directory of the SQLite buffer")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(asyncio.run(run(args.config, args.data)))


if __name__ == "__main__":
    main()
