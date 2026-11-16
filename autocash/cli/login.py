"""Log in to a Telegram account and write its encrypted session file (FT-1.2, FT-1.9).

    python -m autocash.cli.login --sessions <dir> --name <account name>
    python -m autocash.cli.login --sessions <dir> --import-folder "accounts/<folder>"
    python -m autocash.cli.login --new-key

The confirmation code and the two-step password are asked in the console by Telethon. The
result is `<dir>/<name>.session.enc`; putting it into the directory watched by the running
Autocash adds the account to the pool without a restart.

NOT verified against live Telegram: logging in needs a real account.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession

from autocash.accounts import find_account
from autocash.core.pool.sessions import SessionData, load_key, new_key, write_session


def _ask(prompt: str, *, secret: bool = False) -> str:
    while True:
        value = (getpass.getpass(prompt) if secret else input(prompt)).strip()
        if value:
            return value


async def _login(name: str, kind: str, api_id: int, api_hash: str, phone: str, bot_token: str,
                 session: StringSession | str) -> SessionData:
    client = TelegramClient(session, api_id, api_hash, flood_sleep_threshold=0)
    if kind == "bot":
        await client.start(bot_token=bot_token)
    else:
        await client.start(phone=phone)
    try:
        me = await client.get_me()
        print(f"Вход выполнен: id {me.id}")
        return SessionData(name=name, kind=kind, api_id=api_id, api_hash=api_hash,
                           session=StringSession.save(client.session), bot_token=bot_token)
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="Autocash: вход в аккаунт Telegram")
    parser.add_argument("--sessions", type=Path, help="каталог зашифрованных сессий")
    parser.add_argument("--name", help="имя аккаунта (имя файла сессии)")
    parser.add_argument("--import-folder", help="папка прототипа accounts/<имя> с account.env и session.session")
    parser.add_argument("--new-key", action="store_true", help="напечатать новый ключ шифрования сессий")
    args = parser.parse_args()

    if args.new_key:
        # The only secret this tool ever prints: the operator stores it and never commits it.
        print(new_key())
        return
    if args.sessions is None:
        parser.error("--sessions is required")
    key = load_key()

    if args.import_folder:
        account = find_account(Path(args.import_folder).name)
        data = asyncio.run(_login(args.name or account.name, account.kind, account.api_id, account.api_hash,
                                  account.phone, account.bot_token, account.session_path))
    else:
        name = args.name or _ask("Имя аккаунта: ")
        kind = "bot" if _ask("Тип (user/bot) [user]: ").lower().startswith("b") else "user"
        api_id = int(_ask("API ID: "))
        api_hash = _ask("API HASH: ", secret=True)
        phone = _ask("Телефон (+...): ") if kind == "user" else ""
        bot_token = _ask("Токен бота: ", secret=True) if kind == "bot" else ""
        data = asyncio.run(_login(name, kind, api_id, api_hash, phone, bot_token, StringSession()))

    path = write_session(args.sessions, data, key)
    print(f"Сессия записана: {path}")


if __name__ == "__main__":
    main()
