import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from autocash.accounts import find_account  # noqa: E402
from autocash.tg_client import connect  # noqa: E402


async def login(folder_name: str) -> None:
    account = find_account(folder_name)
    client = await connect(account, interactive=True)
    me = await client.get_me()
    print(f"{account.name}: вход выполнен, id {me.id}")
    await client.disconnect()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit('Использование: python accounts/login.py "<папка аккаунта>"')
    asyncio.run(login(sys.argv[1]))
