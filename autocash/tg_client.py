from telethon import TelegramClient

from autocash.accounts import Account


async def connect(account: Account, *, interactive: bool = False) -> TelegramClient:
    client = TelegramClient(account.session_path, account.api_id, account.api_hash)
    if account.kind == "bot":
        await client.start(bot_token=account.bot_token)
        return client
    await client.connect()
    if await client.is_user_authorized():
        return client
    if not interactive:
        await client.disconnect()
        raise RuntimeError(f"{account.name}: не залогинен, запустите login.py")
    await client.start(phone=account.phone)
    return client
