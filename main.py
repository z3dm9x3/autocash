import asyncio

from telethon import events

from autocash.accounts import load_accounts
from autocash.tg_client import connect


async def run_account(account) -> None:
    try:
        client = await connect(account)
    except RuntimeError as exc:
        print(exc)
        return

    @client.on(events.NewMessage)
    async def on_new_message(event: events.NewMessage.Event) -> None:
        print(f"[{account.name}] {event.chat_id}: {event.raw_text}")

    print(f"{account.name}: слушаю")
    await client.run_until_disconnected()


async def main() -> None:
    accounts = load_accounts()
    if not accounts:
        raise SystemExit("В accounts/ нет аккаунтов с account.env")
    await asyncio.gather(*(run_account(a) for a in accounts))


if __name__ == "__main__":
    asyncio.run(main())
