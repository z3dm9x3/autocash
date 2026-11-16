"""Child process for the stage 1 hard-reset test: the pipeline under load, killed at a random moment.

Usage: python -m tests.acceptance.crash_load_child <load|recover> <config> <sqlite> <dsn> <state> <count>

The fake Telegram keeps its server-side state (message ids, delivered random_ids) in a file
that survives the kill, like the real Telegram survives a crash of our server: a send repeated
after a restart with the same random_id is answered RANDOM_ID_DUPLICATE (FT-9.3).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from autocash.core.app import App
from autocash.core.telegram.api import RandomIdDuplicate, TelegramError
from autocash.modules.demo.module import DemoModule
from tests.conftest import ACCOUNT, BOT, CASHER, DEBUG, DEPOSIT_TEXT, KASSA, KASSA2, LOGS
from tests.fake_telegram.server import FakeMessage, FakeTelegram


class PersistentFake(FakeTelegram):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path
        for chat_id in (KASSA, KASSA2, LOGS, DEBUG):
            self.add_chat(chat_id, "supergroup", members=(ACCOUNT,))
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                rec = json.loads(line)
                chat = self.chats[rec["chat"]]
                chat.next_id = max(chat.next_id, rec["next"])
                if rec["t"] == "send":
                    self.random_ids[(rec["acc"], rec["rid"])] = rec["msg_id"]
                    self.sent.append((rec["acc"], rec["chat"], rec["text"], rec["msg_id"]))

    def _log(self, rec: dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def post(self, chat_id: int, sender_id: int | None, text: str, **kw: Any) -> FakeMessage:
        chat = self.chats[chat_id]
        self._log({"t": "post", "chat": chat_id, "next": chat.next_id + 1})
        return super().post(chat_id, sender_id, text, **kw)

    def send(self, account_id: int, chat_id: int, text: str, random_id: int, reply_to: int | None) -> int:
        self._api_call(account_id, "send")
        if (account_id, random_id) in self.random_ids:
            raise RandomIdDuplicate()
        chat = self.chats.get(chat_id)
        if chat is None or account_id not in chat.members:
            raise TelegramError("CHANNEL_PRIVATE")
        msg_id = chat.next_id
        # One durable line: the message is "on the server" from this moment on.
        self._log({"t": "send", "acc": account_id, "rid": random_id, "chat": chat_id, "text": text,
                   "msg_id": msg_id, "next": msg_id + 1})
        FakeTelegram.post(self, chat_id, account_id, text, reply_to=reply_to)
        self.random_ids[(account_id, random_id)] = msg_id
        self.sent.append((account_id, chat_id, text, msg_id))
        return msg_id


async def main(mode: str, config: str, sqlite: str, dsn: str, state: str, count: int) -> None:
    fake = PersistentFake(Path(state))
    app = App(config_path=Path(config), sqlite_path=Path(sqlite), pg_dsn=dsn, client=fake.client(ACCOUNT),
              modules=[DemoModule()])
    await app.start()
    print("STARTED", flush=True)
    if mode == "load":
        for i in range(count):
            if i % 4 == 0:
                fake.post(KASSA, CASHER, "/ping")
            else:
                fake.post(KASSA if i % 2 else KASSA2, BOT, DEPOSIT_TEXT.format(deposit_id=i))
            await asyncio.sleep(0.01)
            if i == 12:
                print("FLOWING", flush=True)  # the parent starts the kill countdown from here
        await fake.drain()
    await app.ready.wait()
    await app.idle(timeout=120)
    print("IDLE", flush=True)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6])))
