"""Notifications to the service groups `logs` and `debug`, and alarms.

Normally a notification is an outbound task. While PostgreSQL is unavailable (or for an
alarm) it is sent directly through the Gateway account - the emergency channel (NFT-18).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque

import asyncpg

from autocash.actions.send import OutboundDraft, insert_outbound
from autocash.core.config.loader import ConfigStore
from autocash.core.output.output import Output
from autocash.core.storage.pg import Database
from autocash.core.storage.writer import BatchWriter

log = logging.getLogger(__name__)


class Alerts:
    def __init__(self, store: ConfigStore, db: Database, writer: BatchWriter, output: Output) -> None:
        self._store = store
        self._db = db
        self._writer = writer
        self._output = output
        self.history: deque[tuple[float, str, str]] = deque(maxlen=200)
        self._tasks: set[asyncio.Task[bool]] = set()

    def logs(self, text: str) -> None:
        self._send("logs", text, emergency=False)

    def debug(self, text: str) -> None:
        self._send("debug", text, emergency=False)

    def alarm(self, text: str, exclude: int | None = None) -> None:
        """An alarm goes out at once, without PostgreSQL; `exclude` is the account it is about."""
        self._send("logs", text, emergency=True, exclude=exclude)

    def _send(self, channel: str, text: str, *, emergency: bool, exclude: int | None = None) -> None:
        self.history.append((time.time(), channel, text))
        log.warning("[%s] %s", channel, text)
        rt = self._store.current
        chat_id = getattr(rt.config.service_chats, channel)
        if chat_id is None:
            return
        if emergency or not self._db.available:
            task = asyncio.create_task(self._output.direct_send(chat_id, text, exclude))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            return
        draft = OutboundDraft(chat_id=chat_id, text=text, rule=f"alert.{channel}", kind="notify",
                              dedup_key=f"alert:{uuid.uuid4()}")
        ttl = rt.config.output.ttl_s.get("notify", 3600.0)

        async def write(conn: asyncpg.Connection) -> int | None:
            return await insert_outbound(conn, draft, parent_trace_id=None, module=None, ttl_s=ttl,
                                         config_version=rt.version)

        self._writer.submit(write, after=lambda task_id: self._output.enqueue([task_id]) if task_id else None)
