"""TS: the ticket system - tickets between accounts, the L1/L2 processes (TS_readme.md, D-TS-1..3).

TS keeps the data and runs the processes, but writes nothing to Telegram: it takes facts from other
modules (`TS_api.receive`) and hands results on through protocols (ticket.d | ticket.ww | ticket.q).
Now: tickets are opened and typed by C-ticket; the ticket that found no trigger in its first `burst_s`
becomes Q here, in the background.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel

from autocash.modules.api import Context, Event, Module, Services
from autocash.modules.ticket_system import TS_api
from autocash.modules.ticket_system.TS_rules import Q

log = logging.getLogger(__name__)

PREFIX = "TS"


class TicketSystemModule(Module):
    name: ClassVar[str] = TS_api.NAME
    config_model: ClassVar[type[BaseModel] | None] = TS_api.TSConfig
    migrations: ClassVar[Path | None] = Path(__file__).parent / "TS_migrations"
    periodic_s: ClassVar[float | None] = 2.0  # the end of the first minute of a ticket

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    async def start(self) -> None:
        # Whole seconds: Telegram dates have no fractions, a message of this very second still counts.
        moment = datetime.now(UTC).replace(microsecond=0)
        TS_api.accept_from(moment)
        log.info("TS: tickets are made from messages written since %s", moment.isoformat())

    async def handle(self, event: Event, ctx: Context) -> None:
        """TS subscribes to no messages: facts come from other modules."""

    async def periodic(self) -> None:
        pool = self.svc.db.pool
        due = await pool.fetch("SELECT id FROM ts_tickets WHERE ticket IS NULL AND decide_by <= now() "
                               "ORDER BY decide_by LIMIT 100")
        emitted = False
        for row in due:
            async with pool.acquire() as conn, conn.transaction():
                still = await conn.fetchval("SELECT ticket IS NULL FROM ts_tickets WHERE id = $1 FOR UPDATE", row["id"])
                if still:
                    emitted |= await TS_api.set_ticket(conn, int(row["id"]), Q, "none", [], None)
        if emitted:
            self.svc.wake_protocols()
