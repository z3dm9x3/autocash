"""AS: Autosup - the player's ID in private dialogs (AS_readme.md, D-AS-1..4).

Called by the protocols of Role Manager, works in the background, private dialogs only:
- contact.first (a person wrote for the first time ever): ask "Здравствуйте! Пришлите ваш ID";
- contact.second (a person not in the base who has a history with the account): look for the ID
  in the history of the dialog; found -> take it; not found -> ask, as on the first contact, but
  the dialog is marked "second";
- the next message of the user with an ID (AS_ids) while Autosup waits for it: the player is
  written (Role Manager: rm_players, rm_player_mel) and the message is pinned. An ID written with
  spaces is sent back normalized by Autosup, and that message of Autosup is pinned.
Every write to Telegram is an Outbox task (send, pin); nothing is written to Telegram directly.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.modules.api import Context, Event, Module, Services
from autocash.modules.autosup.AS_ids import FoundId, find_id
from autocash.modules.role_manager import RM_store

log = logging.getLogger(__name__)

PREFIX = "AS"
NAME = "autosup"


class ASConfig(BaseModel):
    ask_text: str = "Здравствуйте!\nПришлите ваш ID"
    wait_days: float = 7.0  # waiting for the ID stops after this
    history_messages: int = 500  # contact.second: how deep the history of the dialog is read
    history_days: float = 180.0


class AutosupModule(Module):
    name: ClassVar[str] = NAME
    protocols: ClassVar[list[str]] = ["contact.first", "contact.second"]
    private_dialogs: ClassVar[bool] = True
    config_model: ClassVar[type[BaseModel] | None] = ASConfig
    migrations: ClassVar[Path | None] = Path(__file__).parent / "AS_migrations"
    periodic_s: ClassVar[float | None] = 600.0
    timeout_s: ClassVar[float] = 120.0  # contact.second reads the history through the limiter

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    def cfg(self) -> ASConfig:
        mod = self.svc.runtime().config.modules.get(NAME)
        return ASConfig.model_validate(mod.config if mod is not None else {})

    # ---- protocols (D-RM-12) ---------------------------------------------------------------------
    async def on_protocol(self, protocol: str, subject: int, data: dict[str, Any]) -> None:
        account_id, chat_id = int(data["account_id"]), int(data["chat_id"])
        if chat_id != subject:
            return  # private dialogs only
        if protocol == "contact.second":
            found = await self._search_history(account_id, subject)
            if found is not None:
                msg_id, fid = found
                await self._register(subject, account_id, "second", "history", msg_id, fid)
                return
        await self._ask(subject, account_id, protocol.removeprefix("contact."))

    async def _ask(self, tg_id: int, account_id: int, contact: str) -> None:
        cfg = self.cfg()
        pool = self.svc.db.pool
        inserted = await pool.fetchval(
            "INSERT INTO as_dialogs (tg_id, account_id, contact, state, asked_at, expires_at) "
            "VALUES ($1, $2, $3, 'awaiting_id', now(), now() + make_interval(secs => $4::float8)) "
            "ON CONFLICT (tg_id) DO NOTHING RETURNING tg_id", tg_id, account_id, contact, cfg.wait_days * 86400)
        if inserted is None:
            return  # the protocol came again after a crash: the request is queued already
        await self._outbox(OutboundDraft(chat_id=tg_id, text=cfg.ask_text, rule="ask_id", kind="client_reply",
                                         via_account=account_id, dedup_key=f"{NAME}:ask:{tg_id}"))

    async def _search_history(self, account_id: int, tg_id: int) -> tuple[int, FoundId] | None:
        """The ID in the user's own earlier messages of the dialog (contact.second, D-AS-3)."""
        cfg = self.cfg()
        read = self.svc.read_dialog
        if read is None:
            return None
        since = datetime.now(UTC) - timedelta(days=cfg.history_days)
        try:
            events = await read(account_id, tg_id, since, cfg.history_messages)
        except Exception:
            log.warning("AS: history of %s via %s is not read", tg_id, account_id, exc_info=True)
            return None
        found: dict[str, tuple[int, FoundId]] = {}
        for ev in events:
            if ev.sender_id != tg_id:
                continue
            fid = find_id(ev.text)
            if fid is not None:
                found.setdefault(fid.mel_id, (ev.msg_id, fid))
        if len(found) != 1:
            if found:
                log.info("AS: %s has %d different IDs in the history: none is taken", tg_id, len(found))
            return None
        return next(iter(found.values()))

    # ---- private messages ------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if (rec.chat_id <= 0 or rec.sender_id != rec.chat_id or rec.origin != "external" or rec.event != "new"
                or not rec.text):
            return
        row = await ctx.fetchrow("SELECT account_id, contact, asked_at FROM as_dialogs "
                                 "WHERE tg_id = $1 AND state = 'awaiting_id' AND expires_at > now()", rec.sender_id)
        if row is None or rec.msg_date < row["asked_at"].timestamp() - 1:
            return  # not waiting, or a message written before the request
        fid = find_id(rec.text)
        if fid is None:
            return  # "the next number": messages without an ID do not end the wait
        await self._register(rec.sender_id, rec.account_id, row["contact"], "reply", rec.msg_id, fid)

    # ---- the player ------------------------------------------------------------------------------
    async def _register(self, tg_id: int, account_id: int, contact: str, found_in: str, msg_id: int,
                        fid: FoundId) -> None:
        link = await RM_store.link_player(self.svc.db.pool, tg_id, fid.mel_id, source=NAME)
        await self.svc.db.pool.execute(
            "INSERT INTO as_dialogs (tg_id, account_id, contact, state, mel_id, found_in, msg_id, player_id) "
            "VALUES ($1, $2, $3, 'done', $4, $5, $6, $7) ON CONFLICT (tg_id) DO UPDATE SET state = 'done', "
            "mel_id = EXCLUDED.mel_id, found_in = EXCLUDED.found_in, msg_id = EXCLUDED.msg_id, "
            "player_id = EXCLUDED.player_id, updated_at = now()",
            tg_id, account_id, contact, fid.mel_id, found_in, msg_id, link.player_id)
        log.info("AS: user %s -> player %s, MelID %s (%s, %s)", tg_id, link.player_id, fid.mel_id, contact, found_in)
        if link.mel_owner_differs:
            self.svc.notify(f"{PREFIX}: MelID {fid.mel_id} пользователя {tg_id} уже записан за другим игроком; "
                            f"пользователь остался в игроке {link.player_id}, склейки нет")
        # Feedback: the message with the ID is pinned; an ID with spaces is written back normalized.
        if fid.spaced:
            draft = OutboundDraft(chat_id=tg_id, text=fid.mel_id, rule="id_normalized", kind="client_reply",
                                  via_account=account_id, pin_after_send=True, dedup_key=f"{NAME}:id:{tg_id}")
        else:
            draft = OutboundDraft(chat_id=tg_id, text="", rule="pin_id", kind="client_reply", via_account=account_id,
                                  action="pin", pin_msg_id=msg_id, dedup_key=f"{NAME}:pin:{tg_id}")
        await self._outbox(draft)

    async def _outbox(self, draft: OutboundDraft) -> None:
        create = self.svc.outbox
        if create is None:
            raise RuntimeError("the Outbox is not available to the module")
        await create(draft, NAME)

    # ---- background ------------------------------------------------------------------------------
    async def periodic(self) -> None:
        expired = await self.svc.db.pool.execute(
            "UPDATE as_dialogs SET state = 'expired', updated_at = now() "
            "WHERE state = 'awaiting_id' AND expires_at <= now()")
        if expired and expired != "UPDATE 0":
            log.info("AS: waiting for the ID stopped: %s", expired)
