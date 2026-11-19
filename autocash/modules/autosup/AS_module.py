"""AS: Autosup - the player's ID in private dialogs (AS_readme.md, D-AS-1..7).

Called by the protocols of Role Manager, works in the background, private dialogs only:
- contact.first (a person wrote for the first time ever): wait `grace_s` (60 s) from the first
  message; an ID sent in that time (in the first message too) is taken and pinned; no ID -> ask
  "Здравствуйте! Пришлите ваш ID";
- contact.second (a person not in the base who has a history with the account): look for the ID
  in the history of the dialog; found -> take it; not found -> as the first contact (the same 60 s,
  then the request), the dialog is marked "second";
- while Autosup waits, the next message of the user with an ID (AS_ids) ends it: the player is
  written (Role Manager: rm_players, rm_player_mel) and the message is pinned. An ID written with
  spaces is sent back normalized by Autosup, and that message of Autosup is pinned.
- /as on | off | ? (Admin, Dev; only in a group with the role tech): the switch of Autosup.
- the ticket system (TS, D-TS-1): Autosup is the side that talks to players. Every private message of a
  player (not of the staff) is handed to TS (`TS_api.receive`); the results of TS come back as the
  protocols ticket.d | ticket.ww | ticket.q, and Autosup writes each to the group `logs`. This does not
  depend on /as on|off (that switch is about writing to players).
Every write to Telegram is an Outbox task (send, pin); nothing is written to Telegram directly.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.core.commands.rights import can_run, deny
from autocash.core.outbox.OBX_tasks import OutboundDraft
from autocash.core.record import MessageRecord
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, Event, Module, Services
from autocash.modules.autosup.AS_ids import FoundId, find_id
from autocash.modules.role_manager import RM_store
from autocash.modules.role_manager.RM_module import SERVICE_USERS
from autocash.modules.ticket_system import TS_api

log = logging.getLogger(__name__)

PREFIX = "AS"
NAME = "autosup"
SWITCH_ROLE = "tech"  # /as works only in a group with this role (customer, 2026-11-18)
STAFF = frozenset({"casher", "support", "manager", "admin", "dev"})  # their private messages are not tickets


class ASConfig(BaseModel):
    ask_text: str = "Здравствуйте!\nПришлите ваш ID"
    grace_s: float = 60.0  # from the first message: an ID sent in this time is taken without asking
    wait_days: float = 7.0  # waiting for the ID stops after this
    history_messages: int = 500  # contact.second: how deep the history of the dialog is read
    history_days: float = 180.0
    # The record in `logs` of a typed ticket (D-TS-3). Fields: {id} {ticket} {player} {account} {trigger}.
    ticket_log_text: str = "TS: тикет #{id} — {ticket} · игрок {player} · аккаунт {account} · триггер: {trigger}"


class AutosupModule(Module):
    name: ClassVar[str] = NAME
    protocols: ClassVar[list[str]] = ["contact.first", "contact.second", *TS_api.PROTOCOLS]
    private_dialogs: ClassVar[bool] = True
    subscriptions: ClassVar[list[str]] = ["command.as"]
    commands: ClassVar[list[str]] = ["as"]
    config_model: ClassVar[type[BaseModel] | None] = ASConfig
    migrations: ClassVar[Path | None] = Path(__file__).parent / "AS_migrations"
    periodic_s: ClassVar[float | None] = 2.0  # ends of the 60 s grace and of the 7 days of waiting
    timeout_s: ClassVar[float] = 120.0  # contact.second reads the history through the limiter

    def __init__(self) -> None:
        self.enabled = True  # the switch /as on|off (as_settings), read at start

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    def cfg(self) -> ASConfig:
        mod = self.svc.runtime().config.modules.get(NAME)
        return ASConfig.model_validate(mod.config if mod is not None else {})

    async def start(self) -> None:
        self.enabled = bool(await self.svc.db.pool.fetchval("SELECT enabled FROM as_settings WHERE id = 1"))
        log.info("AS: Autosup is %s", "on" if self.enabled else "off")

    # ---- protocols (D-RM-12) ---------------------------------------------------------------------
    async def on_protocol(self, protocol: str, subject: int, data: dict[str, Any]) -> None:
        if protocol in TS_api.PROTOCOLS:
            await self._ticket_log(subject, data)
            return
        account_id, chat_id = int(data["account_id"]), int(data["chat_id"])
        if chat_id != subject or not self.enabled:
            return  # private dialogs only; switched off: nothing
        contact = protocol.removeprefix("contact.")
        if contact == "second":
            found = await self._search_history(account_id, subject)
            if found is not None:
                msg_id, fid = found
                await self._register(subject, account_id, contact, "history", msg_id, fid)
                return
        await self._grace(subject, account_id, contact, data.get("trace_id"))

    async def _grace(self, tg_id: int, account_id: int, contact: str, trace_id: Any) -> None:
        """Wait grace_s from the first message: the user may send the ID without being asked (D-AS-6)."""
        cfg = self.cfg()
        pool = self.svc.db.pool
        first = await pool.fetchval("SELECT msg_date FROM messages WHERE trace_id = $1::uuid", trace_id) \
            if trace_id else None
        start = first or datetime.now(UTC) - timedelta(seconds=5)
        await pool.execute(
            "INSERT INTO as_dialogs (tg_id, account_id, contact, state, contact_at, ask_at, expires_at) "
            "VALUES ($1, $2, $3, 'grace', $4::timestamptz, $4::timestamptz + make_interval(secs => $5::float8), "
            "now() + make_interval(secs => $6::float8)) ON CONFLICT (tg_id) DO NOTHING",
            tg_id, account_id, contact, start, cfg.grace_s, cfg.wait_days * 86400)

    async def _ask(self, tg_id: int, account_id: int) -> None:
        cfg = self.cfg()
        changed = await self.svc.db.pool.fetchval(
            "UPDATE as_dialogs SET state = 'awaiting_id', asked_at = now(), updated_at = now() "
            "WHERE tg_id = $1 AND state = 'grace' RETURNING tg_id", tg_id)
        if changed is None:
            return  # the ID came meanwhile, or the request is queued already
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
        return self._one_id([(ev.msg_id, ev.text) for ev in events if ev.sender_id == tg_id], tg_id)

    @staticmethod
    def _one_id(messages: list[tuple[int, str]], tg_id: int) -> tuple[int, FoundId] | None:
        found: dict[str, tuple[int, FoundId]] = {}
        for msg_id, text in messages:
            fid = find_id(text)
            if fid is not None:
                found.setdefault(fid.mel_id, (msg_id, fid))
        if len(found) != 1:
            if found:
                log.info("AS: %s sent %d different IDs: none is taken", tg_id, len(found))
            return None
        return next(iter(found.values()))

    # ---- events: private messages and /as ----------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if event.msg_type == "command.as":
            await self._command(event, ctx)
            return
        if rec.chat_id <= 0 or rec.sender_id != rec.chat_id or rec.origin != "external" or rec.event != "new":
            return
        await self._to_ts(rec)
        if not rec.text or not self.enabled:
            return
        row = await ctx.fetchrow(
            "SELECT account_id, contact, state, contact_at, asked_at FROM as_dialogs "
            "WHERE tg_id = $1 AND state IN ('grace', 'awaiting_id') AND expires_at > now()", rec.sender_id)
        if row is None:
            return
        since = row["asked_at"] if row["state"] == "awaiting_id" else row["contact_at"]
        if since is not None and rec.msg_date < since.timestamp() - 1:
            return  # written before the first message / the request
        fid = find_id(rec.text)
        if fid is None:
            return  # messages without an ID do not end the wait
        await self._register(rec.sender_id, rec.account_id, row["contact"], "reply", rec.msg_id, fid)

    # ---- the ticket system (D-TS-1, D-TS-3) -------------------------------------------------------
    async def _to_ts(self, rec: MessageRecord) -> None:
        """Hand a private message of a player to TS (staff and Telegram's service messages are not tickets)."""
        rt = self.svc.runtime()
        tg_id = int(rec.sender_id or 0)
        if (TS_api.config(rt) is None or rec.source == "backfill" or tg_id in SERVICE_USERS
                or rt.directory.user_role(tg_id) in STAFF):
            return  # history is never a ticket (D-TS-4); TS also drops messages written before it started
        pool = self.svc.db.pool
        media = await pool.fetchval("SELECT media_type FROM messages WHERE trace_id = $1::uuid LIMIT 1",
                                    rec.trace_id) if rec.trace_id else None
        msg = TS_api.PlayerMessage(tg_id=tg_id, account_id=rec.account_id, msg_id=rec.msg_id,
                                   msg_date=datetime.fromtimestamp(rec.msg_date, UTC), text=rec.text, media_type=media)
        if await TS_api.receive(pool, rt, msg):
            self.svc.wake_protocols()

    async def _ticket_log(self, ticket_id: int, data: dict[str, Any]) -> None:
        """A typed ticket -> one record in `logs` (D-TS-3); once per ticket and type (the dedup key)."""
        rt = self.svc.runtime()
        logs = rt.config.service_chats.logs
        if logs is None:
            return
        pool = self.svc.db.pool
        tg_id, account_id = int(data["tg_id"]), int(data["account_id"])
        user = await pool.fetchrow("SELECT username, player_id FROM rm_users WHERE tg_id = $1", tg_id)
        mels = [str(r["mel_id"]) for r in await pool.fetch(
            "SELECT mel_id FROM rm_player_mel WHERE player_id = $1 ORDER BY mel_id", user["player_id"])] \
            if user is not None and user["player_id"] is not None else []
        about = [f"@{user['username']}"] if user is not None and user["username"] else []
        about += [f"MelID {', '.join(mels)}"] if mels else []
        player = f"{tg_id} ({', '.join(about)})" if about else str(tg_id)
        account = await pool.fetchval("SELECT name FROM accounts WHERE account_id = $1", account_id) or account_id
        words = ", ".join(f"«{w}»" for w in data.get("trigger") or [])
        reason = data.get("reason")
        trigger = (f"{words} — депозит и вывод в одном сообщении" if reason == "mixed"
                   else words if words else "нет за первую минуту")
        text = self.cfg().ticket_log_text.format(id=ticket_id, ticket=data["ticket"], player=player, account=account,
                                                 trigger=trigger)
        await self._outbox(OutboundDraft(chat_id=logs, text=text, rule="ticket_log", kind="notify",
                                         dedup_key=f"{NAME}:ticket:{ticket_id}:{data['ticket']}"))

    async def _command(self, event: Event, ctx: Context) -> None:
        rec = event.record
        rt = self.svc.runtime()
        if rec.origin == "system":
            return
        if not can_run(rt, rec.sender_id, rec.chat_id, "as"):
            deny(self.svc.notify, rt, rec.sender_id, rec.chat_id, "as")  # the chat gets nothing (D-RM-8)
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        if rt.group_role(rec.chat_id) != SWITCH_ROLE:
            ctx.reply(f"Команда /as работает только в группе с ролью {SWITCH_ROLE}.", rule="command.as",
                      kind="command_reply")
            return
        arg = str((event.fields.get("args") or ["?"])[0]).lower()
        if arg in ("on", "off"):
            wanted = arg == "on"
            if wanted != self.enabled:
                await self.svc.db.pool.execute(
                    "UPDATE as_settings SET enabled = $1, updated_by = $2, updated_at = now() WHERE id = 1",
                    wanted, rec.sender_id)
                self.enabled = wanted
                self.svc.notify(f"{PREFIX}: Autosup {'включён' if wanted else 'выключен'}; автор {rec.sender_id}")
            ctx.reply(f"Autosup {'включён' if self.enabled else 'выключен'}.", rule="command.as", kind="command_reply")
            return
        waiting = await self.svc.db.pool.fetchval(
            "SELECT count(*) FROM as_dialogs WHERE state IN ('grace', 'awaiting_id')")
        done = await self.svc.db.pool.fetchval("SELECT count(*) FROM as_dialogs WHERE state = 'done'")
        ctx.reply(f"Autosup {'включён' if self.enabled else 'выключен'}. Ждут ID: {waiting}, получено ID: {done}.\n"
                  "/as on — включить, /as off — выключить.", rule="command.as", kind="command_reply")

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

    # ---- background: the end of the 60 s and of the 7 days -----------------------------------------
    async def periodic(self) -> None:
        pool = self.svc.db.pool
        due = await pool.fetch("SELECT tg_id, account_id, contact_at FROM as_dialogs "
                               "WHERE state = 'grace' AND ask_at <= now() ORDER BY ask_at LIMIT 50")
        for row in due:
            tg_id, account_id = int(row["tg_id"]), int(row["account_id"])
            if not self.enabled:
                await pool.execute("UPDATE as_dialogs SET state = 'expired', updated_at = now() WHERE tg_id = $1",
                                   tg_id)
                continue
            # An ID written in the 60 s whose message was handled before the wait began (the first message).
            messages = await pool.fetch(
                "SELECT msg_id, text FROM messages WHERE chat_id = $1 AND sender_id = $1 AND account_id = $2 "
                "AND event = 'new' AND msg_date >= $3::timestamptz - interval '1 second' AND text IS NOT NULL "
                "ORDER BY msg_date, msg_id", tg_id, account_id, row["contact_at"])
            found = self._one_id([(int(m["msg_id"]), str(m["text"])) for m in messages], tg_id)
            state = await pool.fetchval("SELECT state FROM as_dialogs WHERE tg_id = $1", tg_id)
            if state != "grace":
                continue  # taken meanwhile by a message
            if found is not None:
                msg_id, fid = found
                contact = await pool.fetchval("SELECT contact FROM as_dialogs WHERE tg_id = $1", tg_id)
                await self._register(tg_id, account_id, str(contact), "reply", msg_id, fid)
            else:
                await self._ask(tg_id, account_id)
        expired = await pool.execute(
            "UPDATE as_dialogs SET state = 'expired', updated_at = now() "
            "WHERE state IN ('grace', 'awaiting_id') AND expires_at <= now()")
        if expired and expired != "UPDATE 0":
            log.info("AS: waiting for the ID stopped: %s", expired)
