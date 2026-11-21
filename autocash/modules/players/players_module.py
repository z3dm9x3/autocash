"""players: the module "players" - one person behind many TGIDs and MelIDs (players_readme.md, D-PLAYERS-1..4).

Pairs TGID <-> MelID come from two places:
- typed messages carrying both fields `tgid` and `mel_id` (kassa cards: deposit.*, withdrawal.*, player.*);
- other modules calling players_store.add_link directly (Autosup: the ID a user sent in private).
A pair whose TGID is staff (RM role) or a cashier (card field user_type CASHIER, ADMIN) is kept as a staff link
and joins nothing: staff create deposits for many players.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.actions.entities import report
from autocash.core.commands.rights import can_run, deny
from autocash.core.reactions.engine import register_action
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, EntityDecl, Event, Module, Services
from autocash.modules.players import players_store

log = logging.getLogger(__name__)

NAME = "players"
PREFIX = "players"
COMMANDS = ("player", "player_deposits", "unlink", "relink")
STAFF = frozenset({"casher", "support", "manager", "admin", "dev"})  # RM roles whose pairs join nothing


class PlayersConfig(BaseModel):
    deposits_days: int = 30
    # Card values of «Тип пользователя» that mark a cashier creating a deposit for a player.
    staff_user_types: list[str] = ["CASHIER", "ADMIN"]
    card_links: int = 40  # links listed in /player at most


class PlayersModule(Module):
    name: ClassVar[str] = NAME
    migrations: ClassVar[Path | None] = Path(__file__).parent / "players_migrations"
    subscriptions: ClassVar[list[str]] = ["deposit.*", "withdrawal.*", "player.*",
                                          *(f"command.{c}" for c in COMMANDS)]
    entities: ClassVar[list[EntityDecl]] = [
        EntityDecl("player", "players, players_links, players_nodes, players_journal")]
    commands: ClassVar[list[str]] = list(COMMANDS)
    config_model: ClassVar[type[BaseModel] | None] = PlayersConfig
    # Joins are serialized by an advisory lock anyway; one worker keeps them in arrival order.
    concurrency: ClassVar[int] = 1

    def bind(self, services: Services) -> None:
        super().bind(services)
        # Actions for reaction rules: the core calls them by name, knowing no players (P-1).
        register_action("resolve_player", self._resolve_action)
        register_action("player_mark", self._mark_action)

    async def start(self) -> None:
        assert self.services is not None
        healed = await players_store.heal(self.services.db.pool)
        if healed:
            log.info("%s: %d links got their players at start", PREFIX, healed)

    def cfg(self, ctx: Context) -> PlayersConfig:
        return PlayersConfig.model_validate(ctx.config)

    async def _resolve_action(self, p: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
        """{ident} -> {status: one | ambiguous | none, player_id, ident: "kind:value", players}."""
        assert self.services is not None
        arg = str(p.get("ident") or "").strip()
        found = await players_store.resolve(self.services.db.pool, arg)
        if found.ambiguous:
            return {"status": "ambiguous", "players": found.ambiguous, "ident": arg}
        if found.player_id is None:
            kind, sep, _ = arg.partition(":")
            ident = arg if sep and kind in players_store.KINDS else f"tg:{arg}"
            return {"status": "none", "ident": ident}
        return {"status": "one", "player_id": found.player_id, "ident": f"{found.kind}:{found.value}"}

    async def _mark_action(self, p: dict[str, Any], data: dict[str, Any]) -> bool:
        """A sanction or a mark with its author and time (FT-И.5); a repeat of the event adds nothing."""
        assert self.services is not None
        kind, _, value = str(p["ident"]).partition(":")
        if not value:
            kind, value = "tg", kind
        event = data.get("event", {})
        async with self.services.db.pool.acquire() as conn:
            return await players_store.add_mark(conn, str(p["kind"]), kind, value, author=str(event.get("sender_id")),
                                           text=p.get("text"), trace_id=event.get("trace_id"))

    async def handle(self, event: Event, ctx: Context) -> None:
        if event.msg_type.startswith("command."):
            await self._command(event, ctx)
            return
        f = event.fields
        tgid, mel_id = str(f.get("tgid") or "").strip(), str(f.get("mel_id") or "").strip()
        assert self.services is not None
        pool = self.services.db.pool
        if tgid.isdigit() and (f.get("username") or f.get("name")):
            async with pool.acquire() as conn:
                await players_store.note_name(conn, int(tgid), f.get("username"), f.get("name"))
        if not (tgid.isdigit() and mel_id):
            return
        cfg = self.cfg(ctx)
        role = self.services.runtime().directory.user_role(int(tgid))
        staff = role in STAFF or str(f.get("user_type") or "").upper() in cfg.staff_user_types
        await players_store.add_link(pool, int(tgid), mel_id, source=f"msg:{event.msg_type}", ref=event.trace_id,
                                staff=staff, at=datetime.fromtimestamp(event.record.msg_date, UTC))

    # ---- commands ----------------------------------------------------------------------------------
    async def _command(self, event: Event, ctx: Context) -> None:
        rec = event.record
        name = event.msg_type.removeprefix("command.")
        if name not in COMMANDS or not event.external:
            return
        assert self.services is not None
        if not can_run(self.services.runtime(), rec.sender_id, rec.chat_id, name):
            deny(self.services.notify, self.services.runtime(), rec.sender_id, rec.chat_id, name)
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        args = [str(a) for a in rec.fields.get("args") or []]
        try:
            text = await self.run(name, args, str(rec.sender_id), self.cfg(ctx))
        except ValueError as exc:
            text = str(exc)
        ctx.reply(text, rule=f"command.{name}", kind="command_reply")

    async def run(self, name: str, args: list[str], author: str, cfg: PlayersConfig) -> str:
        assert self.services is not None
        if name in ("unlink", "relink"):
            return await self._set_link(name, args, author)
        if not args:
            raise ValueError(f"Формат: /{name} <TGID | MelID | tg:… | mel:… | #игрок>")
        found = await players_store.resolve(self.services.db.reports, args[0])
        if found.ambiguous:
            # FT-12.5: two different players under the same number - ask which one is meant.
            return (f"«{args[0]}» есть у нескольких игроков (#{', #'.join(map(str, found.ambiguous))}). "
                    f"Уточните: tg:{args[0]} или mel:{args[0]}")
        if found.player_id is None:
            return f"Игрок «{args[0]}» не найден."
        ids = await players_store.idents(self.services.db.reports, found.player_id)
        if name == "player":
            return await self._card(found.player_id, ids, cfg)
        days = int(args[1]) if len(args) > 1 and args[1].isdigit() else cfg.deposits_days
        since = datetime.now(UTC) - timedelta(days=days)
        try:
            rows = await report(self.services.db.reports, "deposits.by_idents", tgids=ids["tg"], mel_ids=ids["mel"],
                                since=since)
        except KeyError:
            return "Модуль заявок не подключён: депозитов нет."
        lines = [f"Депозиты игрока #{found.player_id} за {days} дн.: {len(rows)}"]
        lines += [f"• {r['deposit_id']} касса {r['chat_id']}: {r['amount'] or '—'}, {r['status']}"
                  for r in rows[:40]]
        return "\n".join(lines)

    async def _set_link(self, name: str, args: list[str], author: str) -> str:
        assert self.services is not None
        if len(args) != 2 or not args[0].isdigit():
            raise ValueError(f"Формат: /{name} <TGID> <MelID>")
        tg_id, mel_id = int(args[0]), args[1]
        state = "disabled" if name == "unlink" else "active"
        result = await players_store.set_state(self.services.db.pool, tg_id, mel_id, state, author=author)
        if result == "missing":
            return f"Связи TGID {tg_id} — MelID {mel_id} нет."
        if result == "same":
            return f"Связь TGID {tg_id} — MelID {mel_id} уже {'разорвана' if state == 'disabled' else 'активна'}."
        pt = await players_store.player_of(self.services.db.pool, "tg", tg_id)
        pm = await players_store.player_of(self.services.db.pool, "mel", mel_id)
        where = (f"игрок #{pt}" if pt == pm else
                 f"TGID — игрок #{pt or '—'}, MelID — игрок #{pm or '—'}")
        verb = "разорвана" if state == "disabled" else "восстановлена"
        self.services.notify(f"{PREFIX}: связь TGID {tg_id} — MelID {mel_id} {verb}; {where}; автор {author}")
        return f"Связь TGID {tg_id} — MelID {mel_id} {verb}: {where}."

    async def _card(self, player_id: int, ids: dict[str, list[str]], cfg: PlayersConfig) -> str:
        assert self.services is not None
        db = self.services.db.reports
        tgs = [int(t) for t in ids["tg"]]
        names = await db.fetch("SELECT DISTINCT username, name FROM players_names WHERE tg_id = ANY($1::bigint[])", tgs)
        marks = await db.fetch(
            "SELECT kind, author, created_at, text FROM players_marks WHERE (ident_kind = 'tg' AND ident = ANY($1)) "
            "OR (ident_kind = 'mel' AND ident = ANY($2)) ORDER BY created_at DESC LIMIT 20", ids["tg"], ids["mel"])
        links = await players_store.links_of(db, tgs, ids["mel"])
        lines = [f"Игрок #{player_id}: TGID {len(ids['tg'])}, MelID {len(ids['mel'])}",
                 f"TGID: {_short(ids['tg'])}", f"MelID: {_short(ids['mel'])}"]
        if names:
            lines.append("Имена: " + ", ".join(f"@{n['username']}" if n["username"] else n["name"] for n in names))
        other = [r for r in links if r["state"] != "active"]
        if other:
            label = {"staff": "персонал", "disabled": "разорвана"}
            lines.append("Неактивные связи:")
            lines += [f"• {r['tg_id']} — {r['mel_id']}: {label[r['state']]}" for r in other[:cfg.card_links]]
        if marks:
            lines.append("Отметки:")
            lines += [f"• {m['kind']} ({m['author']}, {m['created_at']:%d.%m.%Y})" + (f": {m['text']}" if m["text"]
                                                                                      else "") for m in marks]
        return "\n".join(lines)


def _short(values: list[str], limit: int = 30) -> str:
    """A long list (a player may have thousands of MelIDs) is cut with a count of the rest."""
    if not values:
        return "—"
    head = ", ".join(values[:limit])
    return head if len(values) <= limit else f"{head} … и ещё {len(values) - limit}"
