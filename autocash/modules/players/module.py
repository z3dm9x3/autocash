"""Module "players" (FT-И.1..И.5): who the player is across TGIDs and MelIDs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel

from autocash.actions.entities import report
from autocash.core.commands.rights import can_run
from autocash.core.reactions.engine import register_action
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, EntityDecl, Event, Module, Services
from autocash.modules.players import store

COMMANDS = ("player", "player_deposits", "merge_ok", "merge_no", "split")


class PlayersConfig(BaseModel):
    # OPEN-QUESTION: FT-И.3 — a join making a group larger than this waits for a staff decision.
    merge_confirm_threshold: int = 6
    deposits_days: int = 30


class PlayersModule(Module):
    name: ClassVar[str] = "players"
    migrations: ClassVar[Path | None] = Path(__file__).parent / "migrations"
    # Every message type carrying both identifiers (fields tgid and mel_id) adds a pair.
    subscriptions: ClassVar[list[str]] = ["deposit.*", "player.*", *(f"command.{c}" for c in COMMANDS)]
    entities: ClassVar[list[EntityDecl]] = [EntityDecl("player", "player_idents, player_merges, player_marks")]
    commands: ClassVar[list[str]] = list(COMMANDS)
    config_model: ClassVar[type[BaseModel] | None] = PlayersConfig
    # Joins are serialized by an advisory lock anyway; one worker keeps them in arrival order.
    concurrency: ClassVar[int] = 1

    def bind(self, services: Services) -> None:
        super().bind(services)
        # Actions for reaction rules (stage 3): the core calls them by name, knowing no players (P-1).
        register_action("resolve_player", self._resolve_action)
        register_action("player_mark", self._mark_action)

    async def _resolve_action(self, p: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
        """{ident} -> {status: one | ambiguous | none, player_id, ident: "kind:value", players}."""
        assert self.services is not None
        arg = str(p.get("ident") or "").strip()
        found = await store.resolve(self.services.db.pool, arg)
        if found.ambiguous:
            return {"status": "ambiguous", "players": found.ambiguous, "ident": arg}
        if found.player_id is None:
            kind, sep, _ = arg.partition(":")
            ident = arg if sep and kind in store.KINDS else f"tg:{arg}"
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
            return await store.add_mark(conn, str(p["kind"]), kind, value, author=str(event.get("sender_id")),
                                        text=p.get("text"), trace_id=event.get("trace_id"))

    def cfg(self, ctx: Context) -> PlayersConfig:
        return PlayersConfig.model_validate(ctx.config)

    async def handle(self, event: Event, ctx: Context) -> None:
        if event.msg_type.startswith("command."):
            await self._command(event, ctx)
            return
        f = event.fields
        tgid, mel_id = f.get("tgid"), f.get("mel_id")
        assert self.services is not None
        pool = self.services.db.pool
        if tgid and (f.get("username") or f.get("name")):
            async with pool.acquire() as conn:
                await store.note_name(conn, str(tgid), f.get("username"), f.get("name"))
        if not (tgid and mel_id):
            return
        async with pool.acquire() as conn:
            seen = await store.observe_pair(conn, str(tgid), str(mel_id), trace_id=event.trace_id,
                                            threshold=self.cfg(ctx).merge_confirm_threshold)
        if seen.new_pending:
            self.services.notify(
                f"Слияние игроков ждёт подтверждения (#{seen.pending}): TGID {tgid} и MelID {mel_id} "
                f"объединяют две крупные группы. /merge_ok {seen.pending} или /merge_no {seen.pending}")

    # ---- commands ----------------------------------------------------------------------------------
    async def _command(self, event: Event, ctx: Context) -> None:
        rec = event.record
        name = event.msg_type.removeprefix("command.")
        if name not in COMMANDS or not event.external:
            return
        assert self.services is not None
        if not can_run(self.services.runtime(), rec.sender_id, rec.chat_id, name):
            ctx.reply(f"Недостаточно прав для /{name}.", rule=f"command.{name}", kind="command_reply")
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        args = [str(a) for a in rec.fields.get("args") or []]
        author = str(rec.sender_id)
        try:
            text = await self.run(name, args, author, self.cfg(ctx))
        except ValueError as exc:
            text = str(exc)
        ctx.reply(text, rule=f"command.{name}", kind="command_reply")

    async def run(self, name: str, args: list[str], author: str, cfg: PlayersConfig) -> str:
        assert self.services is not None
        pool = self.services.db.pool
        if name in ("merge_ok", "merge_no", "split"):
            if len(args) != 1 or not args[0].isdigit():
                raise ValueError(f"Формат: /{name} <номер слияния>")
            async with pool.acquire() as conn:
                if name == "split":
                    result = await store.split(conn, int(args[0]), author=author)
                else:
                    result = await store.decide(conn, int(args[0]), accept=name == "merge_ok", author=author)
            return {
                "done": f"Слияние #{args[0]} выполнено.", "rejected": f"Слияние #{args[0]} отклонено.",
                "split": f"Слияние #{args[0]} разделено: идентификаторы вернулись в прежние группы.",
                "not_pending": f"Слияние #{args[0]} не ждёт решения.", "not_done": f"Слияние #{args[0]} не выполнено.",
                "gone": f"Группа слияния #{args[0]} уже объединена иначе.",
            }[result]
        if not args:
            raise ValueError(f"Формат: /{name} <TGID | MelID | tg:… | mel:…>")
        found = await store.resolve(self.services.db.reports, args[0])
        if found.ambiguous:
            # FT-12.5: two different players under the same number — ask which one is meant.
            return (f"«{args[0]}» есть у нескольких игроков (#{', #'.join(map(str, found.ambiguous))}). "
                    f"Уточните: tg:{args[0]} или mel:{args[0]}")
        if found.player_id is None:
            return f"Игрок «{args[0]}» не найден."
        ids = await store.idents(self.services.db.reports, found.player_id)
        if name == "player":
            return await self._card(found.player_id, ids)
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

    async def _card(self, player_id: int, ids: dict[str, list[str]]) -> str:
        assert self.services is not None
        db = self.services.db.reports
        names = await db.fetch("SELECT DISTINCT username, name FROM player_names WHERE tgid = ANY($1::text[])",
                               ids["tg"])
        marks = await db.fetch(
            "SELECT kind, author, created_at, text FROM player_marks WHERE (ident_kind = 'tg' AND ident = ANY($1)) "
            "OR (ident_kind = 'mel' AND ident = ANY($2)) ORDER BY created_at DESC LIMIT 20", ids["tg"], ids["mel"])
        lines = [f"Игрок #{player_id}", f"TGID: {', '.join(ids['tg']) or '—'}",
                 f"MelID: {', '.join(ids['mel']) or '—'}"]
        if names:
            lines.append("Имена: " + ", ".join(f"@{n['username']}" if n["username"] else n["name"] for n in names))
        if marks:
            lines.append("Отметки:")
            lines += [f"• {m['kind']} ({m['author']}, {m['created_at']:%d.%m.%Y})" + (f": {m['text']}" if m["text"]
                                                                                      else "") for m in marks]
        return "\n".join(lines)

