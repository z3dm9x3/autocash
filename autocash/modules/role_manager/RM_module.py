"""RM: the commands /grole and /rm ? (RM_readme.md).

Roles live in the configuration (`groups`, `group_roles`): the gateway decides what to store from
them even while PostgreSQL is down (D-RM-1). A change goes through the same editor as the web
admin: checked, versioned, journaled in `config_changes`, applied without a restart.

Feedback (AGENTS.md, rule 11): the answer goes to the chat of the command; the change is written
to the group `logs`; the group whose role changed gets nothing.
"""

from __future__ import annotations

from typing import Any, ClassVar

from autocash.core.commands.rights import can_run
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, Event, Module, Services
from autocash.modules.role_manager.RM_ids import RMIdError, bare, resolve

PREFIX = "RM"
USAGE = ("Использование:\n"
         "/grole <ID группы> <роль> — установить роль\n"
         "/grole <ID группы> — показать роль\n"
         "/grole <ID группы> -<роль> — снять роль (группа получит роль по умолчанию)\n"
         "/rm ? — в группе: роль этой группы")


class RoleManagerModule(Module):
    name: ClassVar[str] = "role_manager"
    subscriptions: ClassVar[list[str]] = ["command.grole", "command.rm"]
    commands: ClassVar[list[str]] = ["grole", "rm"]
    commands_any_chat: ClassVar[list[str]] = ["grole", "rm"]
    timeout_s: ClassVar[float] = 30.0  # a change writes files and reloads the configuration

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if rec.origin == "system":
            return  # never react to the system's own messages
        name = event.msg_type.removeprefix("command.")
        rt = self.svc.runtime()
        if not can_run(rt, rec.sender_id, rec.chat_id, name):
            ctx.reply(f"Недостаточно прав для /{name}.", rule=f"command.{name}", kind="command_reply")
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        args = [str(a) for a in event.fields.get("args", [])]
        text = self._rm(args, rec.chat_id) if name == "rm" else await self._grole(args, rec.sender_id or 0, ctx)
        ctx.reply(text, rule=f"command.{name}", kind="command_reply")

    # ---- /rm ? --------------------------------------------------------------------------------
    def _rm(self, args: list[str], chat_id: int) -> str:
        if args != ["?"]:
            return USAGE
        if chat_id > 0:
            return "/rm ? работает в группе: показывает роль этой группы."
        return f"Группа {bare(chat_id)}: {self._role_text(bare(chat_id))}"

    # ---- /grole -------------------------------------------------------------------------------
    async def _grole(self, args: list[str], author: int, ctx: Context) -> str:
        if not args or len(args) > 2:
            return USAGE
        try:
            group = resolve(args[0], await self._known(ctx))
        except RMIdError as exc:
            return f"Не выполнено: {exc}"
        if len(args) == 1:
            return f"Группа {group}: {self._role_text(group)}"
        cfg = self.svc.runtime().config
        current = cfg.groups.get(group)
        wanted = args[1]
        if wanted.startswith("-"):
            role = wanted[1:]
            if current != role:
                return f"Не выполнено: у группы {group} {self._role_text(group)}, а не {role}."
            new: str | None = None
        else:
            role = wanted
            if role not in cfg.group_roles:
                return f"Не выполнено: роли {role!r} нет. Роли групп: {', '.join(cfg.group_roles)}."
            if current == role:
                return f"Группа {group}: роль {role} уже установлена."
            new = role
        before = self._role_text(group)
        edit = self.svc.edit_config
        if edit is None:
            return "Не выполнено: изменение конфигурации недоступно."

        def mutate(raw: dict[str, Any]) -> None:
            groups = dict(raw.get("groups") or {})
            if new is None:
                groups.pop(group, None)
            else:
                groups[group] = new
            raw["groups"] = groups

        try:
            version = await edit(author, f"{PREFIX}: группа {group} -> {new or 'по умолчанию'}", mutate)
        except Exception as exc:  # the editor explains why (validation, a stale version, a file error)
            return f"Не выполнено: {exc}"
        after = self._role_text(group)
        # The group itself is not told (feedback rule): only the logs and the author of the command.
        self.svc.notify(f"{PREFIX}: группа {group}: {before} → {after}; автор {author}, версия {version}")
        return f"Группа {group}: {after} (было: {before})."

    def _role_text(self, group: int) -> str:
        cfg = self.svc.runtime().config
        role = cfg.groups.get(group)
        return f"роль {role}" if role is not None else f"роль {cfg.defaults.group_role} (по умолчанию)"

    async def _known(self, ctx: Context) -> set[int]:
        """Bare ids of the groups the system knows: where pool accounts are, and those with a role."""
        rt = self.svc.runtime()
        rows = await ctx.fetch("SELECT DISTINCT chat_id FROM chat_membership WHERE chat_id < 0")
        known = {bare(int(r["chat_id"])) for r in rows}
        known |= set(rt.config.groups)
        known |= {bare(c) for c in rt.sources if c < 0}
        return known
