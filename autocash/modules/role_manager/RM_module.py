"""RM: roles of users and groups; commands /grole, /urole, /rm (RM_readme.md).

The roles live in PostgreSQL (tables rm_*, D-RM-7). The core keeps them in memory, with a local
snapshot, in its Directory: RM fills it at start and after every change, so the gateway and the
rights work while PostgreSQL is down.

Feedback (AGENTS.md, rule 9): the answer goes to the chat of the command; a change is written to
the group `logs`; the user or group whose role changed is told nothing. A command without rights
is not answered in the chat: the refusal goes to `logs` (D-RM-8).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar

from autocash.core.commands.rights import can_run, deny
from autocash.core.config.loader import Runtime
from autocash.core.directory import GUEST
from autocash.core.record import MessageRecord
from autocash.core.trace.codes import Reason, Status
from autocash.modules.api import Context, Event, Module, Services
from autocash.modules.role_manager import RM_store as store
from autocash.modules.role_manager.RM_ids import RMIdError, bare, resolve

log = logging.getLogger(__name__)

PREFIX = "RM"
SERVICE_USERS = frozenset({777000})  # Telegram's own service notifications
USAGE = ("Использование:\n"
         "/grole <ID группы> <роль> — роль группы; /grole <ID группы> — показать\n"
         "/urole <TGID> <роль> — роль пользователя; /urole <TGID> — показать\n"
         "/rm set <ID> <роль> — то же: по роли понятно, группа это или пользователь\n"
         "/rm ? — в группе: роль этой группы")


class RoleManagerModule(Module):
    name: ClassVar[str] = "role_manager"
    subscriptions: ClassVar[list[str]] = ["command.grole", "command.urole", "command.rm"]
    commands: ClassVar[list[str]] = ["grole", "urole", "rm"]
    commands_any_chat: ClassVar[list[str]] = ["grole", "urole", "rm"]
    migrations: ClassVar[Path | None] = Path(__file__).parent / "RM_migrations"
    private_dialogs: ClassVar[bool] = True  # every private message: contacts (D-RM-11)

    def __init__(self, users: dict[int, str] | None = None) -> None:
        # Users written into rm_users at start if they are not there (tests and tools); the role of a
        # user who is already there is not changed. Production uses RM_cli or /urole.
        self._seed = dict(users or {})

    @property
    def svc(self) -> Services:
        assert self.services is not None, "the module is not bound to the App"
        return self.services

    def rt(self) -> Runtime:
        rt: Runtime = self.svc.runtime()
        return rt

    # ---- start: PostgreSQL -> the Directory of the core ----------------------------------------
    async def start(self) -> None:
        if self._seed:
            added = await store.add_users(self.svc.db.pool, self._seed, source="seed")
            if added:
                log.info("RM: users added at start: %s", added)
        await self.reload()

    async def reload(self) -> None:
        roles, permissions, users, groups = await store.load(self.svc.db.pool)
        self.rt().directory.replace(roles, permissions, users, groups)

    # ---- events --------------------------------------------------------------------------------
    async def handle(self, event: Event, ctx: Context) -> None:
        rec = event.record
        if rec.origin == "system":
            return  # never react to the system's own messages
        if rec.chat_id > 0:
            await self._contact(rec)  # RM first: the user is in the base before any other module acts
        name = event.msg_type.removeprefix("command.")
        if not event.msg_type.startswith("command.") or name not in self.commands:
            return
        rt = self.rt()
        if not can_run(rt, rec.sender_id, rec.chat_id, name):
            deny(self.svc.notify, rt, rec.sender_id, rec.chat_id, name)  # the chat gets nothing (D-RM-8)
            ctx.finish(Status.CANCELLED, Reason.ACCESS_DENIED)
            return
        args = [str(a) for a in event.fields.get("args", [])]
        author = rec.sender_id or 0
        if name == "grole":
            text = await self._group(args, author)
        elif name == "urole":
            text = await self._user(args, author)
        else:
            text = await self._rm(args, rec.chat_id, author)
        ctx.reply(text, rule=f"command.{name}", kind="command_reply")

    async def _rm(self, args: list[str], chat_id: int, author: int) -> str:
        if args == ["?"]:
            if chat_id > 0:
                return "/rm ? работает в группе: показывает роль этой группы."
            return f"Группа {bare(chat_id)}: {self._group_text(bare(chat_id))}"
        if len(args) == 3 and args[0].lower() == "set":
            role = self.rt().directory.roles.get(args[2].lower())
            if role is None:
                return f"Не выполнено: роли {args[2]!r} нет. {self._roles_text()}"
            if role.scope == "user":
                return await self._user(args[1:], author)
            return await self._group(args[1:], author)
        return USAGE

    # ---- contacts: the first private message of a user who is not in the base ------------------
    async def _contact(self, rec: MessageRecord) -> None:
        """A user absent from rm_users wrote to a pool account in private (D-RM-11).

        No history with any pool account -> protocol contact.first; there was a history (the user wrote
        before the monitor knew them) -> contact.second. Either runs once: the user is written in the
        same transaction, and a user who is in the base gets no protocol at all.
        """
        tg_id = rec.sender_id
        if rec.origin != "external" or rec.event != "new" or tg_id is None or tg_id in SERVICE_USERS:
            return
        if tg_id != rec.chat_id or tg_id in self.rt().directory.users:
            return  # not the user's own message in their dialog, or a known user: no protocol
        history = await self._had_history(rec)
        protocol = "contact.second" if history else "contact.first"
        data = {"account_id": rec.account_id, "chat_id": rec.chat_id, "msg_id": rec.msg_id, "trace_id": rec.trace_id}
        added = await store.add_contact(self.svc.db.pool, tg_id, protocol, data)
        if added:
            self.rt().directory.set_user(tg_id, GUEST)
            self.svc.wake_protocols()
            log.info("RM: new user %s (%s via account %s)", tg_id, protocol, rec.account_id)
        else:
            # Written meanwhile (another account, the importer): take the role from the base.
            found = await store.user(self.svc.db.pool, tg_id)
            if found is not None:
                self.rt().directory.set_user(tg_id, str(found["role"]))

    async def _had_history(self, rec: MessageRecord) -> bool:
        """Messages of this dialog before this one: in our base first, then one getHistory to Telegram."""
        if await store.dialog_seen(self.svc.db.pool, rec.chat_id, rec.ingest_id):
            return True
        check = self.svc.dialog_history
        result = await check(rec.account_id, rec.chat_id, rec.msg_id) if check is not None else None
        # Unknown (FloodWait, an error): take "there was a history". A first-contact greeting to a person
        # who already talked to us is worse than a missed one.
        return True if result is None else result

    # ---- users ---------------------------------------------------------------------------------
    async def _user(self, args: list[str], author: int) -> str:
        if not args or len(args) > 2:
            return USAGE
        try:
            tg_id = int(args[0])
        except ValueError:
            return f"Не выполнено: TGID — число, а не {args[0]!r}."
        directory = self.rt().directory
        found = await store.user(self.svc.db.pool, tg_id)
        if found is None:
            return f"Не выполнено: пользователя {tg_id} нет в базе пользователей."
        if len(args) == 1:
            return f"Пользователь {tg_id}: роль {found['role']}"
        role = args[1].lower()
        if role not in directory.roles_of("user"):
            return f"Не выполнено: роли пользователя {args[1]!r} нет. Роли: {', '.join(directory.roles_of('user'))}."
        if found["role"] == role:
            return f"Пользователь {tg_id}: роль {role} уже установлена."
        before = await store.set_user_role(self.svc.db.pool, tg_id, role, author)
        directory.set_user(tg_id, role)
        # The user is not told (feedback rule): only the logs and the author of the command.
        self.svc.notify(f"{PREFIX}: пользователь {tg_id}: роль {before} → {role}; автор {author}")
        return f"Пользователь {tg_id}: роль {role} (было: {before})."

    # ---- groups --------------------------------------------------------------------------------
    async def _group(self, args: list[str], author: int) -> str:
        if not args or len(args) > 2:
            return USAGE
        try:
            group = resolve(args[0], await self._known())
        except RMIdError as exc:
            return f"Не выполнено: {exc}"
        if len(args) == 1:
            return f"Группа {group}: {self._group_text(group)}"
        directory = self.rt().directory
        wanted = args[1].lower()
        removing = wanted.startswith("-")
        role = wanted[1:] if removing else wanted
        if role not in directory.roles_of("group"):
            return f"Не выполнено: роли группы {role!r} нет. Роли: {', '.join(directory.roles_of('group'))}."
        current = directory.group_role(group)
        if removing:
            if current != role:
                return f"Не выполнено: у группы {group} {self._group_text(group)}, а не {role}."
            role = "other"
        if current == role:
            return f"Группа {group}: роль {role} уже установлена."
        before_text = self._group_text(group)
        await store.set_group_role(self.svc.db.pool, group, role, author)
        directory.set_group(group, role)
        # The group itself is not told (feedback rule): only the logs and the author of the command.
        self.svc.notify(f"{PREFIX}: группа {group}: {before_text} → роль {role}; автор {author}")
        return f"Группа {group}: роль {role} (было: {before_text})."

    def _group_text(self, group: int) -> str:
        role = self.rt().directory.group_role(group)
        return f"роль {role}" if role is not None else "роль other (по умолчанию)"

    def _roles_text(self) -> str:
        directory = self.rt().directory
        return (f"Роли пользователей: {', '.join(directory.roles_of('user'))}; "
                f"роли групп: {', '.join(directory.roles_of('group'))}.")

    async def _known(self) -> set[int]:
        """Bare ids of the groups the system knows: where pool accounts are, with a role, in `sources`."""
        rt = self.rt()
        known = {bare(c) for c in await store.member_groups(self.svc.db.pool)}
        known |= set(rt.directory.groups)
        known |= {bare(c) for c in rt.sources if c < 0}
        return known
