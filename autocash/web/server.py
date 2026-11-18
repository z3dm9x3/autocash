"""The web admin (stage 5, FT-13.5; docs/decisions.md D-5-1..D-5-6).

A small aiohttp server inside the Autocash process: it reads PostgreSQL through the reports
pool (statement timeout, NFT-15), shows the same texts as the commands /status, /trace,
/accounts (they are produced by the same code), edits the configuration through
`ConfigEditor` and sends messages only through the outbound queue (the Output keeps the pace
and the limits). It listens on 127.0.0.1 by default (NFT-20); every change is in `admin_audit`.
"""

from __future__ import annotations

import json
import logging
import secrets
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import asyncpg
import jinja2
from aiohttp import web

from autocash.core.commands.service import CommandError
from autocash.core.outbox.OBX_tasks import OutboundDraft, insert_outbound
from autocash.web.auth import (
    WEB_EDIT,
    WEB_POOL,
    WEB_READ,
    WEB_SEND,
    LoginThrottle,
    check_password,
    token_hash,
    web_rights,
)
from autocash.web.editor import ConfigEditor, EditError, dump

if TYPE_CHECKING:
    from autocash.core.app import App

log = logging.getLogger(__name__)

COOKIE = "autocash_admin"
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]
USER = web.RequestKey("user", dict)


class WebAdmin:
    def __init__(self, app: App) -> None:
        self.app = app
        self.editor = ConfigEditor(app)
        self.throttle = LoginThrottle()
        self.port: int | None = None
        self._runner: web.AppRunner | None = None
        self.jinja = jinja2.Environment(
            loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
            autoescape=True, undefined=jinja2.StrictUndefined, trim_blocks=True, lstrip_blocks=True,
        )
        self.jinja.filters["ts"] = self._ts

    # ---- lifecycle ---------------------------------------------------------------------------------
    def build(self) -> web.Application:
        wa = web.Application(middlewares=[self._auth], client_max_size=2 * 1024 * 1024)
        r = wa.router
        r.add_get("/login", self.login_page)
        r.add_post("/login", self.login)
        r.add_post("/logout", self.logout)
        r.add_get("/", self.overview)
        r.add_get("/pool", self.pool_page)
        r.add_post("/pool/action", self.pool_action)
        r.add_get("/trace", self.trace_page)
        r.add_get("/stuck", self.stuck_page)
        r.add_get("/metrics", self.metrics_page)
        r.add_get("/messages", self.messages_page)
        r.add_get("/send", self.send_page)
        r.add_post("/send", self.send)
        r.add_get("/config", self.config_page)
        r.add_post("/config/toggle", self.config_toggle)
        r.add_post("/config/source", self.config_source)
        r.add_post("/config/source/delete", self.config_source_delete)
        r.add_get("/config/section", self.section_page)
        r.add_post("/config/section", self.section_save)
        r.add_get("/dryrun", self.dryrun_page)
        r.add_get("/audit", self.audit_page)
        return wa

    async def start(self) -> None:
        cfg = self.app.store.current.config.web
        self._runner = web.AppRunner(self.build(), access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, cfg.host, cfg.port)
        await site.start()
        server = site._server
        sockets = getattr(server, "sockets", None) or []
        self.port = sockets[0].getsockname()[1] if sockets else cfg.port
        log.info("web admin on http://%s:%s", cfg.host, self.port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    # ---- helpers -------------------------------------------------------------------------------------
    def _ts(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            value = datetime.fromtimestamp(value, UTC)
        tz = ZoneInfo(self.app.store.current.config.report_timezone)
        return str(value.astimezone(tz).strftime("%d.%m %H:%M:%S"))

    def render(self, request: web.Request, template: str, **ctx: Any) -> web.Response:
        user = request.get(USER)
        body = self.jinja.get_template(template).render(
            user=user, rights=user["rights"] if user else set(), csrf=user["csrf"] if user else "",
            version=self.app.store.current.version, path=request.path, flash=request.query.get("msg", ""),
            error=ctx.pop("error", request.query.get("err", "")), **ctx)
        return web.Response(text=body, content_type="text/html")

    @staticmethod
    def back(location: str, *, msg: str = "", err: str = "") -> web.HTTPFound:
        query = "&".join(f"{k}={v}" for k, v in (("msg", _q(msg)), ("err", _q(err))) if v)
        return web.HTTPFound(location + (("&" if "?" in location else "?") + query if query else ""))

    async def audit(self, author: int, action: str, target: str | None, ok: bool, detail: dict[str, Any]) -> None:
        await self.app.db.pool.execute(
            "INSERT INTO admin_audit (author, action, target, ok, detail) VALUES ($1, $2, $3, $4, $5::jsonb)",
            author, action, target, ok, json.dumps(detail, ensure_ascii=False, default=str))

    async def command(self, name: str, args: list[str]) -> str:
        """The same text as the command in Telegram: the same code builds it."""
        try:
            return await self.app.commands.run(name, args)
        except CommandError as exc:
            return str(exc)

    # ---- access ---------------------------------------------------------------------------------------
    @web.middleware
    async def _auth(self, request: web.Request, handler: Handler) -> web.StreamResponse:
        if request.path == "/login":
            return await handler(request)
        token = request.cookies.get(COOKIE)
        row = None
        if token:
            row = await self.app.db.pool.fetchrow(
                "SELECT s.tg_id, s.csrf FROM web_sessions s JOIN web_users u USING (tg_id) "
                "WHERE s.token_hash = $1 AND s.expires_at > now() AND NOT u.disabled", token_hash(token))
        if row is None:
            if request.method == "GET":
                raise web.HTTPFound("/login")
            raise web.HTTPForbidden(text="Нужен вход")
        rights = web_rights(self.app.store.current, row["tg_id"])
        if WEB_READ not in rights:
            raise web.HTTPForbidden(text="Нет прав на админку: нужна роль admin или dev (матрица permissions)")
        request[USER] = {"tg_id": row["tg_id"], "csrf": row["csrf"], "rights": rights}
        if request.method == "POST":
            form = await request.post()
            if not secrets.compare_digest(str(form.get("csrf", "")), row["csrf"]):
                raise web.HTTPForbidden(text="Форма устарела (CSRF): обновите страницу")
        return await handler(request)

    @staticmethod
    def need(request: web.Request, right: str) -> int:
        user = request[USER]
        if right not in user["rights"]:
            raise web.HTTPForbidden(text=f"Нет права {right}")
        return int(user["tg_id"])

    async def login_page(self, request: web.Request) -> web.Response:
        return self.render(request, "login.html")

    async def login(self, request: web.Request) -> web.StreamResponse:
        form = await request.post()
        peer = request.remote or "?"
        raw_id = str(form.get("tg_id", "")).strip()
        password = str(form.get("password", ""))
        if self.throttle.blocked(f"ip:{peer}", f"id:{raw_id}"):
            return self.render(request, "login.html", error="Слишком много попыток, подождите 5 минут")
        tg_id = int(raw_id) if raw_id.lstrip("-").isdigit() else None
        row = await self.app.db.pool.fetchrow(
            "SELECT password_hash FROM web_users WHERE tg_id = $1 AND NOT disabled", tg_id) if tg_id else None
        if row is None or not check_password(password, row["password_hash"]):
            self.throttle.fail(f"ip:{peer}", f"id:{raw_id}")
            return self.render(request, "login.html", error="Неверный ID или пароль")
        assert tg_id is not None
        if WEB_READ not in web_rights(self.app.store.current, tg_id):
            return self.render(request, "login.html", error="Нет прав на админку: нужна роль admin или dev")
        self.throttle.ok(f"ip:{peer}", f"id:{raw_id}")
        token = secrets.token_urlsafe(32)
        hours = self.app.store.current.config.web.session_hours
        await self.app.db.pool.execute(
            "DELETE FROM web_sessions WHERE expires_at < now()")
        await self.app.db.pool.execute(
            "INSERT INTO web_sessions (token_hash, tg_id, csrf, expires_at) VALUES ($1, $2, $3, $4)",
            token_hash(token), tg_id, secrets.token_urlsafe(24), datetime.now(UTC) + timedelta(hours=hours))
        await self.audit(tg_id, "login", None, True, {"ip": peer})
        resp = web.Response(status=302, headers={"Location": "/"})
        resp.set_cookie(COOKIE, token, httponly=True, samesite="Strict", max_age=int(hours * 3600),
                        secure=self.app.store.current.config.web.secure_cookie)
        return resp

    async def logout(self, request: web.Request) -> web.StreamResponse:
        token = request.cookies.get(COOKIE, "")
        await self.app.db.pool.execute("DELETE FROM web_sessions WHERE token_hash = $1", token_hash(token))
        resp = web.Response(status=302, headers={"Location": "/login"})
        resp.del_cookie(COOKIE)
        return resp

    # ---- reading ----------------------------------------------------------------------------------------
    async def overview(self, request: web.Request) -> web.Response:
        backfill = await self.app.backfill.progress()
        return self.render(request, "overview.html", status=await self.command("status", []),
                           queue=await self.command("queue", []), coverage=await self.command("coverage", []),
                           backfill=backfill)

    async def pool_page(self, request: web.Request) -> web.Response:
        return self.render(request, "pool.html", accounts=await self.command("accounts", []),
                           rows=self.app.commands.account_rows(), coverage=await self.command("coverage", []),
                           sessions=self.app.sessions is not None)

    async def trace_page(self, request: web.Request) -> web.Response:
        q = request.query
        text = await self.command("trace", q.get("q", "").split()) if q.get("q") else ""
        filters = {k: q.get(k, "").strip() for k in ("chat", "module", "account", "reason", "status", "hours")}
        rows = await self._traces(filters) if any(filters.values()) or not q.get("q") else []
        return self.render(request, "trace.html", text=text, q=q.get("q", ""), f=filters, rows=rows)

    async def _traces(self, f: dict[str, str]) -> list[asyncpg.Record]:
        hours = float(f["hours"]) if _num(f["hours"]) else 24.0
        where = ["t.accepted_at >= now() - make_interval(secs => $1::float8)"]
        args: list[Any] = [hours * 3600]

        def add(sql: str, value: Any) -> None:
            args.append(value)
            where.append(sql.format(len(args)))

        if _num(f["chat"]):
            add("t.chat_id = ${}", int(f["chat"]))
        if _num(f["account"]):
            add("t.account_id = ${}", int(f["account"]))
        if f["reason"]:
            add("(t.reason_code = ${0} OR m.reason_code = ${0})", f["reason"])
        if f["status"]:
            add("coalesce(m.status, t.status) = ${}", f["status"])
        if f["module"]:
            add("m.module = ${}", f["module"])
        args.append(self.app.store.current.config.web.page_limit)
        sql = ("SELECT t.trace_id, t.accepted_at, t.chat_id, t.msg_id, t.account_id, t.msg_type, t.status, "
               "t.reason_code, m.module, m.status AS m_status, m.reason_code AS m_reason, "
               "extract(epoch FROM m.finished_at - t.accepted_at) AS total_s "
               "FROM trace_inbound t LEFT JOIN trace_module m USING (trace_id) "
               f"WHERE {' AND '.join(where)} ORDER BY t.accepted_at DESC LIMIT ${len(args)}")
        rows: list[asyncpg.Record] = await self.app.db.reports.fetch(sql, *args)
        return rows

    async def stuck_page(self, request: web.Request) -> web.Response:
        stage = request.query.get("stage", "").strip()
        return self.render(request, "text.html", title="Зависшие задачи (/stuck)",
                           text=await self.command("stuck", [stage] if stage else []))

    async def metrics_page(self, request: web.Request) -> web.Response:
        args = request.query.get("args", "").split()
        return self.render(request, "text.html", title="Метрики этапов (/metrics)",
                           text=await self.command("metrics", args))

    async def messages_page(self, request: web.Request) -> web.Response:
        q = request.query
        where: list[str] = ["true"]
        args: list[Any] = []
        if _num(q.get("chat", "")):
            args.append(int(q["chat"]))
            where.append(f"chat_id = ${len(args)}")
        if q.get("text"):
            args.append("%" + q["text"].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
            where.append(f"text ILIKE ${len(args)}")
        if q.get("type"):
            args.append(q["type"])
            where.append(f"msg_type = ${len(args)}")
        if q.get("source"):
            args.append(q["source"])
            where.append(f"source = ${len(args)}")
        args.append(self.app.store.current.config.web.page_limit)
        error = ""
        rows: list[asyncpg.Record] = []
        try:
            rows = await self.app.db.reports.fetch(
                "SELECT chat_id, msg_id, version, msg_date, sender_id, sender_role, msg_type, source, text, "
                f"trace_id FROM messages WHERE {' AND '.join(where)} ORDER BY ingest_id DESC LIMIT ${len(args)}",
                *args)
        except asyncpg.QueryCanceledError:
            error = "Запрос не уложился в лимит времени отчётов: сузьте фильтр (чат, тип)"
        names = {s.chat_id: s.name for s in self.app.store.current.config.sources}
        return self.render(request, "messages.html", rows=rows, q=q, names=names, error=error)

    async def audit_page(self, request: web.Request) -> web.Response:
        limit = self.app.store.current.config.web.page_limit
        audit = await self.app.db.reports.fetch("SELECT * FROM admin_audit ORDER BY id DESC LIMIT $1", limit)
        changes = await self.app.db.reports.fetch("SELECT * FROM config_changes ORDER BY id DESC LIMIT 50")
        return self.render(request, "audit.html", audit=audit, changes=changes)

    # ---- sending through the outbound queue ------------------------------------------------------
    async def send_page(self, request: web.Request) -> web.Response:
        self.need(request, WEB_SEND)
        return self.render(request, "send.html", sources=self.app.store.current.config.sources,
                           chat=request.query.get("chat", ""), reply_to=request.query.get("reply_to", ""))

    async def send(self, request: web.Request) -> web.StreamResponse:
        author = self.need(request, WEB_SEND)
        form = await request.post()
        chat, text, reply = str(form.get("chat", "")).strip(), str(form.get("text", "")).strip(), \
            str(form.get("reply_to", "")).strip()
        if not _num(chat) or not text or (reply and not _num(reply)):
            raise self.back("/send", err="Нужны чат (число), текст и, если есть, номер сообщения для ответа")
        if len(text) > 4096:
            raise self.back("/send", err="Текст длиннее 4096 символов")
        rt = self.app.store.current
        draft = OutboundDraft(chat_id=int(chat), text=text, rule="web.send", kind="notify",
                              reply_to=int(reply) if reply else None, dedup_key=f"web:{uuid.uuid4()}")
        async with self.app.db.pool.acquire() as conn:
            task_id = await insert_outbound(conn, draft, parent_trace_id=None, module="web",
                                            ttl_s=rt.config.outbox.ttl_s.get("notify", 3600.0),
                                            config_version=rt.version)
        if task_id:
            self.app.outbox.enqueue([task_id])
        await self.audit(author, "send", chat, bool(task_id), {"task_id": task_id, "trace_id": draft.trace_id,
                                                                "reply_to": reply or None, "text": text[:500]})
        raise self.back("/send", msg=f"Поставлено в очередь отправки: задача {task_id}, трасса {draft.trace_id}")

    # ---- pool -------------------------------------------------------------------------------------------
    async def pool_action(self, request: web.Request) -> web.StreamResponse:
        author = self.need(request, WEB_POOL)
        form = await request.post()
        action = str(form.get("action", ""))
        account = int(str(form.get("account", "0"))) if _num(str(form.get("account", ""))) else 0
        pool = self.app.pool
        if action == "scan":
            if self.app.sessions is None:
                raise self.back("/pool", err="Каталог сессий не настроен (pool.sessions_dir)")
            await self.app.sessions.scan()
            msg = "Каталог сессий просмотрен"
        elif account not in pool.accounts:
            raise self.back("/pool", err=f"Аккаунт {account} не найден")
        elif action == "remove":
            await pool.remove(account, f"выведен из админки (автор {author})")
            msg = f"Аккаунт {account} выведен из пула"
        elif action in ("restrict", "unrestrict"):
            pool.set_restricted(account, action == "restrict", f"из админки (автор {author})")
            msg = f"Аккаунт {account}: ограничение {'включено' if action == 'restrict' else 'снято'}"
        else:
            raise self.back("/pool", err=f"Неизвестное действие {action}")
        await self.audit(author, f"pool.{action}", str(account or ""), True, {})
        raise self.back("/pool", msg=msg)

    # ---- configuration ------------------------------------------------------------------------------
    async def config_page(self, request: web.Request) -> web.Response:
        cfg = self.app.store.current.config
        known = sorted(set(cfg.modules) | {m.name for m in self.app.modules if m.name != "system"})
        return self.render(request, "config.html", cfg=cfg, modules=known,
                           sections=sorted(type(cfg).model_fields), files=[f.name for f in self.editor.files()])

    async def _edit(self, request: web.Request, summary: str, mutate: Callable[[dict[str, Any]], None],
                    back: str = "/config") -> web.StreamResponse:
        author = self.need(request, WEB_EDIT)
        form = await request.post()
        try:
            result = await self.editor.apply(author, summary, mutate, str(form.get("base_version", "")) or None)
        except EditError as exc:
            await self.audit(author, "config", summary, False, {"error": str(exc)})
            raise self.back(back, err=str(exc)) from None
        await self.audit(author, "config", summary, True,
                         {"from": result.version_before, "to": result.version_after, "git": result.git_commit})
        raise self.back(back, msg=f"Применено: версия {result.version_after}")

    async def config_toggle(self, request: web.Request) -> web.StreamResponse:
        form = await request.post()
        kind, ident, value = str(form.get("kind", "")), str(form.get("id", "")), str(form.get("value", ""))
        on = value == "on"

        def mutate(raw: dict[str, Any]) -> None:
            if kind == "module":
                raw.setdefault("modules", {}).setdefault(ident, {})
                if raw["modules"][ident] is None:
                    raw["modules"][ident] = {}
                raw["modules"][ident]["enabled"] = on
            elif kind == "reaction":
                rule = _find(raw.get("reactions", {}).get("rules", []), ident)
                if value in ("live", "dry_run"):
                    rule["mode"] = value
                else:
                    rule["enabled"] = on
            elif kind == "rule":
                template, rule_id = ident.split("/", 1)
                _find(raw.get("templates", {}).get(template, {}).get("rules", []), rule_id)["enabled"] = on
            elif kind == "source":
                src = _source(raw, int(ident))
                if value in ("full", "meta", "presence", "ignore"):
                    src["policy"] = value
                else:
                    src.pop("policy", None)
            else:
                raise EditError(f"неизвестный переключатель {kind}")

        return await self._edit(request, f"{kind} {ident}: {value or ('вкл' if on else 'выкл')}", mutate)

    async def config_source(self, request: web.Request) -> web.StreamResponse:
        form = await request.post()
        raw_id = str(form.get("chat_id", "")).strip()
        if not _num(raw_id):
            raise self.back("/config", err="chat_id должен быть числом")
        chat_id = int(raw_id)
        name = str(form.get("name", "")).strip()
        roles = _words(str(form.get("roles", "")))
        template = str(form.get("template", "")).strip()
        policy = str(form.get("policy", "")).strip()
        priority = str(form.get("priority", "")).strip()

        def mutate(raw: dict[str, Any]) -> None:
            sources = raw.setdefault("sources", [])
            src = next((s for s in sources if int(s.get("chat_id", 0)) == chat_id), None)
            if src is None:
                src = {"chat_id": chat_id}
                sources.append(src)
            for key, val in (("name", name), ("template", template), ("policy", policy)):
                if val:
                    src[key] = val
                else:
                    src.pop(key, None)
            src["roles"] = roles
            if priority:
                if not _num(priority):
                    raise EditError("приоритет должен быть числом")
                src["priority"] = int(priority)

        return await self._edit(request, f"источник {chat_id}: роли {', '.join(roles) or '—'}", mutate)

    async def config_source_delete(self, request: web.Request) -> web.StreamResponse:
        form = await request.post()
        chat_id = int(str(form.get("chat_id", "0")))

        def mutate(raw: dict[str, Any]) -> None:
            raw["sources"] = [s for s in raw.get("sources", []) if int(s.get("chat_id", 0)) != chat_id]

        return await self._edit(request, f"источник {chat_id} удалён", mutate)

    async def section_page(self, request: web.Request) -> web.Response:
        key = request.query.get("key", "")
        cfg = self.app.store.current.config
        if key not in type(cfg).model_fields:
            raise self.back("/config", err=f"Нет раздела {key}")
        merged = self.editor.merged()
        text = request.query.get("text") or (dump(merged[key]) if key in merged else "")
        effective = dump(cfg.model_dump(mode="json")[key])
        return self.render(request, "section.html", key=key, text=text, effective=effective)

    async def section_save(self, request: web.Request) -> web.StreamResponse:
        author = self.need(request, WEB_EDIT)
        form = await request.post()
        key, text = str(form.get("key", "")), str(form.get("text", ""))
        if key not in type(self.app.store.current.config).model_fields:
            raise self.back("/config", err=f"Нет раздела {key}")
        try:
            result = await self.editor.set_section(author, key, text, str(form.get("base_version", "")) or None)
        except EditError as exc:
            await self.audit(author, "config", f"раздел {key}", False, {"error": str(exc)})
            # The text stays in the form so the author can fix it.
            return self.render(request, "section.html", key=key, text=text, error=str(exc),
                               effective=dump(self.app.store.current.config.model_dump(mode="json")[key]))
        await self.audit(author, "config", f"раздел {key}", True,
                         {"from": result.version_before, "to": result.version_after, "git": result.git_commit})
        raise self.back(f"/config/section?key={key}", msg=f"Применено: версия {result.version_after}")

    # ---- dry-run of a reaction rule ------------------------------------------------------------------
    async def dryrun_page(self, request: web.Request) -> web.Response:
        rule, hours = request.query.get("rule", ""), request.query.get("hours", "24")
        reactions = next((m for m in self.app.modules if m.name == "reactions"), None)
        text = ""
        if rule:
            if reactions is None or not hasattr(reactions, "replay"):
                text = "Модуль reactions не подключён"
            else:
                text = await reactions.replay(rule, float(hours) if _num(hours.replace(".", "", 1)) else 24.0)
        rules = self.app.store.current.config.reactions.rules
        return self.render(request, "dryrun.html", rules=rules, rule=rule, hours=hours, text=text)


def _num(value: str) -> bool:
    return bool(value) and value.lstrip("-").isdigit()


def _q(value: str) -> str:
    from urllib.parse import quote

    return quote(value[:1500])


def _words(value: str) -> list[str]:
    return [w for w in value.replace(",", " ").split() if w]


def _find(items: list[dict[str, Any]], ident: str) -> dict[str, Any]:
    for item in items:
        if str(item.get("id")) == ident:
            return item
    raise EditError(f"правило {ident} не найдено")


def _source(raw: dict[str, Any], chat_id: int) -> dict[str, Any]:
    for src in raw.get("sources", []):
        if int(src.get("chat_id", 0)) == chat_id:
            return src
    raise EditError(f"источник {chat_id} не найден")
