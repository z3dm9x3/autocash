"""Stage 3: reaction rules, dialogs, commands as rules, dry-run (criteria 1-7)."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from autocash.core.app import App
from autocash.core.reactions.module import ReactionsModule
from autocash.modules.players.module import PlayersModule
from tests.conftest import ACCOUNT, KASSA, LOGS, PLAYER, AppFactory, write_config
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001
ROOT = Path(__file__).resolve().parents[2]

RULES: list[dict[str, Any]] = [
    {"id": "hello", "when": {"type": ["test.note"]},
     "if": {"startswith": [{"var": "event.text"}, "note hello"]},
     "then": [{"action": "render", "text": "Привет, {{ event.sender_id }}!", "as": "greeting"},
              {"action": "reply", "text": "{{ r.greeting }}"}]},
    {"id": "auto", "autoreply": True, "when": {"type": ["test.note"]},
     "if": {"startswith": [{"var": "event.text"}, "note help"]},
     "then": [{"action": "reply", "text": "Автоответ: оператор скоро ответит"}]},
    {"id": "dry", "mode": "dry_run", "when": {"type": ["test.note"]},
     "if": {"startswith": [{"var": "event.text"}, "note dry"]},
     "then": [{"action": "reply", "text": "сухой прогон"},
              {"action": "set_state", "entity_type": "probe", "key": "dry", "state": "touched"}]},
    {"id": "later", "when": {"type": ["test.note"]}, "if": {"==": [{"var": "event.text"}, "note later"]},
     "then": [{"action": "schedule", "timer": "remind", "in_s": 1, "payload": {"chat": "{{ event.chat_id }}"}}]},
    {"id": "remind", "when": {"timer": "remind"},
     "then": [{"action": "send", "chat_id": "{{ payload.chat }}", "text": "Напоминание"}]},
    {"id": "asker", "when": {"type": ["test.note"]}, "if": {"==": [{"var": "event.text"}, "note ask me"]},
     "then": [{"action": "ask", "text": "Как вас зовут?", "arg": "name"},
              {"action": "stop"}]},
    {"id": "scam", "when": {"command": "scam", "args": [{"name": "id", "type": "ident"}]},
     "then": [
         {"action": "call", "name": "resolve_player", "ident": "{{ args.id }}", "as": "p"},
         {"action": "if", "cond": {"==": [{"var": "r.p.status"}, "ambiguous"]},
          "then": [{"action": "ask", "text": "Уточните: tg:{{ args.id }} или mel:{{ args.id }}", "arg": "id"}],
          "else": [{"action": "call", "name": "player_mark", "kind": "scam", "ident": "{{ r.p.ident }}"},
                   {"action": "reply", "text": "Страйк записан: {{ r.p.ident }}"}]}]},
    {"id": "unban", "when": {"command": "unban", "args": [{"name": "id", "type": "ident"}]},
     "then": [{"action": "call", "name": "resolve_player", "ident": "{{ args.id }}", "as": "p"},
              {"action": "call", "name": "player_mark", "kind": "unban", "ident": "{{ r.p.ident }}"},
              {"action": "reply", "text": "Разблокирован: {{ r.p.ident }}"}]},
]


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"]}
    config["defaults"] = {"private_template": "kassa"}  # private dialogs get the same simple types
    config["reactions"] = {"rules": RULES, "autoreply_pause_min": 30, "per_dialog_per_hour": 2}
    return config


async def make(app_factory: AppFactory, cfg: dict[str, Any]) -> App:
    return await started(app_factory(cfg=cfg, modules=[ReactionsModule(), PlayersModule()]))


def texts(fake: FakeTelegram, chat: int) -> list[str]:
    return [t for _, t in sent_by(fake, chat)]


# ---- criterion 1: rules through the configuration ----------------------------------------------------
async def test_rule_is_switched_and_changed_by_config_without_restart(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any], tmp_path: Path) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, PLAYER, "note hello")
    await wait_for(lambda: texts(fake, KASSA) == [f"Привет, {PLAYER}!"], what="greeting")

    path = tmp_path / "config.yaml"
    changed = json.loads(json.dumps(cfg))
    changed["reactions"]["rules"][0]["then"][1]["text"] = "Здравствуйте!"
    changed["reactions"]["rules"][0]["version"] = 2
    write_config(path, changed)
    assert app.reload_config()[0]
    fake.post(KASSA, PLAYER, "note hello")
    await wait_for(lambda: texts(fake, KASSA)[-1] == "Здравствуйте!", what="changed greeting")

    changed["reactions"]["rules"][0]["enabled"] = False
    write_config(path, changed)
    assert app.reload_config()[0]
    fake.post(KASSA, PLAYER, "note hello")
    await app.idle()
    await asyncio.sleep(1)
    assert len(texts(fake, KASSA)) == 2  # the switched-off rule did not answer

    # An invalid rule is not applied: the previous version works, the error goes to `logs`.
    broken = json.loads(json.dumps(changed))
    broken["reactions"]["rules"][0]["enabled"] = True
    broken["reactions"]["rules"][0]["if"] = {"no_such_operator": [1]}
    write_config(path, broken)
    version = app.store.current.version
    ok, error = app.reload_config()
    assert not ok and "no_such_operator" in error and app.store.current.version == version
    await wait_for(lambda: any("не применена" in t for t in texts(fake, LOGS)), what="error in logs")
    bad_action = json.loads(json.dumps(changed))
    bad_action["reactions"]["rules"][0]["then"].append({"action": "explode"})
    write_config(path, bad_action)
    assert not app.reload_config()[0]
    log = await app.db.pool.fetch("SELECT rule, rule_version FROM rule_log WHERE rule = 'hello' ORDER BY id")
    assert [r["rule_version"] for r in log] == ["1", "2"]


# ---- criterion 2: dry-run ------------------------------------------------------------------------------
async def test_dry_run_creates_only_simulations_and_the_journal(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, PLAYER, "note dry run please")
    await app.idle()
    assert texts(fake, KASSA) == []  # nothing was sent
    task = await app.db.pool.fetchrow("SELECT status, finished_at, text, rule FROM outbound_tasks")
    assert task["status"] == "simulation" and task["finished_at"] is not None and task["text"] == "сухой прогон"
    assert await app.db.pool.fetchval("SELECT count(*) FROM entities WHERE entity_type = 'probe'") == 0
    log = await app.db.pool.fetchrow("SELECT actions, tasks, input FROM rule_log WHERE rule = 'dry'")
    actions = json.loads(log["actions"])
    assert [a["action"] for a in actions] == ["reply", "set_state"] and json.loads(log["tasks"])
    assert json.loads(log["input"])["event"]["text"] == "note dry run please"

    # A run of a rule over the stored history: what would have fired and been sent.
    fake.post(KASSA, PLAYER, "note hello")
    fake.post(KASSA, PLAYER, "note something else")
    await app.idle()
    module = next(m for m in app.modules if isinstance(m, ReactionsModule))
    report = await module.replay("hello", 1)
    assert "подходящих событий 3" in report and "сработало бы 1" in report and "Привет" in report
    sent_before = len(sent_by(fake, KASSA))
    reply = await ask(app, fake, "/dryrun hello 1")
    assert "сработало бы 1" in reply and len(sent_by(fake, KASSA)) == sent_before + 1


async def ask(app: App, fake: FakeTelegram, text: str, chat: int = KASSA, user: int = ADMIN) -> str:
    before = len(sent_by(fake, chat))
    fake.post(chat, user, text)
    await wait_for(lambda: len(sent_by(fake, chat)) > before, what=f"reply to {text}")
    await app.idle()
    return sent_by(fake, chat)[before][1]


# ---- criterion 3: the human pause --------------------------------------------------------------------------
async def test_autoreply_waits_while_a_staff_member_talks_and_obeys_the_limit(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    fake.add_chat(PLAYER, "private", "player", members=(ACCOUNT,))
    other = 7002
    fake.add_chat(other, "private", "player 2", members=(ACCOUNT,))
    app = await make(app_factory, cfg)

    # A staff member answered the player by hand from the pool account: origin=human.
    fake.post(PLAYER, ACCOUNT, "Сейчас посмотрю")
    await app.idle()
    fake.post(PLAYER, PLAYER, "note help me")
    await app.idle()
    assert [t for t in texts(fake, PLAYER) if "Автоответ" in t] == []
    log = await app.db.pool.fetchval("SELECT actions FROM rule_log WHERE rule = 'auto'")
    assert "staff member" in log

    # Nobody wrote by hand to the other player: the answer goes, but not more than the limit.
    for _ in range(3):
        fake.post(other, other, "note help")
        await app.idle()
    assert texts(fake, other) == ["Автоответ: оператор скоро ответит"] * 2
    origin = await app.db.pool.fetchval("SELECT origin FROM messages WHERE text = 'Сейчас посмотрю'")
    assert origin == "human"


# ---- criteria 4 and 7: every task is final; an ambiguous identifier is clarified -----------------------------
async def test_scam_unban_rights_marks_and_clarification(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    # "555" is a TGID of one player and a MelID of another (FT-12.5).
    await app.db.pool.execute("INSERT INTO player_idents (kind, value, player_id) VALUES "
                              "('tg', '555', 1), ('mel', 'A', 1), ('mel', '555', 2), ('tg', '42', 2)")

    # Criterion 6: without rights the command is refused with ACCESS_DENIED: nothing in the chat,
    # the refusal goes to the group logs (D-RM-8).
    before = len(sent_by(fake, KASSA))
    fake.post(KASSA, PLAYER, "scam 777")
    await app.idle()
    assert len(sent_by(fake, KASSA)) == before
    await wait_for(lambda: any("Отказ в правах" in t and "/scam" in t for _, t in sent_by(fake, LOGS)),
                   what="the refusal in logs")
    row = await app.db.pool.fetchrow("SELECT t.status, t.reason_code FROM trace_inbound t JOIN messages m "
                                     "USING (trace_id) WHERE m.text = 'scam 777'")
    assert (row["status"], row["reason_code"]) == ("cancelled", "ACCESS_DENIED")
    assert await app.db.pool.fetchval("SELECT count(*) FROM player_marks") == 0

    # With rights: an unambiguous id is marked at once, with the author.
    assert "Страйк записан: tg:777" in await ask(app, fake, "scam 777")
    # Criterion 7: an ambiguous id -> a question; the answer finishes the command.
    assert "Уточните: tg:555 или mel:555" in await ask(app, fake, "/scam 555")
    assert "Страйк записан: mel:555" in await ask(app, fake, "mel:555")
    assert "Разблокирован: tg:777" in await ask(app, fake, "unban tg:777")
    marks = await app.db.pool.fetch("SELECT kind, ident_kind, ident, author FROM player_marks ORDER BY id")
    assert [(m["kind"], m["ident_kind"], m["ident"], m["author"]) for m in marks] == [
        ("scam", "tg", "777", str(ADMIN)), ("scam", "mel", "555", str(ADMIN)), ("unban", "tg", "777", str(ADMIN))]
    # A wrong argument type is explained, not executed.
    assert "TGID, MelID" in await ask(app, fake, "scam 5!5")

    # Criterion 4: every outbound task has reached a final status.
    await app.idle()
    open_tasks = await app.db.pool.fetchval("SELECT count(*) FROM outbound_tasks WHERE finished_at IS NULL")
    statuses = {r["status"] for r in await app.db.pool.fetch("SELECT DISTINCT status FROM outbound_tasks")}
    assert open_tasks == 0 and statuses == {"delivered"}


async def test_timer_fires_later_and_external_actions_are_final(
        app_factory: AppFactory, fake: FakeTelegram, cfg: dict[str, Any]) -> None:
    app = await make(app_factory, cfg)
    fake.post(KASSA, PLAYER, "note later")
    await wait_for(lambda: texts(fake, KASSA) == ["Напоминание"], timeout=15, what="timer")
    await app.idle()
    timer = await app.db.pool.fetchrow("SELECT fired_at, fire_at - created_at AS delay, rule FROM timers")
    assert timer["fired_at"] is not None and timer["rule"] == "later" and timer["delay"].total_seconds() >= 1
    task = await app.db.pool.fetchrow("SELECT status, module, parent_trace_id FROM outbound_tasks")
    assert task["status"] == "delivered" and task["module"] == "reactions" and task["parent_trace_id"] is not None


# ---- criterion 5: kill -9 between the dialog state and the send -----------------------------------------------
@pytest.mark.timeout(600)
@pytest.mark.parametrize("point", ["module_after_commit", "output_before_send"])
async def test_kill_9_between_dialog_state_and_send_answers_exactly_once(
        app_factory: AppFactory, cfg: dict[str, Any], tmp_path: Path, pg_dsn: str, point: str) -> None:
    config_path = write_config(tmp_path / "config.yaml", cfg)
    sqlite, state = tmp_path / "buffer.sqlite", tmp_path / "telegram.jsonl"
    args = [str(config_path), str(sqlite), pg_dsn, str(state)]
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "AUTOCASH_CRASHPOINT": point, "AUTOCASH_CRASHPOINT_AFTER": "1"}
    first = await asyncio.to_thread(subprocess.run, [sys.executable, "-m", "tests.acceptance.crash_react_child",
                                                     "ask", *args], cwd=ROOT, env=env, capture_output=True,
                                    text=True, encoding="utf-8", errors="replace", timeout=240)
    assert first.returncode == 137 and "IDLE" not in first.stdout, first.stdout[-2000:] + first.stderr[-2000:]
    env.pop("AUTOCASH_CRASHPOINT")
    second = await asyncio.to_thread(subprocess.run, [sys.executable, "-m", "tests.acceptance.crash_react_child",
                                                      "recover", *args], cwd=ROOT, env=env, capture_output=True,
                                     text=True, encoding="utf-8", errors="replace", timeout=240)
    assert "IDLE" in second.stdout, second.stdout[-2000:] + second.stderr[-2000:]

    sends = [json.loads(line) for line in state.read_text(encoding="utf-8").splitlines()]
    questions = [s for s in sends if s["t"] == "send" and s["text"] == "Как вас зовут?"]
    assert len(questions) == 1  # exactly once
    app = app_factory(cfg=cfg)
    await app.db.connect()
    dialog = await app.db.pool.fetchrow("SELECT state FROM entities WHERE entity_type = 'dialog'")
    assert dialog["state"] == "awaiting"  # the state was written before the send (FT-8.9)
    task = await app.db.pool.fetchrow("SELECT status FROM outbound_tasks WHERE rule LIKE 'asker:%'")
    assert task["status"] == "delivered"
    await app.db.close()


def test_rule_config_errors_are_reported_at_load() -> None:
    from autocash.core.config.loader import ConfigError, build_runtime

    base: dict[str, Any] = yaml.safe_load(yaml.safe_dump({"reactions": {"rules": []}}))
    for rule, message in (
        ({"id": "x", "then": []}, "needs type, command or timer"),
        ({"id": "x", "when": {"type": ["a"]}, "then": [{"action": "reply"}]}, "needs `text`"),
        ({"id": "x", "when": {"type": ["a"]}, "then": [{"action": "reply", "text": "{{ broken"}]}, "template"),
        ({"id": "x", "when": {"type": ["a"]}, "then": [{"action": "if", "then": []}]}, "needs `cond`"),
    ):
        base["reactions"]["rules"] = [rule]
        with pytest.raises(ConfigError, match=message.replace("`", ".")):
            build_runtime(base)

