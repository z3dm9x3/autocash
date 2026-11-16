from __future__ import annotations

import asyncio
import copy
import itertools
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest
import yaml

from autocash.core.app import App
from autocash.modules.api import Module
from autocash.modules.demo.module import DemoModule
from tests.fake_telegram.server import FakeTelegram
from tests.pgcluster import PgCluster

ACCOUNT = 111
BOT = 5001  # kassa bot
CASHER = 6001
PLAYER = 7001
KASSA = -1001000000001
KASSA2 = -1001000000002
LOGS = -1009000000001
DEBUG = -1009000000002

BASE_CONFIG: dict[str, Any] = {
    "service_chats": {"logs": LOGS, "debug": DEBUG},
    "sources": [
        {"chat_id": KASSA, "name": "kassa 1", "roles": ["kassa"], "template": "kassa", "priority": 1,
         "senders": {BOT: "kassa_bot", CASHER: "casher"}},
        {"chat_id": KASSA2, "name": "kassa 2", "roles": ["kassa"], "template": "kassa", "priority": 5,
         "senders": {BOT: "kassa_bot"}},
    ],
    "manifest": {
        "dictionaries": {"deposit": ["депозит"]},
        "subfields": {
            "Пользователь": {"regex": r"^(?P<name>\S+)\s+ID:\s*(?P<id>\d+)$"},
            "Успешных депозитов": {"regex": r"^(?P<marker>\S+)\s+(?P<count>\d+)\s+\(всего:\s*(?P<total>\d+)\)$"},
            "Метод оплаты": {"regex": r"^(?P<method>.*?)\s+(?P<min>\d[\d ]*\d)\s*-\s*(?P<max>\d[\d ]*\d)$"},
        },
        "commands": {"bare": ["scam", "unban"]},
    },
    "templates": {
        "kassa": {
            "rules": [
                {"id": "deposit_request", "type": "deposit.request",
                 "when": {"sender_role": ["kassa_bot"], "header": ["ДЕПОЗИТ ТРЕУГОЛЬНИК"]},
                 "fields": {"deposit_id": "Deposit ID", "mel_id": "Номер счёта", "tgid": "Пользователь.id"},
                 "order_key": ["deposit_id"], "priority": 3, "text_ttl_days": 30},
                {"id": "keyed", "type": "test.keyed", "when": {"labels_all": ["key"]},
                 "fields": {"key": "key", "n": "n"}, "order_key": ["key"]},
                {"id": "note", "type": "test.note", "when": {"text_regex": "^note"}},
            ]
        }
    },
    "uploader": {"batch_interval_ms": 50, "retry_max_s": 0.5},
    # Tests run without send pacing and without an API tempo unless a test sets them.
    "output": {"pacing": {kind: {"per_chat_interval_s": 0, "per_account_interval_s": 0, "group_per_minute": 0}
                          for kind in ("user", "bot")}},
    "pool": {"api_rate_per_s": {"user": 0, "bot": 0}, "reconnect_s": 0.1, "rebalance_s": 2.0},
}

_db_counter = itertools.count(1)


@pytest.fixture(scope="session")
def pg_cluster() -> Iterator[PgCluster]:
    cluster = PgCluster()
    cluster.init()
    cluster.start()
    asyncio.run(_drop_leftovers(cluster))
    try:
        yield cluster
    finally:
        cluster.destroy()


async def _drop_leftovers(cluster: PgCluster) -> None:
    """Databases of a run that was killed before its teardown (the cluster is reused)."""
    conn = await asyncpg.connect(cluster.dsn())
    try:
        for row in await conn.fetch("SELECT datname FROM pg_database WHERE datname ~ '^t[0-9]+$'"):
            await conn.execute(f"DROP DATABASE IF EXISTS {row['datname']} WITH (FORCE)")
    finally:
        await conn.close()


@pytest.fixture
async def pg_dsn(pg_cluster: PgCluster) -> AsyncIterator[str]:
    if not pg_cluster.running:
        pg_cluster.start()
    name = f"t{next(_db_counter)}"
    conn = await asyncpg.connect(pg_cluster.dsn())
    try:
        await conn.execute(f"CREATE DATABASE {name}")
    finally:
        await conn.close()
    yield pg_cluster.dsn(name)
    # Every database takes ~8 MB: drop it right after the test, not with the whole cluster,
    # so a full run does not eat the disk.
    if not pg_cluster.running:
        pg_cluster.start()
    conn = await asyncpg.connect(pg_cluster.dsn())
    try:
        await conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    finally:
        await conn.close()


def write_config(path: Path, config: dict[str, Any]) -> Path:
    path.write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def make_fake() -> FakeTelegram:
    fake = FakeTelegram()
    for chat_id in (KASSA, KASSA2, LOGS, DEBUG):
        fake.add_chat(chat_id, "supergroup", members=(ACCOUNT,))
    return fake


@pytest.fixture
def fake() -> FakeTelegram:
    return make_fake()


@pytest.fixture
def config() -> dict[str, Any]:
    return copy.deepcopy(BASE_CONFIG)


AppFactory = Callable[..., App]


@pytest.fixture
async def app_factory(
    tmp_path: Path, pg_dsn: str, fake: FakeTelegram, config: dict[str, Any]
) -> AsyncIterator[AppFactory]:
    apps: list[App] = []

    def factory(*, modules: list[Module] | None = None, cfg: dict[str, Any] | None = None,
                accounts: tuple[int, ...] = (ACCOUNT,), server: FakeTelegram | None = None,
                **kwargs: Any) -> App:
        path = write_config(tmp_path / "config.yaml", cfg or config)
        tg = server or fake
        app = App(
            config_path=path, sqlite_path=tmp_path / "buffer.sqlite", pg_dsn=pg_dsn,
            clients=[tg.client(a) for a in accounts], modules=modules if modules is not None else [DemoModule()],
            **kwargs,
        )
        apps.append(app)
        return app

    yield factory
    for app in apps:
        try:
            await app.stop()
        except Exception:
            pass


@pytest.fixture
async def app(app_factory: AppFactory) -> App:
    application = app_factory()
    await application.start()
    await application.ready.wait()
    return application


DEPOSIT_TEXT = (
    "[ДЕПОЗИТ ТРЕУГОЛЬНИК]\n"
    "Deposit ID: {deposit_id}\n"
    "Пользователь: @x ID: 7287295969\n"
    "Номер счёта: [1481598133](tel:1481598133)\n"
    "Сумма: 5 001.34"
)
