"""OBX fuse (D-OBX-7): a FloodWait halves the limits of the account once; manual reset; survives a restart."""

from __future__ import annotations

from typing import Any

import pytest

from autocash.core.config.loader import bare_group_id
from autocash.core.telegram.api import FloodWait
from tests.conftest import ACCOUNT, CASHER, KASSA, KASSA2, LOGS, AppFactory
from tests.fake_telegram.server import FakeTelegram
from tests.helpers import sent_by, started, wait_for

ADMIN = 8001


@pytest.fixture
def cfg(config: dict[str, Any]) -> dict[str, Any]:
    config["users"] = {ADMIN: ["admin"]}
    return config


def notices(fake: FakeTelegram) -> list[str]:
    return [t for _, t in sent_by(fake, LOGS) if "снижены вдвое" in t]


async def test_a_flood_wait_halves_the_limits_once_until_a_reset(app_factory: AppFactory, fake: FakeTelegram,
                                                                 cfg: dict[str, Any]) -> None:
    app = await started(app_factory(cfg=cfg))
    fake.fail_next(ACCOUNT, FloodWait(1.0, "send"), method="send")
    fake.post(KASSA, CASHER, "/ping")  # the demo module answers: the send gets the FloodWait
    await wait_for(lambda: notices(fake), what="the notice of the fuse in logs")
    await wait_for(lambda: ("pong" in [t for _, t in sent_by(fake, KASSA)]), what="pong after the pause")
    assert app.outbox.fuse.factor(ACCOUNT) == 0.5
    assert float(await app.db.pool.fetchval(
        "SELECT factor FROM obx_account_limits WHERE account_id = $1", ACCOUNT)) == 0.5

    # A second FloodWait: the account is paused again, the limits are not halved twice.
    fake.fail_next(ACCOUNT, FloodWait(1.0, "send"), method="send")
    fake.post(KASSA, CASHER, "/ping")
    await wait_for(lambda: [t for _, t in sent_by(fake, KASSA)].count("pong") == 2, what="the second pong")
    assert app.outbox.fuse.factor(ACCOUNT) == 0.5 and len(notices(fake)) == 1

    # /obx works only in a group with the role tech; there it shows and resets the limits.
    app.store.directory.set_group(bare_group_id(KASSA2), "tech")
    before = len(sent_by(fake, KASSA2))
    fake.post(KASSA2, ADMIN, "/obx")
    await wait_for(lambda: len(sent_by(fake, KASSA2)) > before, what="the answer to /obx")
    assert "СНИЖЕНЫ" in sent_by(fake, KASSA2)[-1][1] and "записи 0/10" not in sent_by(fake, KASSA2)[-1][1]
    fake.post(KASSA2, ADMIN, f"/obx reset {ACCOUNT}")
    await wait_for(lambda: "Лимиты восстановлены" in sent_by(fake, KASSA2)[-1][1], what="the reset")
    assert app.outbox.fuse.factor(ACCOUNT) == 1.0
    assert float(await app.db.pool.fetchval(
        "SELECT factor FROM obx_account_limits WHERE account_id = $1", ACCOUNT)) == 1.0


async def test_lowered_limits_survive_a_restart(app_factory: AppFactory, fake: FakeTelegram,
                                                cfg: dict[str, Any]) -> None:
    app = await started(app_factory(cfg=cfg))
    fake.fail_next(ACCOUNT, FloodWait(1.0, "getHistory"), method="getHistory")
    await app.outbox.read_gate(ACCOUNT, "user", "getHistory")  # a read passes the gate first
    app.outbox.on_flood(ACCOUNT, "getHistory", 1.0)  # as the limiter reports a FloodWait on a read
    await wait_for(lambda: notices(fake), what="the notice of the fuse")
    await wait_for(lambda: _stored(app), what="the stored factor")
    await app.stop()
    again = await started(app_factory(cfg=cfg))
    assert again.outbox.fuse.factor(ACCOUNT) == 0.5  # read back at start: no silent return to full limits


async def _stored(app: Any) -> bool:
    return bool(await app.db.pool.fetchval("SELECT count(*) FROM obx_account_limits WHERE factor < 1"))
