"""Criterion 6 of stage 1a: order by key under parallel processing, isolation of modules."""

from __future__ import annotations

import asyncio
import itertools
import time
from typing import ClassVar

from autocash.modules.api import Context, Event, Module
from tests.conftest import KASSA, PLAYER, AppFactory
from tests.fake_telegram.server import FakeTelegram


class Recorder(Module):
    name: ClassVar[str] = "recorder"
    subscriptions: ClassVar[list[str]] = ["test.keyed"]
    concurrency: ClassVar[int] = 8

    def __init__(self) -> None:
        self.spans: list[tuple[str, int, float, float]] = []
        self.active = 0
        self.max_active = 0

    async def handle(self, event: Event, ctx: Context) -> None:
        started = time.monotonic()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            # Later events of a key are faster: without per-key ordering they would overtake.
            await asyncio.sleep(0.03 / int(event.fields["n"]))
        finally:
            self.active -= 1
        self.spans.append((event.fields["key"], int(event.fields["n"]), started, time.monotonic()))
        ctx.upsert_entity("test.last", event.fields["key"], attrs={"n": int(event.fields["n"])})


class Slow(Module):
    name: ClassVar[str] = "slow"
    subscriptions: ClassVar[list[str]] = ["*"]
    concurrency: ClassVar[int] = 1

    def __init__(self) -> None:
        self.done = 0

    async def handle(self, event: Event, ctx: Context) -> None:
        await asyncio.sleep(0.25)
        self.done += 1


class Fast(Module):
    name: ClassVar[str] = "fast"
    subscriptions: ClassVar[list[str]] = ["test.*"]

    def __init__(self) -> None:
        self.finished_at: list[float] = []

    async def handle(self, event: Event, ctx: Context) -> None:
        self.finished_at.append(time.monotonic())


class Broken(Module):
    name: ClassVar[str] = "broken"
    subscriptions: ClassVar[list[str]] = ["test.note"]
    timeout_s: ClassVar[float] = 0.2

    async def handle(self, event: Event, ctx: Context) -> None:
        if "hang" in event.record.text:
            await asyncio.sleep(5)
        if "boom" in event.record.text:
            raise RuntimeError("boom")
        ctx.upsert_entity("test.ok", str(event.record.msg_id))


async def test_same_key_is_sequential_and_different_keys_are_parallel(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    recorder = Recorder()
    app = app_factory(modules=[recorder])
    await app.start()
    await app.ready.wait()

    keys = ["A", "B", "C", "D", "E", "F"]
    per_key = 8
    for n in range(1, per_key + 1):
        for key in keys:
            fake.post(KASSA, PLAYER, f"key: {key}\nn: {n}")
    await app.idle()

    assert len(recorder.spans) == len(keys) * per_key
    for key in keys:
        spans = [s for s in recorder.spans if s[0] == key]
        assert [s[1] for s in spans] == list(range(1, per_key + 1)), key  # strictly in order
        for prev, cur in itertools.pairwise(spans):
            assert cur[2] >= prev[3], key  # the next event starts after the previous one finished
    assert recorder.max_active > 1  # different keys really ran in parallel
    last = await app.db.pool.fetch("SELECT entity_key, attrs->>'n' AS n FROM entities WHERE entity_type = 'test.last'")
    assert {r["entity_key"]: r["n"] for r in last} == {key: str(per_key) for key in keys}


async def test_slow_module_does_not_delay_another_module(app_factory: AppFactory, fake: FakeTelegram) -> None:
    slow, fast = Slow(), Fast()
    app = app_factory(modules=[slow, fast])
    await app.start()
    await app.ready.wait()

    total = 12
    posted = time.monotonic()
    for i in range(total):
        fake.post(KASSA, PLAYER, f"note {i}")

    deadline = time.monotonic() + 10
    while len(fast.finished_at) < total and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert len(fast.finished_at) == total
    assert max(fast.finished_at) - posted < 1.5  # the fast module is done long before the slow one
    assert slow.done < total  # ... while the slow module is still working through its own queue

    await app.idle(timeout=30)
    assert slow.done == total
    rows = await app.db.pool.fetch(
        "SELECT module, count(*) AS n FROM trace_module WHERE status = 'processed' GROUP BY 1")
    assert {r["module"]: r["n"] for r in rows} == {"slow": total, "fast": total}
    assert await app.db.pool.fetchval("SELECT count(*) FROM trace_inbound WHERE status = 'processed'") == total


async def test_module_error_and_timeout_get_reason_codes_and_queue_goes_on(
    app_factory: AppFactory, fake: FakeTelegram
) -> None:
    """FT-8.11: MODULE_ERROR / MODULE_TIMEOUT, the module queue keeps working."""
    app = app_factory(modules=[Broken()])
    await app.start()
    await app.ready.wait()

    fake.post(KASSA, PLAYER, "note boom")
    fake.post(KASSA, PLAYER, "note hang")
    ok = fake.post(KASSA, PLAYER, "note fine")
    await app.idle()

    rows = await app.db.pool.fetch(
        "SELECT m.text, t.status, t.reason_code, i.status AS inbound, i.reason_code AS inbound_reason "
        "FROM trace_module t JOIN messages m USING (trace_id) JOIN trace_inbound i USING (trace_id) "
        "ORDER BY m.ingest_id"
    )
    assert [(r["text"], r["status"], r["reason_code"]) for r in rows] == [
        ("note boom", "waiting", "MODULE_ERROR"),
        ("note hang", "waiting", "MODULE_TIMEOUT"),
        ("note fine", "processed", None),
    ]
    assert [(r["inbound"], r["inbound_reason"]) for r in rows] == [
        ("waiting", "MODULE_ERROR"), ("waiting", "MODULE_TIMEOUT"), ("processed", None)]
    assert await app.db.pool.fetchval(
        "SELECT entity_key FROM entities WHERE entity_type = 'test.ok'") == str(ok.id_for(0))
