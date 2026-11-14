"""Criteria 3 and 5 of stage 1a: PostgreSQL outage, SQLite pragmas, invalid config/manifest."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import pytest

from autocash.core.app import App
from autocash.core.buffer.sqlite_buffer import BufferPragmaError
from tests.conftest import BOT, DEPOSIT_TEXT, KASSA, LOGS, AppFactory, write_config
from tests.fake_telegram.server import FakeTelegram
from tests.pgcluster import PgCluster


async def wait_for(predicate: Any, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("condition was not reached")


async def test_postgres_outage_intake_continues_then_everything_is_processed_once(
    app: App, fake: FakeTelegram, pg_cluster: PgCluster
) -> None:
    """Criterion 3 (item 3.3 of the TZ)."""
    for i in range(5):
        fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=i))
    await app.idle()

    await asyncio.to_thread(pg_cluster.stop)
    try:
        for i in range(5, 25):
            fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=i))
        await fake.drain()
        # Intake and SQLite keep working: all 20 events are accepted (committed to the buffer).
        assert app.gateway.accepted == 25

        async def all_waiting() -> bool:
            stats = await app.buffer.stats()
            return bool(stats["pending"] == 20 and stats["waiting"] == {"DB_UNAVAILABLE": 20})

        await wait_for(all_waiting)
        assert app.uploader.outage and not app.db.available
        assert app.queues.stats()["demo"]["processed"] == 5  # sorting and modules wait
        # The alarm goes through the emergency channel, without PostgreSQL (NFT-18).
        await wait_for(lambda: any(chat == LOGS and "PostgreSQL недоступен" in text for _, chat, text, _ in fake.sent))
    finally:
        await asyncio.to_thread(pg_cluster.start)

    await app.idle(timeout=60)
    rows = await app.db.pool.fetch(
        "SELECT m.msg_id, t.status, t.waited_reason FROM messages m JOIN trace_inbound t USING (trace_id) "
        "WHERE m.chat_id = $1 ORDER BY m.ingest_id", KASSA,
    )
    # Uploaded in the order of arrival, nothing lost, nothing duplicated.
    assert [r["msg_id"] for r in rows] == list(range(1, 26))
    assert all(r["status"] == "processed" for r in rows)
    assert [r["waited_reason"] for r in rows] == [None] * 5 + ["DB_UNAVAILABLE"] * 20
    assert await app.db.pool.fetchval("SELECT count(*) FROM trace_module WHERE status = 'processed'") == 25
    assert await app.db.pool.fetchval(
        "SELECT attrs->>'count' FROM entities WHERE entity_type = 'demo.counter' AND entity_key = $1", str(KASSA)
    ) == "25"
    stats = await app.buffer.stats()
    assert stats["pending"] == 0


async def test_wrong_sqlite_pragma_stops_with_an_alarm(
    app_factory: AppFactory, fake: FakeTelegram, config: dict[str, Any]
) -> None:
    """Criterion 5 (FT-4.5)."""
    config["buffer"] = {"synchronous": "NORMAL"}
    application = app_factory(cfg=config)
    with pytest.raises(BufferPragmaError, match="synchronous=1"):
        await application.start()
    assert any(chat == LOGS and "Остановка" in text and "synchronous" in text for _, chat, text, _ in fake.sent)
    assert application.gateway.accepted == 0 and not application.ready.is_set()


@pytest.mark.parametrize("breakage", ["schema", "manifest", "rule", "yaml"])
async def test_invalid_config_keeps_previous_version_and_reports_to_logs(
    app: App, fake: FakeTelegram, config: dict[str, Any], tmp_path: Path, breakage: str
) -> None:
    """Criterion 5 (FT-13.2, FT-3.8)."""
    path = tmp_path / "config.yaml"
    version = app.store.current.version
    manifest_version = app.store.current.manifest.version
    if breakage == "schema":
        config["sources"][0]["template"] = "no_such_template"
    elif breakage == "manifest":
        config["manifest"]["subfields"]["Сломано"] = {"regex": "(?P<x>"}
    elif breakage == "rule":
        config["templates"]["kassa"]["rules"][0]["when"]["text_regex"] = "(?<=a)b"
    if breakage == "yaml":
        path.write_text("sources: [oops", encoding="utf-8")
    else:
        write_config(path, config)

    ok, error = app.reload_config()
    assert not ok and error
    assert app.store.current.version == version and app.store.current.manifest.version == manifest_version

    # The previous version keeps working.
    fake.post(KASSA, BOT, DEPOSIT_TEXT.format(deposit_id=1))
    await app.idle()
    row = await app.db.pool.fetchrow("SELECT msg_type, config_version, manifest_version FROM messages")
    assert (row["msg_type"], row["config_version"], row["manifest_version"]) == (
        "deposit.request", version, manifest_version)
    assert any(chat == LOGS and "Конфигурация не применена" in text and version in text
               for _, chat, text, _ in fake.sent)


async def test_valid_reload_applies_without_restart_and_is_versioned(
    app: App, fake: FakeTelegram, config: dict[str, Any], tmp_path: Path
) -> None:
    """FT-13.3, FT-13.4, FT-3.9: hot reload, version in every record, features recomputed."""
    old = app.store.current.version
    fake.post(KASSA, BOT, "Код: 123")  # 3 digits: below the default minimal number length
    await app.idle()

    config["templates"]["kassa"]["rules"].append({"id": "code", "type": "test.code", "when": {"labels_all": ["Код"]}})
    config["manifest"]["number_min_len"] = 3
    write_config(tmp_path / "config.yaml", config)
    ok, new = app.reload_config()
    assert ok and new != old and app.store.current.version == new

    fake.post(KASSA, BOT, "Код: 456")
    await app.idle()

    async def recomputed() -> bool:
        return bool(await app.db.pool.fetchval(
            "SELECT count(DISTINCT manifest_version) = 1 FROM messages WHERE text LIKE 'Код:%'"))

    await wait_for(recomputed)
    rows = await app.db.pool.fetch(
        "SELECT m.msg_type, m.config_version, t.config_version AS trace_version, m.features::text AS features "
        "FROM messages m JOIN trace_inbound t USING (trace_id) WHERE m.text LIKE 'Код:%' ORDER BY m.ingest_id"
    )
    assert [(r["msg_type"], r["config_version"], r["trace_version"]) for r in rows] == [
        ("unknown", old, old), ("test.code", new, new)]
    assert all('"number"' in r["features"] for r in rows)  # the old message got its features recomputed
    versions = await app.db.pool.fetch("SELECT version FROM config_versions")
    assert {r["version"] for r in versions} == {old, new}
