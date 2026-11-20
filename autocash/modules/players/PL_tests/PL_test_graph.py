"""PL: the graph of players on a real PostgreSQL (D-PL-1..4)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest

from autocash.modules.players import PL_store

MIGRATIONS = Path(__file__).resolve().parents[1] / "PL_migrations"
RM_DROP = Path(__file__).resolve().parents[2] / "role_manager" / "RM_migrations" / "004_rm_players_to_pl.sql"


async def migrate(pool: asyncpg.Pool) -> None:
    for f in sorted(MIGRATIONS.glob("*.sql")):
        await pool.execute(f.read_text(encoding="utf-8"))


@pytest.fixture
async def pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    p = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    assert p is not None
    await migrate(p)
    try:
        yield p
    finally:
        await p.close()


async def groups(pool: asyncpg.Pool) -> list[list[str]]:
    rows = await pool.fetch("SELECT player_id, kind || ':' || value AS ident FROM pl_nodes")
    out: dict[int, list[str]] = {}
    for r in rows:
        out.setdefault(r["player_id"], []).append(r["ident"])
    return sorted(sorted(g) for g in out.values())


async def test_a_new_pair_makes_a_player_and_shared_identifiers_join_it(pool: asyncpg.Pool) -> None:
    a = await PL_store.add_link(pool, 1, "100", source="test")
    assert a.new_link and a.created and a.joined == []
    # The same MelID from another Telegram account: the same person.
    b = await PL_store.add_link(pool, 2, "100", source="test")
    assert b.player_id == a.player_id and not b.created
    # The same Telegram account with another MelID: the same person.
    c = await PL_store.add_link(pool, 2, "200", source="test")
    assert c.player_id == a.player_id
    assert await groups(pool) == [["mel:100", "mel:200", "tg:1", "tg:2"]]
    # A repeat changes nothing.
    again = await PL_store.add_link(pool, 2, "200", source="test")
    assert not again.new_link and again.player_id == a.player_id
    assert await pool.fetchval("SELECT count(*) FROM pl_links") == 3


async def test_two_players_meet_the_older_stays_and_old_ids_point_at_it(pool: asyncpg.Pool) -> None:
    p1 = (await PL_store.add_link(pool, 1, "100", source="test")).player_id
    p2 = (await PL_store.add_link(pool, 2, "200", source="test")).player_id
    p3 = (await PL_store.add_link(pool, 3, "300", source="test")).player_id
    assert p1 is not None and p2 is not None and p3 is not None
    j = await PL_store.add_link(pool, 3, "200", source="test")  # 2 and 3 join: 2 is older
    assert (j.player_id, j.joined) == (p2, [p3])
    j = await PL_store.add_link(pool, 1, "300", source="test")  # 1 and {2, 3} join: 1 is older
    assert (j.player_id, j.joined) == (p1, [p2])
    assert await groups(pool) == [["mel:100", "mel:200", "mel:300", "tg:1", "tg:2", "tg:3"]]
    # Every old id resolves to the player that stayed, in one step.
    assert [await PL_store.current(pool, p) for p in (p1, p2, p3)] == [p1, p1, p1]
    assert await pool.fetchval("SELECT count(*) FROM pl_journal WHERE kind = 'join'") == 2
    assert (await PL_store.resolve(pool, f"#{p3}")).player_id == p1


async def test_no_size_limit_thousands_of_melids_and_accounts_are_one_player(pool: asyncpg.Pool) -> None:
    # Customer: one person may have 5 200 MelIDs and an unknown number of TGIDs (D-PL-1).
    for tg in range(10, 20):  # ten accounts, each with its own 520 MelIDs: ten players so far
        for n in range(520):
            await PL_store.add_link(pool, tg, f"{tg}{n:05d}", source="test")
    assert len(await groups(pool)) == 10
    # Nine pairs crossing the accounts join everything into one player.
    for tg in range(11, 20):
        await PL_store.add_link(pool, tg, "1000000", source="test")
    ids = await PL_store.idents(pool, await PL_store.player_of(pool, "tg", 10) or 0)
    assert (len(ids["tg"]), len(ids["mel"])) == (10, 5200)


async def test_a_staff_pair_is_kept_but_joins_nothing(pool: asyncpg.Pool) -> None:
    p1 = (await PL_store.add_link(pool, 1, "100", source="test")).player_id
    p2 = (await PL_store.add_link(pool, 2, "200", source="test")).player_id
    staff = 9000  # a cashier creating deposits for both players
    s1 = await PL_store.add_link(pool, staff, "100", source="test", staff=True)
    await PL_store.add_link(pool, staff, "200", source="test", staff=True)
    assert s1.player_id == p1  # the MelID's player
    assert await groups(pool) == [["mel:100", "tg:1"], ["mel:200", "tg:2"]]
    assert await PL_store.player_of(pool, "tg", staff) is None
    assert p2 is not None
    # By hand the link becomes active: the staff account is then a player's own account.
    assert await PL_store.set_state(pool, staff, "100", "active", author="8001") == "done"
    assert await PL_store.player_of(pool, "tg", staff) == p1


async def test_a_cut_link_splits_the_player_and_the_part_with_its_first_link_keeps_the_id(
        pool: asyncpg.Pool) -> None:
    p = (await PL_store.add_link(pool, 1, "100", source="test")).player_id
    await PL_store.add_link(pool, 2, "100", source="test")
    await PL_store.add_link(pool, 2, "200", source="test")
    await PL_store.add_link(pool, 3, "200", source="test")
    await PL_store.add_link(pool, 3, "300", source="test")
    assert len(await groups(pool)) == 1
    # Cut 2 - 200: {1, 2, 100} keeps the id (the first link 1 - 100), {3, 200, 300} is a new player.
    assert await PL_store.set_state(pool, 2, "200", "disabled", author="8001") == "done"
    assert await groups(pool) == [["mel:100", "tg:1", "tg:2"], ["mel:200", "mel:300", "tg:3"]]
    assert await PL_store.player_of(pool, "tg", 1) == p
    new = await PL_store.player_of(pool, "tg", 3)
    assert new is not None and new != p
    assert await pool.fetchval("SELECT count(*) FROM pl_journal WHERE kind = 'split'") == 1
    # The same pair seen again keeps the cut; a cut of a lone link frees its identifiers.
    await PL_store.add_link(pool, 2, "200", source="test")
    assert len(await groups(pool)) == 2
    assert await PL_store.set_state(pool, 1, "100", "disabled", author="8001") == "done"
    assert await groups(pool) == [["mel:100", "tg:2"], ["mel:200", "mel:300", "tg:3"]]
    assert await PL_store.set_state(pool, 1, "100", "disabled", author="8001") == "same"
    assert await PL_store.set_state(pool, 5, "500", "disabled", author="8001") == "missing"
    # Restored: everything is one player again.
    await PL_store.set_state(pool, 2, "200", "active", author="8001")
    await PL_store.set_state(pool, 1, "100", "active", author="8001")
    assert len(await groups(pool)) == 1


async def test_parallel_pairs_build_the_same_graph(pool: asyncpg.Pool) -> None:
    # Pairs arriving at once from several modules are serialized by the lock: no lost join.
    pairs = [(t, str(m)) for t in range(1, 30) for m in (t, t + 1)]
    await asyncio.gather(*(PL_store.add_link(pool, t, m, source="test") for t, m in pairs))
    assert len(await groups(pool)) == 1
    assert await pool.fetchval("SELECT count(DISTINCT player_id) FROM pl_nodes") == 1


async def test_the_migration_copies_known_pairs_and_heal_builds_their_players(pg_dsn: str) -> None:
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        # Tables of Autosup and of RM players as they were before the module players (D-RM-14).
        await pool.execute(
            "CREATE TABLE as_dialogs (tg_id bigint PRIMARY KEY, mel_id text, updated_at timestamptz DEFAULT now());"
            "CREATE TABLE rm_users (tg_id bigint PRIMARY KEY, player_id bigint);"
            "CREATE TABLE rm_players (player_id bigserial PRIMARY KEY);"
            "CREATE TABLE rm_player_mel (mel_id text PRIMARY KEY, player_id bigint, source text, tg_id bigint,"
            " created_at timestamptz DEFAULT now());"
            "INSERT INTO as_dialogs (tg_id, mel_id) VALUES (1, '100'), (2, NULL);"
            "INSERT INTO rm_player_mel (mel_id, player_id, source, tg_id) VALUES ('100', 1, 'autosup', 1),"
            " ('200', 1, 'autosup', 3);")
        await migrate(pool)
        assert await pool.fetchval("SELECT count(*) FROM pl_links") == 2
        assert await PL_store.heal(pool) == 2
        assert await groups(pool) == [["mel:100", "tg:1"], ["mel:200", "tg:3"]]
        assert await PL_store.heal(pool) == 0  # nothing left to heal
        # The RM tables go once the graph is there.
        await pool.execute(RM_DROP.read_text(encoding="utf-8"))
        assert await pool.fetchval("SELECT to_regclass('rm_player_mel')") is None
        assert await pool.fetchval(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'rm_users' "
            "AND column_name = 'player_id'") == 0
    finally:
        await pool.close()
