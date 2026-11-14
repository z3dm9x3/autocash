"""ModuleQueue: lanes, priorities and the per-key order that outranks lanes (FT-7.3..7.5)."""

from __future__ import annotations

from autocash.core.queue.manager import LANE_BACKFILL, LANE_COMMANDS, LANE_LIVE, ModuleQueue
from autocash.core.record import MessageRecord


def rec(ingest_id: int, key: str, lane: int = LANE_LIVE, priority: int = 5) -> MessageRecord:
    return MessageRecord(
        trace_id=f"t{ingest_id}", ingest_id=ingest_id, scope_id=0, chat_id=-1001, msg_id=ingest_id, version=0,
        msg_date=0.0, account_id=1, event="new", source="backfill" if lane == LANE_BACKFILL else "live",
        policy="full", origin="external", sender_id=1, sender_role="user", can_command=True, source_roles=[],
        src_priority=5, reply_to=None, text="", features=[], config_version="v", received_at=0.0,
        msg_type="t", order_key=key, priority=priority, lane=lane,
    )


def queue(**kw: float) -> ModuleQueue:
    mq = ModuleQueue("m", maxsize=int(kw.get("maxsize", 100)), live_threshold=int(kw.get("live_threshold", 100)),
                     backfill_rate=kw.get("backfill_rate", 1000.0))
    mq.overflow = False
    return mq


def drain(mq: ModuleQueue) -> list[int]:
    out = []
    while (r := mq.take()) is not None:
        out.append(r.ingest_id)
        mq.release(r)
    return out


def test_lanes_commands_then_live_then_backfill() -> None:
    mq = queue()
    for r in (rec(1, "a", LANE_BACKFILL), rec(2, "b", LANE_LIVE), rec(3, "c", LANE_COMMANDS), rec(4, "d", LANE_LIVE)):
        assert mq.offer(r)
    assert drain(mq) == [3, 2, 4, 1]


def test_priority_inside_a_lane() -> None:
    mq = queue()
    for r in (rec(1, "a", priority=5), rec(2, "b", priority=1), rec(3, "c", priority=3)):
        mq.offer(r)
    assert drain(mq) == [2, 3, 1]


def test_one_key_is_strictly_sequential_and_other_keys_run_in_parallel() -> None:
    mq = queue()
    for r in (rec(1, "a"), rec(2, "a"), rec(3, "b")):
        mq.offer(r)
    first = mq.take()
    second = mq.take()
    assert first is not None and second is not None
    assert (first.ingest_id, second.ingest_id) == (1, 3)  # key "a" is busy: "b" runs in parallel
    assert mq.take() is None  # the second event of "a" waits for the first one
    mq.release(first)
    third = mq.take()
    assert third is not None and third.ingest_id == 2


def test_key_order_outranks_lanes() -> None:
    """A live event waits for the unprocessed backfill of its key, and that backfill is promoted."""
    mq = queue()
    mq.offer(rec(1, "k", LANE_BACKFILL))
    mq.offer(rec(2, "other", LANE_BACKFILL))
    mq.offer(rec(3, "k", LANE_LIVE))
    mq.offer(rec(4, "z", LANE_LIVE, priority=9))
    # Key "k" now carries a live event, so its backfill head goes with the live lane.
    assert drain(mq) == [1, 3, 4, 2]


def test_backfill_waits_while_live_lane_is_above_threshold() -> None:
    mq = queue(live_threshold=2)
    mq.offer(rec(1, "bf", LANE_BACKFILL))
    busy = [rec(10 + i, "same") for i in range(3)]  # one key: only its head can run
    for r in busy:
        mq.offer(r)
    head = mq.take()
    assert head is not None and head.ingest_id == 10
    assert mq.lane_counts[LANE_LIVE] == 2
    assert mq.take() is None  # live lane is at the threshold: backfill is held back
    mq.release(head)
    nxt = mq.take()
    assert nxt is not None and nxt.ingest_id == 11
    assert mq.lane_counts[LANE_LIVE] == 1
    bf = mq.take()
    assert bf is not None and bf.ingest_id == 1


def test_backfill_rate_is_limited() -> None:
    mq = queue(backfill_rate=1.0)
    mq.offer(rec(1, "a", LANE_BACKFILL))
    mq.offer(rec(2, "b", LANE_BACKFILL))
    first = mq.take()
    assert first is not None
    assert mq.take() is None and mq.next_retry > 0  # the token bucket is empty


def test_full_queue_drops_to_postgres_and_checkpoint_stays_behind() -> None:
    mq = queue(maxsize=2)
    assert mq.offer(rec(1, "a")) and mq.offer(rec(2, "b"))
    assert not mq.offer(rec(3, "c"))  # not held in memory: re-read from PostgreSQL later
    assert mq.overflow and mq.dropped_low == 3
    assert mq.checkpoint() == 0  # nothing is finished yet
    for r in (mq.take(), mq.take()):
        assert r is not None
        mq.release(r)
    assert mq.checkpoint() <= 2  # never passes the dropped message
