"""The batch writer must survive anything: every synchronous caller waits for it (invariant 5)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from autocash.core.storage.writer import BatchWriter


class _Db:
    """Only what the writer touches before it reaches PostgreSQL."""

    def __init__(self) -> None:
        self.calls = 0


@pytest.mark.parametrize("failure", [RuntimeError("boom"), asyncio.CancelledError()])
async def test_writer_retries_the_batch_after_an_unexpected_failure(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    writer = BatchWriter(_Db(), flush_interval_ms=1)  # type: ignore[arg-type]
    seen: list[int] = []
    attempts = 0

    async def fake_write(batch: list[Any]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise failure  # e.g. an error leaked from the driver while the connection was dying
        for item in batch:
            seen.append(await item.fn(None))
            if item.fut is not None:
                item.fut.set_result(None)

    monkeypatch.setattr(writer, "_write", fake_write)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep(asyncio.sleep))
    writer.start()

    async def fn(value: int) -> int:
        return value

    writer.submit(lambda conn: fn(1))
    await asyncio.wait_for(writer.submit_sync(lambda conn: fn(2)), 5)
    assert seen == [1, 2] and attempts == 2  # nothing lost, order kept
    await writer.stop()


async def test_writer_stops_on_a_real_cancellation() -> None:
    writer = BatchWriter(_Db(), flush_interval_ms=1)  # type: ignore[arg-type]
    writer.start()
    await asyncio.sleep(0)
    await writer.stop(drain=False)
    assert writer._task is None


def _no_sleep(real: Any) -> Any:
    async def sleep(delay: float, *args: Any) -> None:
        await real(0)

    return sleep
