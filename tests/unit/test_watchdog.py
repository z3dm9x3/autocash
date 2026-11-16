from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import sys
import tempfile
from pathlib import Path

import pytest

from autocash.core.watchdog import SystemdNotifier


def test_without_notify_socket_everything_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.delenv("WATCHDOG_USEC", raising=False)
    n = SystemdNotifier()
    assert not n.enabled and n.interval_s is None
    n.ready("ok")
    assert n.sent == ["READY=1\nSTATUS=ok"]


async def test_heartbeat_pings_twice_per_watchdog_period(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.setenv("WATCHDOG_USEC", "200000")  # WatchdogSec=0.2 -> ping every 0.1 s
    monkeypatch.setenv("WATCHDOG_PID", str(os.getpid()))
    n = SystemdNotifier()
    assert n.interval_s == pytest.approx(0.1)
    task = asyncio.create_task(n.heartbeat())
    await asyncio.sleep(0.35)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert 3 <= n.sent.count("WATCHDOG=1") <= 5


async def test_a_blocked_loop_stops_the_heartbeat(monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    monkeypatch.setenv("WATCHDOG_USEC", "100000")
    monkeypatch.setenv("WATCHDOG_PID", str(os.getpid()))
    n = SystemdNotifier()
    task = asyncio.create_task(n.heartbeat())
    await asyncio.sleep(0)
    time.sleep(0.3)  # noqa: ASYNC251 - the event loop is stuck on purpose: no pings meanwhile
    assert n.sent.count("WATCHDOG=1") == 1
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def test_watchdog_of_another_process_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WATCHDOG_USEC", "100000")
    monkeypatch.setenv("WATCHDOG_PID", "1")
    assert SystemdNotifier().interval_s is None


@pytest.mark.skipif(sys.platform == "win32" or not hasattr(socket, "AF_UNIX"), reason="sd_notify needs AF_UNIX")
def test_messages_reach_the_notify_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    path = Path(tempfile.mkdtemp()) / "notify.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)  # type: ignore[attr-defined,unused-ignore]
    server.bind(str(path))
    try:
        monkeypatch.setenv("NOTIFY_SOCKET", str(path))
        n = SystemdNotifier()
        n.ready()
        assert server.recv(100) == b"READY=1"
        n.close()
    finally:
        server.close()
