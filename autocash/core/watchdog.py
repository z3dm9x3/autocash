"""systemd integration: READY=1 and a watchdog heartbeat from the main event loop (NFT-16).

The heartbeat is sent by a task of the main loop, so a hung loop stops the heartbeat and
systemd restarts the process (`WatchdogSec`). Without NOTIFY_SOCKET (Windows, tests, a manual
run) every call is a no-op.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket

log = logging.getLogger(__name__)


class SystemdNotifier:
    def __init__(self) -> None:
        self._addr = os.environ.get("NOTIFY_SOCKET", "")
        self._sock: socket.socket | None = None
        usec = os.environ.get("WATCHDOG_USEC", "")
        pid = os.environ.get("WATCHDOG_PID", "")
        self.interval_s: float | None = None
        if usec.isdigit() and (not pid or pid == str(os.getpid())):
            self.interval_s = int(usec) / 1_000_000 / 2  # ping twice per WatchdogSec
        self.sent: list[str] = []  # for tests

    @property
    def enabled(self) -> bool:
        return bool(self._addr) and hasattr(socket, "AF_UNIX")

    def notify(self, message: str) -> None:
        self.sent.append(message)
        if not self.enabled:
            return
        try:
            if self._sock is None:
                self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)  # type: ignore[attr-defined,unused-ignore]
            addr = self._addr
            if addr.startswith("@"):
                addr = "\0" + addr[1:]  # abstract namespace
            self._sock.sendto(message.encode(), addr)
        except OSError:
            log.exception("sd_notify failed")

    def ready(self, status: str = "") -> None:
        self.notify("READY=1" + (f"\nSTATUS={status}" if status else ""))

    def stopping(self) -> None:
        self.notify("STOPPING=1")

    async def heartbeat(self, interval_s: float | None = None) -> None:
        """Runs in the main loop for the whole life of the process."""
        period = interval_s or self.interval_s
        if period is None:
            return
        while True:
            self.notify("WATCHDOG=1")
            await asyncio.sleep(period)

    def close(self) -> None:
        if self._sock is not None:
            with contextlib.suppress(OSError):
                self._sock.close()
            self._sock = None
