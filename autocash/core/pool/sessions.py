"""Encrypted session files and the watcher of the sessions directory (FT-1.2, FT-1.9).

A session file `<name>.session.enc` is self-contained: it holds the Telegram session string and
the API credentials of the account, encrypted with a key that never lies next to the files
(environment variable or a systemd credential). A file appearing in the directory adds the
account to the pool, a file vanishing removes it - no restart, no code change.

Format: b"ACS1" | nonce (16) | AES-256-CTR ciphertext | HMAC-SHA256 tag (32), encrypt-then-MAC
with two keys derived from the master key (docs/decisions.md, D-1b-1).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import os
import secrets
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyaes

from autocash.core.pool.pool import AccountPool, PoolFull
from autocash.core.telegram.api import TelegramClient

log = logging.getLogger(__name__)

MAGIC = b"ACS1"
SUFFIX = ".session.enc"
KEY_ENV = "AUTOCASH_SESSION_KEY"
KEY_FILE_ENV = "AUTOCASH_SESSION_KEY_FILE"


class SessionError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SessionData:
    name: str
    kind: str  # "user" | "bot"
    api_id: int
    api_hash: str
    session: str  # Telethon StringSession
    bot_token: str = ""


ClientFactory = Callable[[SessionData], TelegramClient]


def load_key() -> bytes:
    """The 32-byte master key: base64 in AUTOCASH_SESSION_KEY or in the file AUTOCASH_SESSION_KEY_FILE."""
    raw = os.environ.get(KEY_ENV, "")
    path = os.environ.get(KEY_FILE_ENV, "")
    if not raw and path:
        raw = Path(path).read_text(encoding="ascii").strip()
    if not raw:
        raise SessionError(f"session key is not set: {KEY_ENV} or {KEY_FILE_ENV}")
    try:
        key = base64.b64decode(raw, validate=True)
    except ValueError as exc:
        raise SessionError("session key is not valid base64") from exc
    if len(key) != 32:
        raise SessionError("session key must be 32 bytes")
    return key


def new_key() -> str:
    return base64.b64encode(secrets.token_bytes(32)).decode("ascii")


def _subkeys(key: bytes) -> tuple[bytes, bytes]:
    return (hmac.new(key, b"autocash-session-enc", hashlib.sha256).digest(),
            hmac.new(key, b"autocash-session-mac", hashlib.sha256).digest())


def _ctr(enc_key: bytes, nonce: bytes, data: bytes) -> bytes:
    cipher = pyaes.AESModeOfOperationCTR(enc_key, counter=pyaes.Counter(int.from_bytes(nonce, "big")))
    return bytes(cipher.encrypt(data))


def encrypt(data: SessionData, key: bytes) -> bytes:
    enc_key, mac_key = _subkeys(key)
    nonce = secrets.token_bytes(16)
    body = _ctr(enc_key, nonce, json.dumps(asdict(data), ensure_ascii=False).encode("utf-8"))
    tag = hmac.new(mac_key, MAGIC + nonce + body, hashlib.sha256).digest()
    return MAGIC + nonce + body + tag


def decrypt(blob: bytes, key: bytes) -> SessionData:
    if len(blob) < len(MAGIC) + 16 + 32 or not blob.startswith(MAGIC):
        raise SessionError("not an Autocash session file")
    enc_key, mac_key = _subkeys(key)
    nonce, body, tag = blob[4:20], blob[20:-32], blob[-32:]
    if not hmac.compare_digest(tag, hmac.new(mac_key, MAGIC + nonce + body, hashlib.sha256).digest()):
        raise SessionError("session file is damaged or the key is wrong")
    try:
        return SessionData(**json.loads(_ctr(enc_key, nonce, body).decode("utf-8")))
    except (ValueError, TypeError) as exc:
        raise SessionError("session file has an unknown layout") from exc


def write_session(directory: Path, data: SessionData, key: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{data.name}{SUFFIX}"
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)  # only the service user may read it
    with os.fdopen(fd, "wb") as fh:
        fh.write(encrypt(data, key))
    os.replace(tmp, path)
    return path


def read_session(path: Path, key: bytes) -> SessionData:
    return decrypt(path.read_bytes(), key)


class SessionLock:
    """One session - one process: an OS lock on `<file>.lock`, released when the process dies."""

    def __init__(self, session_path: Path) -> None:
        self.path = session_path.with_suffix(session_path.suffix + ".lock")
        self._fh: Any = None

    def acquire(self) -> bool:
        fh = open(self.path, "a+b")  # noqa: SIM115 - held for the lifetime of the account
        try:
            if sys.platform == "win32":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True

    def release(self) -> None:
        if self._fh is not None:
            with contextlib.suppress(OSError):
                self._fh.close()
            self._fh = None
            with contextlib.suppress(OSError):
                self.path.unlink()


class SessionWatcher:
    """Adds an account when its session file appears and removes it when the file is gone."""

    def __init__(self, directory: Path, key: bytes, factory: ClientFactory, pool: AccountPool,
                 interval_s: float = 2.0) -> None:
        self.directory = directory
        self._key = key
        self._factory = factory
        self._pool = pool
        self._interval = interval_s
        self._known: dict[str, tuple[TelegramClient, SessionLock]] = {}
        self._failed: dict[str, float] = {}  # file -> mtime of the version that could not be used
        self._task: asyncio.Task[None] | None = None

    async def scan(self) -> None:
        present = {p.name: p for p in sorted(self.directory.glob(f"*{SUFFIX}"))} if self.directory.is_dir() else {}
        for name in [n for n in self._known if n not in present]:
            client, lock = self._known.pop(name)
            log.warning("session file %s is gone: the account leaves the pool", name)
            await self._pool.remove(int(client.account_id), "файл сессии удалён")
            lock.release()
        for name, path in present.items():
            if name in self._known:
                continue
            mtime = path.stat().st_mtime
            if self._failed.get(name) == mtime:
                continue
            lock = SessionLock(path)
            try:
                if not lock.acquire():
                    raise SessionError("the session is used by another process")
                data = await asyncio.to_thread(read_session, path, self._key)
                client = self._factory(data)
                await self._pool.add(client, data.name)
            except (SessionError, PoolFull, OSError) as exc:
                lock.release()
                self._failed[name] = mtime
                log.error("session file %s is not used: %s", name, exc)
                continue
            self._known[name] = (client, lock)
            log.warning("session file %s: the account joins the pool", name)

    async def start(self) -> None:
        await self.scan()
        self._task = asyncio.create_task(self._run(), name="session-watcher")

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.scan()
            except Exception:
                log.exception("session watcher: scan failed")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        for _, lock in self._known.values():
            lock.release()
        self._known.clear()
