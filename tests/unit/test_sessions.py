"""Encrypted session files (FT-1.9): round trip, tampering, wrong key, one session - one process."""

from __future__ import annotations

import base64
import stat
import sys
from pathlib import Path

import pytest

from autocash.core.pool.sessions import (
    KEY_ENV,
    KEY_FILE_ENV,
    SUFFIX,
    SessionData,
    SessionError,
    SessionLock,
    decrypt,
    encrypt,
    load_key,
    new_key,
    read_session,
    write_session,
)

KEY = bytes(range(32))
DATA = SessionData(name="касса-1", kind="user", api_id=12345, api_hash="0123456789abcdef", session="1BVtsOK-secret")


def test_round_trip_and_nothing_readable_in_the_file(tmp_path: Path) -> None:
    path = write_session(tmp_path / "sessions", DATA, KEY)
    assert path.name == f"касса-1{SUFFIX}"
    blob = path.read_bytes()
    for secret in (DATA.session, DATA.api_hash, "12345", "касса"):
        assert secret.encode("utf-8") not in blob
    assert read_session(path, KEY) == DATA
    assert encrypt(DATA, KEY) != encrypt(DATA, KEY)  # a fresh nonce every time
    if sys.platform != "win32":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600  # only the service user may read it


def test_wrong_key_and_tampering_are_detected() -> None:
    blob = encrypt(DATA, KEY)
    with pytest.raises(SessionError, match="damaged or the key is wrong"):
        decrypt(blob, bytes(32))
    for position in (5, 25, len(blob) - 1):
        broken = bytearray(blob)
        broken[position] ^= 1
        with pytest.raises(SessionError):
            decrypt(bytes(broken), KEY)
    with pytest.raises(SessionError, match="not an Autocash session"):
        decrypt(b"SQLite format 3\x00" + bytes(64), KEY)


def test_key_comes_from_the_environment_or_a_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(KEY_ENV, raising=False)
    monkeypatch.delenv(KEY_FILE_ENV, raising=False)
    with pytest.raises(SessionError, match="not set"):
        load_key()
    encoded = new_key()
    assert len(base64.b64decode(encoded)) == 32
    monkeypatch.setenv(KEY_ENV, encoded)
    assert load_key() == base64.b64decode(encoded)
    monkeypatch.delenv(KEY_ENV)
    key_file = tmp_path / "key"
    key_file.write_text(encoded + "\n", encoding="ascii")
    monkeypatch.setenv(KEY_FILE_ENV, str(key_file))
    assert load_key() == base64.b64decode(encoded)
    monkeypatch.setenv(KEY_ENV, base64.b64encode(b"short").decode())
    with pytest.raises(SessionError, match="32 bytes"):
        load_key()


def test_one_session_one_process_lock(tmp_path: Path) -> None:
    path = write_session(tmp_path, DATA, KEY)
    first, second = SessionLock(path), SessionLock(path)
    assert first.acquire()
    assert not second.acquire()  # the same session cannot be opened twice
    first.release()
    assert second.acquire()
    second.release()
