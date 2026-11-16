"""Login to the admin (FT-13.5, item 5; D-5-3).

A user is a Telegram user id with a password set by `python -m autocash.cli.webuser`. Rights
come from the configuration, the same matrix as for commands: the pseudo-commands below, or
"*", for the user's global roles. A session is a random cookie; only its sha256 is stored.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time

from autocash.core.commands.rights import ANY, allowed_commands
from autocash.core.config.loader import Runtime

# Pseudo-commands of the matrix `permissions` that open parts of the admin.
WEB_READ = "web"  # every page that only reads
WEB_EDIT = "web_edit"  # configuration
WEB_SEND = "web_send"  # sending a message through the outbound queue
WEB_POOL = "web_pool"  # pool actions (remove, restrict, rescan)
WEB_RIGHTS = (WEB_READ, WEB_EDIT, WEB_SEND, WEB_POOL)

_N, _R, _P = 2**14, 8, 1


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P, dklen=32)
    return f"scrypt${_N}${_R}${_P}${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}"


def check_password(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt, digest = stored.split("$")
        if scheme != "scrypt":
            return False
        got = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p),
                             dklen=len(base64.b64decode(digest)))
        return hmac.compare_digest(got, base64.b64decode(digest))
    except (ValueError, TypeError):
        return False


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def web_rights(rt: Runtime, tg_id: int) -> set[str]:
    """Rights of the admin from the global roles of the user (chat 0: no source role applies)."""
    allowed = allowed_commands(rt, tg_id, 0)
    if ANY in allowed:
        return set(WEB_RIGHTS)
    rights = {r for r in WEB_RIGHTS if r in allowed}
    if rights:
        rights.add(WEB_READ)
    return rights


class LoginThrottle:
    """At most `limit` failed logins per user id or address in `window_s` seconds."""

    def __init__(self, limit: int = 5, window_s: float = 300.0) -> None:
        self.limit = limit
        self.window_s = window_s
        self._fails: dict[str, list[float]] = {}

    def blocked(self, *keys: str) -> bool:
        now = time.monotonic()
        for key in keys:
            fresh = [t for t in self._fails.get(key, []) if now - t < self.window_s]
            self._fails[key] = fresh
            if len(fresh) >= self.limit:
                return True
        return False

    def fail(self, *keys: str) -> None:
        now = time.monotonic()
        for key in keys:
            self._fails.setdefault(key, []).append(now)

    def ok(self, *keys: str) -> None:
        for key in keys:
            self._fails.pop(key, None)
