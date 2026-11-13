"""Named crash points for recovery tests (kill -9 at an exact place). No-op in production.

Enabled only by AUTOCASH_CRASHPOINT=<name> [AUTOCASH_CRASHPOINT_AFTER=<n>]: the process
terminates without any cleanup on the n-th hit of the named point.
"""

from __future__ import annotations

import os

_POINT = os.environ.get("AUTOCASH_CRASHPOINT", "")
_AFTER = int(os.environ.get("AUTOCASH_CRASHPOINT_AFTER", "1"))
_hits = 0


def hit(name: str) -> None:
    global _hits
    if _POINT and name == _POINT:
        _hits += 1
        if _hits >= _AFTER:
            os._exit(137)
