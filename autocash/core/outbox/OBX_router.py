"""OBX_Router: which account sends a task that is not bound to an account (D-OBX-3).

A task is BOUND when only one account may send it: a private dialog (the account the user wrote
to) or an account the module named. A bound task is not routed. An unbound task (a group, a
notification) goes to the account, among those that may write to the recipient, with the fewest
writes in the last minute plus tasks in its queue; on a tie, the account the scheduler gave the
sending duty for this chat (duties spread the chats over the accounts), then the one that wrote
longest ago.
Example: A sent 3, B 2, C 9 in the last minute -> B.

The decision is stored with the task: whether the router worked and the load of every candidate.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from autocash.core.outbox.OBX_antiflood import Antiflood


@dataclass(slots=True)
class Decision:
    account_id: int
    routed: bool  # True: chosen by the router among several accounts
    load: dict[int, int] = field(default_factory=dict)  # candidate -> writes last minute + queued


def choose(candidates: list[int], antiflood: Antiflood, queued: Callable[[int], int],
           preferred: int | None = None) -> Decision | None:
    if not candidates:
        return None
    load = {a: antiflood.recent(a) + queued(a) for a in candidates}
    best = min(candidates, key=lambda a: (load[a], a != preferred, antiflood.last_write(a), a))
    return Decision(best, len(candidates) > 1, load)
