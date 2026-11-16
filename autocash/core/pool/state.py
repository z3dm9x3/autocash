"""Account states and the table of Telegram errors that change the pool (FT-1.3, FT-10.5)."""

from __future__ import annotations

from enum import StrEnum

from autocash.core.trace.codes import Reason


class AccountState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"  # FloodWait on one or several methods until a known time
    RESTRICTED = "restricted"  # PEER_FLOOD: cannot send, still receives
    DISCONNECTED = "disconnected"
    LOST = "lost"  # session revoked or account banned
    REMOVED = "removed"


# States counted in the coverage of a chat (FT-1.5).
COVERING = frozenset({AccountState.ACTIVE, AccountState.PAUSED, AccountState.RESTRICTED})

# Reason a task waits for an account in this state (FT-9.4).
WAIT_REASON: dict[AccountState, Reason] = {
    AccountState.PAUSED: Reason.ACCOUNT_PAUSED,
    AccountState.DISCONNECTED: Reason.ACCOUNT_PAUSED,
    AccountState.RESTRICTED: Reason.ACCOUNT_RESTRICTED,
    AccountState.LOST: Reason.ACCOUNT_LOST,
    AccountState.REMOVED: Reason.ACCOUNT_LOST,
}

RESTRICTING = frozenset({"PEER_FLOOD"})
LOSING = frozenset({"USER_DEACTIVATED_BAN", "USER_DEACTIVATED", "AUTH_KEY_UNREGISTERED", "SESSION_REVOKED",
                    "AUTH_KEY_DUPLICATED"})
# The "account - chat" link is removed from the membership map.
CHAT_GONE = frozenset({"CHANNEL_PRIVATE", "USER_KICKED", "CHAT_ID_INVALID", "CHANNEL_INVALID", "PEER_ID_INVALID",
                       "USER_NOT_PARTICIPANT", "CHAT_FORBIDDEN"})
# The account stays a reader of the chat and leaves its senders.
WRITE_FORBIDDEN = frozenset({"CHAT_WRITE_FORBIDDEN", "USER_BANNED_IN_CHANNEL", "CHAT_SEND_PLAIN_FORBIDDEN",
                             "CHAT_GUEST_SEND_FORBIDDEN", "CHAT_ADMIN_REQUIRED"})
BLOCKED = frozenset({"USER_IS_BLOCKED"})
