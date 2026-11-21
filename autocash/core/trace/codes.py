"""Task statuses and the dictionary of reason codes (FT-11.4, FT-11.5)."""

from __future__ import annotations

from enum import StrEnum


class Status(StrEnum):
    IN_PROGRESS = "in_progress"
    WAITING = "waiting"
    DELIVERED = "delivered"
    PROCESSED = "processed"
    SKIPPED = "skipped"
    NOT_DELIVERED = "not_delivered"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    SIMULATION = "simulation"


FINAL = frozenset(
    {Status.DELIVERED, Status.PROCESSED, Status.SKIPPED, Status.NOT_DELIVERED, Status.EXPIRED, Status.CANCELLED,
     Status.SIMULATION}
)


class Reason(StrEnum):
    FLOOD_WAIT = "FLOOD_WAIT"
    ACCOUNT_PAUSED = "ACCOUNT_PAUSED"
    NO_ACCOUNT_IN_CHAT = "NO_ACCOUNT_IN_CHAT"
    DB_UNAVAILABLE = "DB_UNAVAILABLE"
    ACCOUNT_RESTRICTED = "ACCOUNT_RESTRICTED"
    ACCOUNT_LOST = "ACCOUNT_LOST"
    CHAT_DELETED = "CHAT_DELETED"
    CHAT_WRITE_FORBIDDEN = "CHAT_WRITE_FORBIDDEN"
    USER_IS_BLOCKED = "USER_IS_BLOCKED"
    TTL_EXPIRED = "TTL_EXPIRED"
    MODULE_ERROR = "MODULE_ERROR"
    MODULE_TIMEOUT = "MODULE_TIMEOUT"
    UNKNOWN_TYPE = "UNKNOWN_TYPE"
    ACCESS_DENIED = "ACCESS_DENIED"  # amendment to the TZ, see AGENTS.md
    CANCELLED_BY_USER = "CANCELLED_BY_USER"  # /cancel (stage 1c, docs/decisions.md D-1c-4)
    SCHEDULED = "SCHEDULED"  # an outbound task waits for its not_before (D-OBX-8)
    DELETE_FAILED = "DELETE_FAILED"  # no account of the pool could delete the message (D-OBX-8)


# code -> (meaning, typical status); seeded into the reason_codes table.
REASONS: dict[Reason, tuple[str, str]] = {
    Reason.FLOOD_WAIT: ("аккаунт или метод на паузе до времени T", "waiting"),
    Reason.ACCOUNT_PAUSED: ("нужный аккаунт в состоянии paused или disconnected", "waiting"),
    Reason.NO_ACCOUNT_IN_CHAT: ("в чате нет активного аккаунта пула", "waiting"),
    Reason.DB_UNAVAILABLE: ("PostgreSQL недоступен", "waiting"),
    Reason.ACCOUNT_RESTRICTED: ("аккаунт ограничен на отправку (PEER_FLOOD)", "waiting/not_delivered"),
    Reason.ACCOUNT_LOST: ("аккаунт заблокирован или сессия отозвана", "waiting/not_delivered"),
    Reason.CHAT_DELETED: ("чат удалён или аккаунт исключён", "not_delivered"),
    Reason.CHAT_WRITE_FORBIDDEN: ("у аккаунта нет права писать в чат", "not_delivered"),
    Reason.USER_IS_BLOCKED: ("клиент заблокировал аккаунт", "not_delivered"),
    Reason.TTL_EXPIRED: ("истёк срок жизни задачи", "expired"),
    Reason.MODULE_ERROR: ("ошибка модуля", "waiting"),
    Reason.MODULE_TIMEOUT: ("таймаут модуля", "waiting"),
    Reason.UNKNOWN_TYPE: ("сообщение без подходящего правила", "skipped"),
    Reason.ACCESS_DENIED: ("недостаточно прав", "cancelled"),
    Reason.CANCELLED_BY_USER: ("отменено командой оператора", "cancelled"),
    Reason.SCHEDULED: ("задача ждёт своего времени (not_before)", "waiting"),
    Reason.DELETE_FAILED: ("сообщение не удалось удалить ни с одного аккаунта", "not_delivered"),
}
