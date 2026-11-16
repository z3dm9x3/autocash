"""Text of command replies. Pure functions over already collected data."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

MAX_TEXT = 3900  # Telegram allows 4096 characters per message

STATUS_RU = {
    "in_progress": "в работе", "waiting": "ожидает", "delivered": "доставлено", "processed": "обработано",
    "skipped": "пропущено", "not_delivered": "не доставлено", "expired": "истёк срок", "cancelled": "отменено",
    "simulation": "симуляция",
}


def clip(text: str) -> str:
    if len(text) <= MAX_TEXT:
        return text
    return text[: MAX_TEXT - 40].rstrip() + "\n… (вывод обрезан)"


def dur(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 1:
        return f"{seconds * 1000:.0f} мс"
    if seconds < 120:
        return f"{seconds:.1f} с"
    if seconds < 7200:
        return f"{seconds / 60:.0f} мин"
    return f"{seconds / 3600:.1f} ч"


def ts(value: datetime | float | None, tz: str) -> str:
    if value is None:
        return "—"
    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(value, ZoneInfo("UTC"))
    return value.astimezone(ZoneInfo(tz)).strftime("%d.%m %H:%M:%S")


def status(value: str | None, reason: str | None = None) -> str:
    text = STATUS_RU.get(value or "", value or "—")
    return f"{text} ({reason})" if reason else text


def _delta(a: datetime | None, b: datetime | None) -> float | None:
    if a is None or b is None:
        return None
    return (b - a).total_seconds()


def inbound_trace(t: Any, modules: list[Any], outbound: list[tuple[Any, list[Any]]], tz: str) -> str:
    lines = [
        f"Входящая {t['trace_id']}",
        f"чат {t['chat_id']}, сообщение {t['msg_id']}, событие {t['event']}"
        + (f", версия {t['version']}" if t["version"] else "")
        + f", аккаунт {t['account_id']}",
        f"тип: {t['msg_type'] or '—'}; статус: {status(t['status'], t['reason_code'])}",
    ]
    if t["waited_reason"]:
        lines.append(f"ожидала: {t['waited_reason']}")
    if t["stuck_at"] is not None:
        lines.append(f"⚠️ зависла на этапе {t['stuck_stage']} ({ts(t['stuck_at'], tz)})")
    stages = [("принято", "accepted_at"), ("извлечено", "extracted_at"), ("в буфере", "buffered_at"),
              ("в PostgreSQL", "in_pg_at"), ("отсортировано", "sorted_at")]
    prev = None
    for title, col in stages:
        value = t[col]
        lines.append(f"  {title}: {ts(value, tz)}" + (f" (+{dur(_delta(prev, value))})" if prev and value else ""))
        prev = value or prev
    for m in modules:
        line = f"  модуль {m['module']}: {status(m['status'], m['reason_code'])}"
        line += f", в очереди {ts(m['queued_at'], tz)}"
        if m["finished_at"] is not None:
            line += f", обработано за {dur(_delta(m['queued_at'], m['finished_at']))}"
        if m["error"]:
            line += f"\n    ошибка: {m['error'][:200]}"
        if m["stuck_at"] is not None:
            line += " ⚠️ зависла"
        lines.append(line)
    if t["finished_at"] is not None:
        lines.append(f"  финал: {ts(t['finished_at'], tz)} (всего {dur(_delta(t['accepted_at'], t['finished_at']))})")
    for o, attempts in outbound:
        lines.append(outbound_trace(o, attempts, tz, indent="  "))
    return "\n".join(lines)


def outbound_trace(o: Any, attempts: list[Any], tz: str, indent: str = "") -> str:
    lines = [
        f"{indent}Исходящая #{o['id']} ({o['trace_id']})",
        f"{indent}  {o['kind']} в чат {o['chat_id']}, правило {o['rule'] or '—'}, модуль {o['module'] or '—'}",
        f"{indent}  этап: {o['stage']}; статус: {status(o['status'], o['reason_code'])}"
        + (f"; аккаунт {o['account_id']}" if o["account_id"] else ""),
    ]
    if o["last_wait_reason"] and o["last_wait_reason"] != o["reason_code"]:
        lines.append(f"{indent}  последняя причина ожидания: {o['last_wait_reason']}")
    if o["wait_until"] is not None and o["finished_at"] is None:
        lines.append(f"{indent}  ждёт до {ts(o['wait_until'], tz)}")
    if o["cancelled_by"]:
        lines.append(f"{indent}  отменил: {o['cancelled_by']}")
    if o["stuck_at"] is not None:
        lines.append(f"{indent}  ⚠️ зависла на этапе {o['stuck_stage']}")
    created = o["created_at"]
    for title, col in (("создана", "created_at"), ("назначен аккаунт", "assigned_at"), ("отправка", "sending_at"),
                       ("финал", "finished_at")):
        if o[col] is not None:
            lines.append(f"{indent}  {title}: {ts(o[col], tz)}"
                         + (f" (+{dur(_delta(created, o[col]))})" if col != "created_at" else ""))
    if o["delivered_msg_id"]:
        lines.append(f"{indent}  доставлено, msg_id {o['delivered_msg_id']}")
    for a in attempts:
        lines.append(f"{indent}  попытка {ts(a['at'], tz)}: аккаунт {a['account_id']}, {a['result']}"
                     + (f", {a['error_code']}" if a["error_code"] else ""))
    return "\n".join(lines)


def accounts(rows: list[dict[str, Any]], tz: str) -> str:
    if not rows:
        return "Пул пуст: нет ни одного аккаунта."
    lines = [f"Аккаунты пула: {len(rows)}"]
    for r in rows:
        line = f"• {r['account_id']} {r['name']} ({r['kind']}): {r['state']}"
        if r["reason"]:
            line += f" — {r['reason']}"
        line += f", с {ts(r['since'], tz)}"
        if r["paused_until"]:
            line += f"\n  пауза FloodWait до {ts(r['paused_until'], tz)}"
        line += f"\n  бюджет: {r['budget'] * 100:.0f}%, запросов в работе: {r['load']}"
        duties = ", ".join(f"{k}: {v}" for k, v in r["duties"].items() if v)
        line += f"\n  обязанности: {duties or 'нет'}"
        lines.append(line)
    return "\n".join(lines)


def coverage(under: list[tuple[int, str, int, int]], approx: list[tuple[int, str]]) -> str:
    lines = []
    if under:
        lines.append(f"Покрытие ниже требуемого: {len(under)}")
        lines += [f"• {name} ({chat}): {have} из {need}" + (" ⚠️" if have == 0 else "")
                  for chat, name, have, need in under]
    else:
        lines.append("Все источники покрыты не ниже требуемого.")
    if approx:
        lines.append(f"Приблизительная дедупликация (обычные группы): {len(approx)}")
        lines += [f"• {name} ({chat})" for chat, name in approx]
    return "\n".join(lines)


def queues(modules: dict[str, dict[str, Any]], output: dict[str, Any]) -> str:
    lines = ["Очереди модулей:"]
    for name, q in modules.items():
        lanes = q["lanes"]
        lines.append(f"• {name}: {q['size']} (команды {lanes[0]}, живые {lanes[1]}, история {lanes[2]}), "
                     f"в работе {q['running']}, обработано {q['processed']}, ошибок {q['failed']}"
                     + (", догоняет из PostgreSQL" if q["overflow"] else ""))
    lines.append("Вывод по аккаунтам:")
    queued = output.get("queued", {})
    lines += [f"• {acc}: {n}" for acc, n in queued.items()] or ["• очередей нет"]
    waiting = output.get("waiting", {})
    if waiting:
        lines.append("Ожидают: " + ", ".join(f"{r}: {n}" for r, n in waiting.items()))
    lines.append(f"Доставлено {output.get('delivered', 0)}, неуспешных финалов {output.get('failed', 0)}")
    return "\n".join(lines)


def stuck(by_stage: dict[str, dict[str, int]], examples: dict[str, list[str]], titles: dict[str, str],
          norms: dict[str, float]) -> str:
    if not by_stage:
        return "Зависших задач нет."
    lines = ["Зависшие задачи:"]
    for stage, reasons in sorted(by_stage.items()):
        total = sum(reasons.values())
        lines.append(f"• {stage} ({titles.get(stage, stage)}, норматив {norms.get(stage, 0):g} с): {total}")
        lines += [f"    {reason or 'в работе'}: {n}" for reason, n in sorted(reasons.items(), key=lambda x: -x[1])]
        lines += [f"    {e}" for e in examples.get(stage, [])]
    return "\n".join(lines)


def uptime(started: float) -> str:
    return dur(time.time() - started)
