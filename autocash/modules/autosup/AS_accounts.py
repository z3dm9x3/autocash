"""AS: the parameters of an account of the pool - its working hours (AS_readme.md, D-AS-8).

The file `account.yaml` in the folder of the account (accounts/acc_<TGID> <name>/):

    work_hours: "11:00-23:00"                  # Moscow time, the end within the same day (24:00: to midnight)
    work_days: [пн, вт, ср, чт, пт, сб, вс]

No file, or no key: every day 11:00-23:00. Autosup reads the files at start and the file of an account added
to the running pool (the protocol pool.account_added); never in between.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

MSK = ZoneInfo("Europe/Moscow")
FILE = "account.yaml"
DAYS = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")
_HOURS = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$")
_FOLDER = re.compile(r"^acc_(\d+)(?:\s|$)")


@dataclass(frozen=True, slots=True)
class Schedule:
    start: time
    end: time
    days: frozenset[int]  # 0 = Monday

    def is_open(self, at: datetime) -> bool:
        local = at.astimezone(MSK)
        return local.weekday() in self.days and self.start <= local.time() < self.end

    def closed_since(self, at: datetime) -> datetime:
        """The start of the non-working period `at` is in: the end of the last working day before it."""
        local = at.astimezone(MSK)
        for back in range(8):
            day = local.date() - timedelta(days=back)
            if day.weekday() not in self.days:
                continue
            end = datetime.combine(day, self.end, MSK)
            if end <= local:
                return end
        return datetime.combine(local.date() - timedelta(days=7), time(0), MSK)

    def describe(self) -> str:
        days = sorted(self.days)
        if days == list(range(7)):
            when = "ежедневно"
        elif days and days == list(range(days[0], days[-1] + 1)):
            when = f"{DAYS[days[0]]}–{DAYS[days[-1]]}"
        else:
            when = ", ".join(DAYS[d] for d in days) or "нет рабочих дней"
        end = "24:00" if self.end == time.max else f"{self.end:%H:%M}"
        return f"{when} {self.start:%H:%M}–{end}"


DEFAULT = Schedule(time(11, 0), time(23, 0), frozenset(range(7)))


class ScheduleError(ValueError):
    pass


def parse(data: object) -> Schedule:
    """The parameters of an account file; a missing key takes the default."""
    if data is None:
        return DEFAULT
    if not isinstance(data, dict):
        raise ScheduleError("файл должен содержать ключи work_hours и work_days")
    start, end = DEFAULT.start, DEFAULT.end
    hours = data.get("work_hours")
    if hours is not None:
        m = _HOURS.match(str(hours))
        if m is None:
            raise ScheduleError(f"work_hours «{hours}»: нужен вид 11:00-23:00")
        try:
            start = time(int(m[1]), int(m[2]))
            end = time.max if (m[3], m[4]) == ("24", "00") else time(int(m[3]), int(m[4]))
        except ValueError:
            raise ScheduleError(f"work_hours «{hours}»: такого времени нет") from None
        if end <= start:
            raise ScheduleError(f"work_hours «{hours}»: конец должен быть позже начала в тот же день")
    days = DEFAULT.days
    raw_days = data.get("work_days")
    if raw_days is not None:
        if not isinstance(raw_days, list):
            raise ScheduleError("work_days: нужен список, например [пн, вт, ср, чт, пт]")
        names = [str(d).strip().lower() for d in raw_days]
        wrong = [d for d in names if d not in DAYS]
        if wrong:
            raise ScheduleError(f"work_days: неизвестные дни {', '.join(wrong)}; дни: {', '.join(DAYS)}")
        days = frozenset(DAYS.index(d) for d in names)
    return Schedule(start, end, days)


def folder_of(accounts_dir: Path, account_id: int) -> Path | None:
    """accounts/acc_<TGID> <name> (or accounts/acc_<TGID>) of this account."""
    if not accounts_dir.is_dir():
        return None
    for path in sorted(accounts_dir.iterdir()):
        m = _FOLDER.match(path.name)
        if path.is_dir() and m is not None and int(m[1]) == account_id:
            return path
    return None


def load(accounts_dir: Path, account_id: int) -> tuple[Schedule, Path | None]:
    """The schedule of the account and its file (None: no file, the default). ScheduleError: a broken file."""
    folder = folder_of(accounts_dir, account_id)
    path = folder / FILE if folder is not None else None
    if path is None or not path.is_file():
        return DEFAULT, None
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ScheduleError(f"{path.name}: не читается ({exc})") from None
    return parse(data), path


def account_ids(accounts_dir: Path) -> list[int]:
    """TGIDs of the folders of accounts (acc_<TGID> …)."""
    if not accounts_dir.is_dir():
        return []
    ids = [_FOLDER.match(p.name) for p in accounts_dir.iterdir() if p.is_dir()]
    return sorted({int(m[1]) for m in ids if m is not None})


TEMPLATE = """\
# Параметры аккаунта пула. Autosup читает этот файл при старте Autocash и когда аккаунт добавляется в пул.
# Время московское; конец рабочего времени — в тот же день.
work_hours: "11:00-23:00"
work_days: [пн, вт, ср, чт, пт, сб, вс]
"""
