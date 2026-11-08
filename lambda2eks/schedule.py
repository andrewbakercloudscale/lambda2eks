"""Convert EventBridge rule and EventBridge Scheduler expressions to Kubernetes CronJob schedules."""

from __future__ import annotations

import re
from datetime import datetime, timezone

_RATE = re.compile(r"^rate\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days)\s*\)$")
_CRON = re.compile(r"^cron\((.+)\)$")
# EventBridge Scheduler one-time schedule: at(yyyy-mm-ddThh:mm:ss), in the schedule's time zone.
_AT = re.compile(r"^at\((\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\)$")
# Year fields the shim can check: 2027, 2026-2028, 2026,2028.
_YEARS = re.compile(r"^\d{4}(-\d{4})?(,\d{4}(-\d{4})?)*$")
_DOW_NAMES = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"}


class ScheduleError(ValueError):
    pass


def to_k8s_cron(expression: str) -> tuple[str, list[str]]:
    """Return (five-field cron, warnings). The time zone is the CronJob's timeZone, not part of the cron."""
    expr = expression.strip()
    warnings: list[str] = []

    m = _AT.match(expr)
    if m:
        _year, month, day, hour, minute, second = (int(g) for g in m.groups())
        if second:
            warnings.append(f"{expression}: seconds are dropped; the run starts at {hour:02d}:{minute:02d}")
        return f"{minute} {hour} {day} {month} *", warnings

    m = _RATE.match(expr)
    if m:
        n, unit = int(m.group(1)), m.group(2).rstrip("s")
        if n <= 0:
            raise ScheduleError(f"invalid rate: {expression}")
        if unit == "minute":
            if n == 1:
                return "* * * * *", warnings
            if 60 % n:
                warnings.append(f"{expression}: {n} does not divide 60, so the CronJob restarts its count every hour")
            return f"*/{n} * * * *", warnings
        if unit == "hour":
            if n == 1:
                return "0 * * * *", warnings
            if 24 % n:
                warnings.append(f"{expression}: {n} does not divide 24, so the CronJob restarts its count every day")
            return f"0 */{n} * * *", warnings
        if n != 1:
            warnings.append(f"{expression}: every {n} days is approximated by day-of-month step */{n}")
            return f"0 0 */{n} * *", warnings
        return "0 0 * * *", warnings

    m = _CRON.match(expr)
    if not m:
        raise ScheduleError(f"unrecognised schedule expression: {expression}")
    fields = m.group(1).split()
    if len(fields) != 6:
        raise ScheduleError(f"EventBridge cron needs 6 fields: {expression}")
    minute, hour, dom, month, dow, year = fields
    if year not in ("*", "?") and _YEARS.match(year):
        warnings.append(f"{expression}: Kubernetes cron has no year; the CronJob fires every year and the shim skips runs outside {year}")
    elif year not in ("*", "?"):
        warnings.append(f"{expression}: year field '{year}' dropped; Kubernetes cron has no year and the shim checks only "
                        "plain years and ranges (2027, 2026-2028, 2026,2028)")
    for f in (dom, dow):
        if any(c in f for c in "LW#"):
            warnings.append(f"{expression}: '{f}' uses L/W/# which Kubernetes cron does not support; edit by hand")
    dom = "*" if dom == "?" else dom
    dow = "*" if dow == "?" else _convert_dow(dow)
    return f"{minute} {hour} {dom} {month} {dow}", warnings


def _convert_dow(field: str) -> str:
    # EventBridge numbers days 1-7 from Sunday; Kubernetes numbers them 0-6.
    def conv(token: str) -> str:
        if token.upper() in _DOW_NAMES or token == "*":
            return token.upper() if token != "*" else token
        if token.isdigit():
            n = int(token)
            if not 1 <= n <= 7:
                raise ScheduleError(f"day-of-week {n} out of range 1-7")
            return str(n - 1)
        return token

    out = []
    for part in field.split(","):
        step = ""
        if "/" in part:
            part, step = part.split("/", 1)
            step = "/" + step
        if "-" in part:
            a, b = part.split("-", 1)
            out.append(f"{conv(a)}-{conv(b)}{step}")
        else:
            out.append(conv(part) + step)
    return ",".join(out)


def schedule_years(expression: str) -> str | None:
    """The years a schedule may run in, for the shim to check (Kubernetes cron has no year field), or None for any year."""
    expr = expression.strip()
    m = _AT.match(expr)
    if m:
        return m.group(1)
    m = _CRON.match(expr)
    if m and len(fields := m.group(1).split()) == 6 and _YEARS.match(fields[5]):
        return fields[5]
    return None


def at_time(expression: str, tz: str | None) -> datetime | None:
    """When a one-time at() schedule runs, as an aware datetime, or None for a recurring schedule."""
    m = _AT.match(expression.strip())
    if not m:
        return None
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo(tz) if tz else timezone.utc
    except Exception:  # noqa: BLE001 - unknown zone or no tz database: UTC is the closest answer
        zone = timezone.utc
    return datetime(*(int(g) for g in m.groups()), tzinfo=zone)
