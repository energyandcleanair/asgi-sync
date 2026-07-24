from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta


def parse_gas_day(value: str) -> date:
    return date.fromisoformat(value)


def latest_gas_day(today_utc: date, override: date | None = None) -> date:
    if override is not None:
        return override
    return today_utc - timedelta(days=1)


def iter_gas_days(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
