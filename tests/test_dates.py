from __future__ import annotations

from datetime import date

import pytest

from agsi_pipeline.dates import iter_gas_days, latest_gas_day, parse_gas_day


def test_latest_gas_day_default() -> None:
    assert latest_gas_day(date(2026, 7, 23)) == date(2026, 7, 22)


def test_latest_gas_day_override() -> None:
    assert latest_gas_day(date(2026, 7, 23), date(2026, 7, 20)) == date(2026, 7, 20)


def test_iter_gas_days_inclusive() -> None:
    days = list(iter_gas_days(date(2026, 7, 1), date(2026, 7, 3)))
    assert days == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]


def test_iter_gas_days_invalid() -> None:
    with pytest.raises(ValueError):
        list(iter_gas_days(date(2026, 7, 3), date(2026, 7, 1)))


def test_parse_gas_day() -> None:
    assert parse_gas_day("2026-07-21") == date(2026, 7, 21)
