from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from agsi_pipeline.models import DuplicateKeyError
from agsi_pipeline.parsing import (
    HierarchyLevel,
    extract_countries,
    parse_value,
    walk_hierarchy,
)


@pytest.fixture
def daily_payload() -> dict:
    return json.loads(
        (Path(__file__).parent / "fixtures" / "daily-response.json").read_text(encoding="utf-8")
    )


def test_walk_hierarchy_levels(daily_payload: dict) -> None:
    records = walk_hierarchy(daily_payload["data"])
    levels = {record.level for record in records}
    assert HierarchyLevel.AGGREGATE in levels
    assert HierarchyLevel.COUNTRY in levels
    assert HierarchyLevel.OPERATOR in levels
    assert HierarchyLevel.FACILITY in levels


def test_extract_countries(daily_payload: dict) -> None:
    observed = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    rows = extract_countries(
        daily_payload,
        request_version=1,
        gas_day=date(2026, 7, 21),
        observed_at=observed,
    )
    codes = {row.country_code for row in rows}
    assert codes == {"DE", "GB"}
    de = next(row for row in rows if row.country_code == "DE")
    assert de.gas_in_storage == 180.5
    gb = next(row for row in rows if row.country_code == "GB")
    assert gb.gas_in_storage is None
    assert gb.full is None


def test_parse_value_unavailable() -> None:
    assert parse_value("-") is None
    assert parse_value("12.5") == 12.5


def test_duplicate_country_key_raises(daily_payload: dict) -> None:
    payload = json.loads(json.dumps(daily_payload))
    payload["data"][0]["children"].append(payload["data"][0]["children"][0])
    observed = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    with pytest.raises(DuplicateKeyError):
        extract_countries(
            payload,
            request_version=1,
            gas_day=date(2026, 7, 21),
            observed_at=observed,
        )
