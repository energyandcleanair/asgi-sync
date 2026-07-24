from __future__ import annotations

import json
from pathlib import Path

import pytest

from agsi_pipeline.fetching import validate_response
from agsi_pipeline.models import InvalidResponseError


@pytest.fixture
def daily_payload() -> dict:
    return json.loads(
        (Path(__file__).parent / "fixtures" / "daily-response.json").read_text(encoding="utf-8")
    )


def test_validate_response_accepts_fixture(daily_payload: dict) -> None:
    validate_response(daily_payload, gas_day=__import__("datetime").date(2026, 7, 21))


def test_validate_response_rejects_empty_data(daily_payload: dict) -> None:
    payload = dict(daily_payload)
    payload["data"] = []
    with pytest.raises(InvalidResponseError):
        validate_response(payload, gas_day=__import__("datetime").date(2026, 7, 21))


def test_validate_response_rejects_gas_day_mismatch(daily_payload: dict) -> None:
    payload = {
        "gas_day": "2026-07-21",
        "data": [{"name": "EU", "code": "eu", "gasDayStart": "2026-07-20", "children": []}],
    }
    with pytest.raises(InvalidResponseError):
        validate_response(payload, gas_day=__import__("datetime").date(2026, 7, 21))


def test_validate_response_accepts_gas_day_end_match() -> None:
    payload = {
        "gas_day": "2026-07-21",
        "data": [
            {
                "name": "EU",
                "code": "eu",
                "gasDayStart": "2026-07-21",
                "gasDayEnd": "2026-07-22",
                "children": [],
            }
        ],
    }
    validate_response(payload, gas_day=__import__("datetime").date(2026, 7, 22))
