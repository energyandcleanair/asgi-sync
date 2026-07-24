from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from agsi_pipeline.datasets import build_as_of, build_current, build_history, publish_release
from agsi_pipeline.paths import build_current_path, public_current_path, raw_response_path
from agsi_pipeline.storage import atomic_write_bytes, exists, read_bytes


@pytest.fixture
def daily_payload() -> dict:
    return json.loads(
        (Path(__file__).parent / "fixtures" / "daily-response.json").read_text(encoding="utf-8")
    )


def _store_snapshot(
    storage,
    *,
    gas_day: date,
    observed_at: datetime,
    payload: dict,
    request_version: int = 1,
) -> str:
    key = raw_response_path(request_version, gas_day, observed_at)
    body = json.dumps(payload).encode("utf-8")
    atomic_write_bytes(storage.artifacts, key, gzip.compress(body))
    return key


def test_build_history_and_current(storage, daily_payload: dict, tmp_path) -> None:
    observed_old = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    observed_new = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
    payload_v1 = json.loads(json.dumps(daily_payload))
    payload_v2 = json.loads(json.dumps(daily_payload))
    payload_v2["data"][0]["children"][0]["full"] = "80.0"

    _store_snapshot(storage, gas_day=date(2026, 7, 21), observed_at=observed_old, payload=payload_v1)
    _store_snapshot(storage, gas_day=date(2026, 7, 21), observed_at=observed_new, payload=payload_v2)

    build_history(storage, 1)
    build_current(storage, 1)

    build_key = build_current_path(1)
    assert exists(storage.artifacts, build_key)
    publish_release(storage, 1)
    public_key = public_current_path(1)
    assert exists(storage.public, public_key)

    from io import BytesIO

    import polars as pl

    current = pl.read_parquet(BytesIO(read_bytes(storage.artifacts, build_key)))
    de = current.filter(pl.col("country_code") == "DE")
    assert de["full"][0] == 80.0

    as_of_path = tmp_path / "as-of.parquet"
    build_as_of(
        storage,
        request_version=1,
        as_of=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
        output=str(as_of_path),
    )
    as_of_df = pl.read_parquet(as_of_path)
    de_old = as_of_df.filter(pl.col("country_code") == "DE")
    assert de_old["full"][0] == 75.2
