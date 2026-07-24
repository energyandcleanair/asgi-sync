from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path

import polars as pl
import pytest

from agsi_pipeline.datasets import build_as_of, build_current, build_history, publish_release
from agsi_pipeline.paths import (
    build_current_path,
    public_current_path,
    raw_response_path,
    silver_history_partition_path,
)
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


def test_build_history_multi_partition(storage, daily_payload: dict) -> None:
    observed_aug = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    observed_sep = datetime(2026, 9, 1, 2, 0, tzinfo=UTC)
    payload = json.loads(json.dumps(daily_payload))

    _store_snapshot(storage, gas_day=date(2026, 7, 21), observed_at=observed_aug, payload=payload)
    _store_snapshot(storage, gas_day=date(2026, 7, 22), observed_at=observed_sep, payload=payload)

    build_history(storage, 1)

    aug_key = silver_history_partition_path(1, observed_aug)
    sep_key = silver_history_partition_path(1, observed_sep)
    assert exists(storage.artifacts, aug_key)
    assert exists(storage.artifacts, sep_key)

    aug_frame = pl.read_parquet(BytesIO(read_bytes(storage.artifacts, aug_key)))
    sep_frame = pl.read_parquet(BytesIO(read_bytes(storage.artifacts, sep_key)))
    assert aug_frame.height == 2
    assert sep_frame.height == 2
    assert set(aug_frame["gas_day"].to_list()) == {date(2026, 7, 21)}
    assert set(sep_frame["gas_day"].to_list()) == {date(2026, 7, 22)}


def test_build_history_removes_orphan_partitions(storage, daily_payload: dict) -> None:
    observed = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    payload = json.loads(json.dumps(daily_payload))
    _store_snapshot(storage, gas_day=date(2026, 7, 21), observed_at=observed, payload=payload)

    orphan_key = silver_history_partition_path(1, datetime(2020, 1, 1, tzinfo=UTC))
    orphan_frame = pl.DataFrame({"request_version": [1], "gas_day": [date(2020, 1, 1)]})
    buffer = BytesIO()
    orphan_frame.write_parquet(buffer)
    atomic_write_bytes(storage.artifacts, orphan_key, buffer.getvalue())
    assert exists(storage.artifacts, orphan_key)

    build_history(storage, 1)

    assert not exists(storage.artifacts, orphan_key)
    assert exists(storage.artifacts, silver_history_partition_path(1, observed))


def test_build_history_many_snapshots(storage, daily_payload: dict) -> None:
    observed_old = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    observed_new = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
    start_day = date(2026, 1, 1)

    for offset in range(120):
        gas_day = start_day + timedelta(days=offset)
        payload = json.loads(json.dumps(daily_payload))
        _store_snapshot(storage, gas_day=gas_day, observed_at=observed_old, payload=payload)
        _store_snapshot(storage, gas_day=gas_day, observed_at=observed_new, payload=payload)

    build_history(storage, 1)
    build_current(storage, 1)

    current = pl.read_parquet(
        BytesIO(read_bytes(storage.artifacts, build_current_path(1)))
    )
    assert current.height == 240
    assert current["gas_day"].n_unique() == 120
