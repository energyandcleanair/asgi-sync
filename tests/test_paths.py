from __future__ import annotations

from datetime import UTC, date, datetime

from agsi_pipeline.paths import (
    BucketTarget,
    bucket_for_path,
    build_current_path,
    format_observed_at,
    parse_observed_at,
    public_current_path,
    raw_response_path,
    sync_state_path,
)


def test_raw_response_path() -> None:
    observed = datetime(2026, 8, 1, 2, 0, 0, tzinfo=UTC)
    path = raw_response_path(1, date(2026, 7, 21), observed)
    assert path == (
        "agsi/raw/request_version=1/date=2026-07-21/"
        "observed_at=2026-08-01T020000Z/response.json.gz"
    )


def test_observed_at_round_trip() -> None:
    observed = datetime(2026, 8, 1, 2, 0, 0, tzinfo=UTC)
    text = format_observed_at(observed)
    assert parse_observed_at(text) == observed


def test_bucket_routing() -> None:
    assert bucket_for_path(build_current_path(1)) == BucketTarget.ARTIFACTS
    assert bucket_for_path(public_current_path(1)) == BucketTarget.PUBLIC
    assert bucket_for_path(sync_state_path()) == BucketTarget.ARTIFACTS
