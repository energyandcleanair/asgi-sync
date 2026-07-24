from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from agsi_pipeline.client import AgsiClient
from agsi_pipeline.fetching import RateLimiter, reconcile, should_skip_gas_day
from agsi_pipeline.paths import raw_response_path
from agsi_pipeline.storage import atomic_write_bytes, exists


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


def test_should_skip_gas_day_recent_snapshot() -> None:
    now = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    observed = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    assert should_skip_gas_day(observed, now=now, resume_days=3, force=False)


def test_should_skip_gas_day_stale_snapshot() -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    observed = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    assert not should_skip_gas_day(observed, now=now, resume_days=3, force=False)


def test_should_skip_gas_day_force() -> None:
    now = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    observed = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    assert not should_skip_gas_day(observed, now=now, resume_days=3, force=True)


def test_should_skip_gas_day_missing_snapshot() -> None:
    now = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    assert not should_skip_gas_day(None, now=now, resume_days=3, force=False)


@respx.mock
def test_reconcile_skips_recent_snapshot(storage, daily_payload: dict) -> None:
    gas_day = date(2026, 7, 21)
    job_time = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    prior_observed = job_time - timedelta(days=1)
    _store_snapshot(storage, gas_day=gas_day, observed_at=prior_observed, payload=daily_payload)

    route = respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json=daily_payload)
    )

    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    result = reconcile(
        start=gas_day,
        end=gas_day,
        request_version=1,
        observed_at=job_time,
        client=client,
        storage=storage,
        rate_limiter=RateLimiter(requests_per_minute=6000),
        resume_days=3,
    )
    client.close()

    assert result.skipped_dates == [gas_day]
    assert result.failed_dates == []
    assert route.call_count == 0


@respx.mock
def test_reconcile_fetches_missing_day(storage, daily_payload: dict) -> None:
    gas_day = date(2026, 7, 21)
    job_time = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)

    route = respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json=daily_payload)
    )

    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    result = reconcile(
        start=gas_day,
        end=gas_day,
        request_version=1,
        observed_at=job_time,
        client=client,
        storage=storage,
        rate_limiter=RateLimiter(requests_per_minute=6000),
        resume_days=3,
    )
    client.close()

    assert result.skipped_dates == []
    assert result.failed_dates == []
    assert route.call_count == 1
    assert exists(
        storage.artifacts,
        raw_response_path(1, gas_day, job_time),
    )


@respx.mock
def test_reconcile_refetches_stale_snapshot(storage, daily_payload: dict) -> None:
    gas_day = date(2026, 7, 21)
    job_time = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    prior_observed = job_time - timedelta(days=4)
    _store_snapshot(storage, gas_day=gas_day, observed_at=prior_observed, payload=daily_payload)

    route = respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json=daily_payload)
    )

    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    result = reconcile(
        start=gas_day,
        end=gas_day,
        request_version=1,
        observed_at=job_time,
        client=client,
        storage=storage,
        rate_limiter=RateLimiter(requests_per_minute=6000),
        resume_days=3,
    )
    client.close()

    assert result.skipped_dates == []
    assert result.failed_dates == []
    assert route.call_count == 1
    assert exists(
        storage.artifacts,
        raw_response_path(1, gas_day, job_time),
    )


@respx.mock
def test_reconcile_force_refetches_recent_snapshot(storage, daily_payload: dict) -> None:
    gas_day = date(2026, 7, 21)
    job_time = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    prior_observed = job_time - timedelta(days=1)
    _store_snapshot(storage, gas_day=gas_day, observed_at=prior_observed, payload=daily_payload)

    route = respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json=daily_payload)
    )

    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    result = reconcile(
        start=gas_day,
        end=gas_day,
        request_version=1,
        observed_at=job_time,
        client=client,
        storage=storage,
        rate_limiter=RateLimiter(requests_per_minute=6000),
        force=True,
        resume_days=3,
    )
    client.close()

    assert result.skipped_dates == []
    assert result.failed_dates == []
    assert route.call_count == 1
    assert exists(
        storage.artifacts,
        raw_response_path(1, gas_day, job_time),
    )
