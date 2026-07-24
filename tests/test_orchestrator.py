from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import respx

from agsi_pipeline.client import AgsiClient
from agsi_pipeline.config import SyncPolicy, load_sync_policy
from agsi_pipeline.fetching import RateLimiter, fetch_and_store_day, reconcile
from agsi_pipeline.orchestrator import OrchestratorError, run_sync
from agsi_pipeline.paths import raw_response_path
from agsi_pipeline.storage import read_bytes


@pytest.fixture
def daily_payload() -> dict:
    return json.loads(
        (Path(__file__).parent / "fixtures" / "daily-response.json").read_text(encoding="utf-8")
    )


@respx.mock
def test_fetch_and_store_idempotent_within_job(storage, daily_payload: dict) -> None:
    respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json=daily_payload)
    )
    observed = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    limiter = RateLimiter(requests_per_minute=6000)
    fetch_and_store_day(
        date(2026, 7, 21),
        1,
        observed,
        client=client,
        storage=storage,
        rate_limiter=limiter,
    )
    first = read_bytes(storage.artifacts, raw_response_path(1, date(2026, 7, 21), observed))
    fetch_and_store_day(
        date(2026, 7, 21),
        1,
        observed,
        client=client,
        storage=storage,
        rate_limiter=limiter,
    )
    second = read_bytes(storage.artifacts, raw_response_path(1, date(2026, 7, 21), observed))
    assert first == second
    client.close()


@respx.mock
def test_reconcile_reports_failed_dates(storage, daily_payload: dict, monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        gas_day = request.url.params.get("date")
        if gas_day == "2026-07-21":
            return httpx.Response(200, json=daily_payload)
        return httpx.Response(500, json={"error": "fail"})

    respx.get("https://agsi.gie.eu/api").mock(side_effect=handler)

    import agsi_pipeline.fetching as fetching_mod

    original_fetch = fetching_mod.fetch_and_store_day

    def fetch_with_single_attempt(*args, **kwargs):
        kwargs["max_attempts"] = 1
        return original_fetch(*args, **kwargs)

    monkeypatch.setattr(fetching_mod, "fetch_and_store_day", fetch_with_single_attempt)

    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    result = reconcile(
        start=date(2026, 7, 21),
        end=date(2026, 7, 22),
        request_version=1,
        observed_at=datetime(2026, 8, 1, 2, 0, tzinfo=UTC),
        client=client,
        storage=storage,
        rate_limiter=RateLimiter(requests_per_minute=6000),
    )
    assert result.failed_dates == [date(2026, 7, 22)]
    client.close()


@respx.mock
def test_orchestrator_updates_state_only_after_success(storage, daily_payload: dict, policy_file) -> None:
    respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json=daily_payload)
    )
    policy = load_sync_policy(policy_file)
    policy = SyncPolicy(
        request_version=1,
        history_start_date=date(2026, 7, 21),
        recent_days=1,
        reconciliation_interval_days=31,
    )
    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    with patch("agsi_pipeline.orchestrator.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 23, tzinfo=UTC)
        run_sync(
            storage=storage,
            policy=policy,
            client=client,
            rate_limiter=RateLimiter(requests_per_minute=6000),
            force_reconcile=True,
            today=date(2026, 7, 23),
            latest_gas_day_override=date(2026, 7, 21),
        )
    from agsi_pipeline.state import read_sync_state_or_none

    state = read_sync_state_or_none(storage)
    assert state is not None
    assert state.last_successful_reconciliation_date == date(2026, 7, 23)
    client.close()


@respx.mock
def test_orchestrator_failure_leaves_state_unchanged(storage, daily_payload: dict, policy_file) -> None:
    respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(500, json={"error": "fail"})
    )
    policy = load_sync_policy(policy_file)
    policy = SyncPolicy(
        request_version=1,
        history_start_date=date(2026, 7, 21),
        recent_days=1,
        reconciliation_interval_days=31,
    )
    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="key")
    with pytest.raises(OrchestratorError):
        run_sync(
            storage=storage,
            policy=policy,
            client=client,
            rate_limiter=RateLimiter(requests_per_minute=6000),
            force_reconcile=True,
            today=date(2026, 7, 23),
            latest_gas_day_override=date(2026, 7, 21),
        )
    from agsi_pipeline.state import read_sync_state_or_none

    assert read_sync_state_or_none(storage) is None
    client.close()
