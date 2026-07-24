from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import polars as pl
import pytest

from agsi_pipeline.client import AgsiClient
from agsi_pipeline.config import Settings, load_sync_policy
from agsi_pipeline.datasets import build_current, build_history, publish_release
from agsi_pipeline.fetching import RateLimiter, refresh_recent
from agsi_pipeline.paths import build_current_path, public_current_path, silver_history_prefix
from agsi_pipeline.storage import exists, list_files, read_bytes
from tests.integration.cassettes import CASSETTE_NAME, gas_days_from_manifest, make_vcr


@pytest.mark.integration
def test_recorded_pipeline(
    cassette_manifest: dict[str, object],
    integration_policy: Path,
    local_storage,
    integration_env: None,
) -> None:
    settings = Settings()
    policy = load_sync_policy(integration_policy)
    gas_days = gas_days_from_manifest(cassette_manifest)
    end = max(gas_days)
    request_count = int(cassette_manifest["request_count"])

    my_vcr = make_vcr(record_mode="none")
    with (
        my_vcr.use_cassette(CASSETTE_NAME, allow_playback_repeats=True),
        AgsiClient(
            base_url=settings.api_base_url,
            api_key=settings.api_key_value(),
        ) as client,
    ):
        result = refresh_recent(
            days=request_count,
            request_version=policy.request_version,
            observed_at=datetime.now(UTC),
            end=end,
            client=client,
            storage=local_storage,
            rate_limiter=RateLimiter(requests_per_minute=settings.requests_per_minute),
        )

    assert result.ok, f"fetch failed for: {result.failed_dates}"

    raw_prefix = f"agsi/raw/request_version={policy.request_version}/"
    raw_files = [k for k in list_files(local_storage.artifacts, raw_prefix) if k.endswith(".json.gz")]
    assert len(raw_files) == request_count

    build_history(local_storage, policy.request_version)
    build_current(local_storage, policy.request_version)
    publish_release(local_storage, policy.request_version)

    silver_files = [
        k
        for k in list_files(local_storage.artifacts, silver_history_prefix(policy.request_version))
        if k.endswith(".parquet")
    ]
    assert silver_files

    build_key = build_current_path(policy.request_version)
    public_key = public_current_path(policy.request_version)
    assert exists(local_storage.artifacts, build_key)
    assert exists(local_storage.public, public_key)

    current = pl.read_parquet(BytesIO(read_bytes(local_storage.public, public_key)))
    assert current.height > 0
    assert "country_code" in current.columns
