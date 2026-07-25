#!/usr/bin/env python3
"""Record AGSI API cassettes for offline integration tests (local use only)."""

from __future__ import annotations

import logging
import sys
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from agsi_pipeline.client import AgsiClient  # noqa: E402
from agsi_pipeline.config import Settings  # noqa: E402
from agsi_pipeline.logging_config import configure_logging  # noqa: E402
from agsi_pipeline.dates import latest_gas_day  # noqa: E402
from agsi_pipeline.fetching import RateLimiter, refresh_recent  # noqa: E402
from agsi_pipeline.storage import open_storage_context  # noqa: E402
from integration.cassettes import (  # noqa: E402
    CASSETTE_NAME,
    PLACEHOLDER_API_KEY,
    gas_days_from_cassette,
    make_vcr,
    normalize_cassette_file,
    save_manifest,
)

REQUEST_COUNT = 10
RECORD_MODE = "rewrite" if "--rewrite" in sys.argv else "once"

logger = logging.getLogger(__name__)


def _gas_day_window(end: date) -> list[date]:
    start = end - timedelta(days=REQUEST_COUNT - 1)
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def main() -> int:
    settings = Settings()
    configure_logging(settings.log_level)
    logging.getLogger("vcr").setLevel(logging.WARNING)
    api_key = settings.api_key_value()
    if not api_key or api_key == PLACEHOLDER_API_KEY:
        logger.error("Configure a real AGSI_API_KEY in .env before recording cassettes")
        return 1

    end = latest_gas_day(datetime.now(UTC).date())
    gas_days = _gas_day_window(end)

    my_vcr = make_vcr(record_mode=RECORD_MODE)
    logger.info(
        "Recording %s gas days (%s to %s) into %s",
        len(gas_days),
        gas_days[0],
        gas_days[-1],
        CASSETTE_NAME,
    )

    with tempfile.TemporaryDirectory() as tmp:
        storage = open_storage_context(
            f"file://{tmp}/artifacts",
            f"file://{tmp}/public",
        )
        with (
            my_vcr.use_cassette(CASSETTE_NAME),
            AgsiClient(base_url=settings.api_base_url, api_key=api_key) as client,
        ):
            result = refresh_recent(
                days=REQUEST_COUNT,
                request_version=1,
                observed_at=datetime.now(UTC),
                end=end,
                client=client,
                storage=storage,
                rate_limiter=RateLimiter(requests_per_minute=settings.requests_per_minute),
            )

    if not result.ok:
        logger.error("Recording failed for dates: %s", result.failed_dates)
        return 1

    normalize_cassette_file(max_interactions=REQUEST_COUNT)
    recorded_days = gas_days_from_cassette()
    save_manifest(
        base_url=settings.api_base_url,
        gas_days=recorded_days,
        recorded_at=datetime.now(UTC).isoformat(),
    )
    logger.info("Recorded %s interactions into tests/fixtures/cassettes/", len(recorded_days))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
