from __future__ import annotations

import gzip
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import httpx

from agsi_pipeline.client import AgsiClient
from agsi_pipeline.datasets import latest_observed_at_by_gas_day
from agsi_pipeline.logging_config import log_progress
from agsi_pipeline.models import InvalidResponseError
from agsi_pipeline.parsing import collect_snapshot_gas_days
from agsi_pipeline.paths import raw_response_path
from agsi_pipeline.storage import StorageContext, atomic_write_bytes

logger = logging.getLogger(__name__)


def validate_response(payload: dict[str, Any], *, gas_day: date) -> None:
    if payload.get("error") is not None:
        raise InvalidResponseError("API returned an error payload")
    if "message" in payload and "data" not in payload:
        raise InvalidResponseError("API returned an error message")

    if "data" not in payload or "gas_day" not in payload:
        raise InvalidResponseError("Response missing required top-level keys")

    data = payload["data"]
    if not isinstance(data, list):
        raise InvalidResponseError("Response data is not a list")
    if len(data) == 0:
        raise InvalidResponseError("Response data is empty")

    snapshot_starts, snapshot_ends = collect_snapshot_gas_days(data)
    requested = gas_day.isoformat()
    if requested not in snapshot_starts and requested not in snapshot_ends:
        top_level = str(payload.get("gas_day", ""))[:10]
        raise InvalidResponseError(
            f"gas_day mismatch: requested {requested}, "
            f"snapshot gasDayStart values {sorted(snapshot_starts)}, "
            f"gasDayEnd values {sorted(snapshot_ends)}, top-level gas_day {top_level!r}"
        )


@dataclass
class RateLimiter:
    requests_per_minute: int
    _last_request_at: float = field(default=0.0, init=False, repr=False)

    def wait(self) -> None:
        min_interval = 60.0 / self.requests_per_minute
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_at = time.monotonic()


def fetch_and_store_day(
    gas_day: date,
    request_version: int,
    observed_at: datetime,
    *,
    client: AgsiClient,
    storage: StorageContext,
    rate_limiter: RateLimiter,
    max_attempts: int = 5,
) -> None:
    path = raw_response_path(request_version, gas_day, observed_at)
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        rate_limiter.wait()
        try:
            status_code, body = client.fetch_day(gas_day)
            if status_code == 429:
                logger.warning("Rate limited for %s; waiting 60s", gas_day.isoformat())
                time.sleep(60)
                continue
            if status_code < 200 or status_code >= 300:
                raise InvalidResponseError(f"HTTP {status_code} for {gas_day.isoformat()}")

            try:
                payload = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise InvalidResponseError("Response is not valid JSON") from exc

            if not isinstance(payload, dict):
                raise InvalidResponseError("Response JSON root is not an object")

            validate_response(payload, gas_day=gas_day)
            compressed = gzip.compress(body, mtime=0)
            atomic_write_bytes(storage.artifacts, path, compressed)
            logger.debug("Stored %s -> %s", gas_day.isoformat(), path)
            return
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            last_error = exc
            if attempt < max_attempts:
                logger.warning(
                    "Fetch attempt %s/%s for %s failed: %s",
                    attempt,
                    max_attempts,
                    gas_day.isoformat(),
                    exc,
                )
            time.sleep(min(2**attempt, 30))
        except InvalidResponseError as exc:
            last_error = exc
            if attempt < max_attempts:
                logger.warning(
                    "Fetch attempt %s/%s for %s failed: %s",
                    attempt,
                    max_attempts,
                    gas_day.isoformat(),
                    exc,
                )
            if attempt == max_attempts:
                break
            time.sleep(min(2**attempt, 30))

    raise InvalidResponseError(
        f"Failed to fetch {gas_day.isoformat()}: {last_error}"
    ) from last_error


@dataclass
class FetchBatchResult:
    failed_dates: list[date]
    skipped_dates: list[date] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed_dates


def should_skip_gas_day(
    latest_observed_at: datetime | None,
    *,
    now: datetime,
    resume_days: int,
    force: bool,
) -> bool:
    if force:
        return False
    if latest_observed_at is None:
        return False
    observed = (
        latest_observed_at.replace(tzinfo=UTC)
        if latest_observed_at.tzinfo is None
        else latest_observed_at.astimezone(UTC)
    )
    now_utc = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    age_days = (now_utc.date() - observed.date()).days
    return age_days < resume_days


def refresh_recent(
    *,
    days: int,
    request_version: int,
    observed_at: datetime,
    end: date,
    client: AgsiClient,
    storage: StorageContext,
    rate_limiter: RateLimiter,
    force: bool = False,
    resume_days: int = 3,
) -> FetchBatchResult:
    from datetime import timedelta

    start = end - timedelta(days=days - 1)
    return reconcile(
        start=start,
        end=end,
        request_version=request_version,
        observed_at=observed_at,
        client=client,
        storage=storage,
        rate_limiter=rate_limiter,
        force=force,
        resume_days=resume_days,
    )


def reconcile(
    *,
    start: date,
    end: date,
    request_version: int,
    observed_at: datetime,
    client: AgsiClient,
    storage: StorageContext,
    rate_limiter: RateLimiter,
    force: bool = False,
    resume_days: int = 3,
) -> FetchBatchResult:
    from agsi_pipeline.dates import iter_gas_days

    now = observed_at
    latest_by_day = latest_observed_at_by_gas_day(storage, request_version)
    failed: list[date] = []
    skipped: list[date] = []
    fetched = 0
    total_days = (end - start).days + 1
    logger.info(
        "Processing %s gas days from %s to %s",
        total_days,
        start.isoformat(),
        end.isoformat(),
    )
    for index, gas_day in enumerate(iter_gas_days(start, end), start=1):
        latest_observed = latest_by_day.get(gas_day)
        if should_skip_gas_day(
            latest_observed, now=now, resume_days=resume_days, force=force
        ):
            skipped.append(gas_day)
            logger.debug(
                "Skipping %s (fetched %s days ago)",
                gas_day.isoformat(),
                (now.date() - latest_observed.date()).days,
            )
            continue
        try:
            fetch_and_store_day(
                gas_day,
                request_version,
                observed_at,
                client=client,
                storage=storage,
                rate_limiter=rate_limiter,
            )
            fetched += 1
        except InvalidResponseError as exc:
            logger.error("Failed to fetch %s: %s", gas_day.isoformat(), exc)
            failed.append(gas_day)
        log_progress(
            logger,
            index,
            total_days,
            50,
            "Fetch progress: %s/%s (fetched=%s, skipped=%s, failed=%s)",
            index,
            total_days,
            fetched,
            len(skipped),
            len(failed),
        )
    logger.info(
        "Reconcile: fetched %s, skipped %s, failed %s",
        fetched,
        len(skipped),
        len(failed),
    )
    return FetchBatchResult(failed_dates=failed, skipped_dates=skipped)
