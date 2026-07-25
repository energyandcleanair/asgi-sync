from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

from agsi_pipeline.client import AgsiClient
from agsi_pipeline.config import SyncPolicy
from agsi_pipeline.datasets import build_current, build_history, publish_release
from agsi_pipeline.dates import latest_gas_day
from agsi_pipeline.fetching import RateLimiter, reconcile, refresh_recent
from agsi_pipeline.state import SyncState, read_sync_state_or_none, write_sync_state_atomically
from agsi_pipeline.storage import StorageContext

logger = logging.getLogger(__name__)


class OrchestratorError(Exception):
    """Raised when orchestration fails."""


@dataclass
class OrchestratorResult:
    reconciled: bool
    failed_dates: list[date]


def needs_reconciliation(
    policy: SyncPolicy,
    state: SyncState | None,
    *,
    today: date,
    force: bool,
) -> bool:
    if force:
        return True
    if state is None:
        return True
    if state.last_reconciled_request_version != policy.request_version:
        return True
    age = today - state.last_successful_reconciliation_date
    return age >= policy.reconciliation_interval


def _reconciliation_reason(
    policy: SyncPolicy,
    state: SyncState | None,
    *,
    today: date,
    force: bool,
) -> str:
    if force:
        return "forced"
    if state is None:
        return "no prior state"
    if state.last_reconciled_request_version != policy.request_version:
        return "request version changed"
    age = today - state.last_successful_reconciliation_date
    if age >= policy.reconciliation_interval:
        return "reconciliation interval elapsed"
    return f"state age={age.days}d"


def run_sync(
    *,
    storage: StorageContext,
    policy: SyncPolicy,
    client: AgsiClient,
    rate_limiter: RateLimiter,
    force_reconcile: bool = False,
    force_fetch: bool = False,
    today: date | None = None,
    latest_gas_day_override: date | None = None,
) -> OrchestratorResult:
    started_at = time.monotonic()
    today_utc = today or datetime.now(UTC).date()
    state = read_sync_state_or_none(storage)
    do_reconcile = needs_reconciliation(policy, state, today=today_utc, force=force_reconcile)
    observed_at = datetime.now(UTC)
    end = latest_gas_day(today_utc, latest_gas_day_override)

    logger.info(
        "Starting sync (request_version=%s, end=%s)",
        policy.request_version,
        end,
    )

    if do_reconcile:
        reason = _reconciliation_reason(policy, state, today=today_utc, force=force_reconcile)
        logger.info("Reconciliation required: %s", reason)
        logger.info("Running full reconciliation from %s to %s", policy.history_start_date, end)
        fetch_started = time.monotonic()
        result = reconcile(
            start=policy.history_start_date,
            end=end,
            request_version=policy.request_version,
            observed_at=observed_at,
            client=client,
            storage=storage,
            rate_limiter=rate_limiter,
            force=force_fetch,
            resume_days=policy.reconciliation_resume_days,
        )
        logger.info("Fetch stage completed in %.1fs", time.monotonic() - fetch_started)
    else:
        reason = _reconciliation_reason(policy, state, today=today_utc, force=force_reconcile)
        logger.info("Using recent refresh (%s)", reason)
        logger.info("Running recent refresh for %s days ending %s", policy.recent_days, end)
        fetch_started = time.monotonic()
        result = refresh_recent(
            days=policy.recent_days,
            request_version=policy.request_version,
            observed_at=observed_at,
            end=end,
            client=client,
            storage=storage,
            rate_limiter=rate_limiter,
            force=force_fetch,
            resume_days=policy.reconciliation_resume_days,
        )
        logger.info("Fetch stage completed in %.1fs", time.monotonic() - fetch_started)

    if not result.ok:
        logger.error("Fetch failed for dates: %s", result.failed_dates)
        raise OrchestratorError(f"Fetch failed for {len(result.failed_dates)} dates")

    history_started = time.monotonic()
    build_history(storage, policy.request_version)
    logger.info("Silver history stage completed in %.1fs", time.monotonic() - history_started)

    current_started = time.monotonic()
    build_current(storage, policy.request_version)
    logger.info("Current dataset stage completed in %.1fs", time.monotonic() - current_started)

    publish_started = time.monotonic()
    publish_release(storage, policy.request_version)
    logger.info("Publish stage completed in %.1fs", time.monotonic() - publish_started)

    if do_reconcile:
        write_sync_state_atomically(
            storage,
            SyncState(
                last_successful_reconciliation_date=today_utc,
                last_reconciled_request_version=policy.request_version,
            ),
        )
        logger.info(
            "Updated sync state: reconciliation_date=%s, request_version=%s",
            today_utc,
            policy.request_version,
        )

    elapsed = time.monotonic() - started_at
    logger.info("Sync completed in %.1fs (reconciled=%s)", elapsed, do_reconcile)
    return OrchestratorResult(reconciled=do_reconcile, failed_dates=result.failed_dates)
