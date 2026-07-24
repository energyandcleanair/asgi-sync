from __future__ import annotations

import logging
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


def run_sync(
    *,
    storage: StorageContext,
    policy: SyncPolicy,
    client: AgsiClient,
    rate_limiter: RateLimiter,
    force_reconcile: bool = False,
    today: date | None = None,
    latest_gas_day_override: date | None = None,
) -> OrchestratorResult:
    today_utc = today or datetime.now(UTC).date()
    state = read_sync_state_or_none(storage)
    do_reconcile = needs_reconciliation(policy, state, today=today_utc, force=force_reconcile)
    observed_at = datetime.now(UTC)
    end = latest_gas_day(today_utc, latest_gas_day_override)

    if do_reconcile:
        logger.info("Running full reconciliation from %s to %s", policy.history_start_date, end)
        result = reconcile(
            start=policy.history_start_date,
            end=end,
            request_version=policy.request_version,
            observed_at=observed_at,
            client=client,
            storage=storage,
            rate_limiter=rate_limiter,
        )
    else:
        logger.info("Running recent refresh for %s days ending %s", policy.recent_days, end)
        result = refresh_recent(
            days=policy.recent_days,
            request_version=policy.request_version,
            observed_at=observed_at,
            end=end,
            client=client,
            storage=storage,
            rate_limiter=rate_limiter,
        )

    if not result.ok:
        logger.error("Fetch failed for dates: %s", result.failed_dates)
        raise OrchestratorError(f"Fetch failed for {len(result.failed_dates)} dates")

    build_history(storage, policy.request_version)
    build_current(storage, policy.request_version)
    publish_release(storage, policy.request_version)

    if do_reconcile:
        write_sync_state_atomically(
            storage,
            SyncState(
                last_successful_reconciliation_date=today_utc,
                last_reconciled_request_version=policy.request_version,
            ),
        )

    return OrchestratorResult(reconciled=do_reconcile, failed_dates=result.failed_dates)
