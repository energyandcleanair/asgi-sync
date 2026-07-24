from __future__ import annotations

from datetime import date

from agsi_pipeline.config import load_sync_policy
from agsi_pipeline.orchestrator import needs_reconciliation
from agsi_pipeline.state import SyncState, read_sync_state_or_none, write_sync_state_atomically


def test_missing_state_triggers_reconciliation(policy_file) -> None:
    policy = load_sync_policy(policy_file)
    assert needs_reconciliation(policy, None, today=date(2026, 7, 23), force=False)


def test_matching_version_allows_refresh(policy_file) -> None:
    policy = load_sync_policy(policy_file)
    state = SyncState(date(2026, 7, 1), 1)
    assert not needs_reconciliation(policy, state, today=date(2026, 7, 10), force=False)


def test_expired_interval_triggers_reconciliation(policy_file) -> None:
    policy = load_sync_policy(policy_file)
    state = SyncState(date(2026, 6, 1), 1)
    assert needs_reconciliation(policy, state, today=date(2026, 7, 23), force=False)


def test_changed_request_version_triggers_reconciliation(policy_file) -> None:
    policy = load_sync_policy(policy_file)
    state = SyncState(date(2026, 7, 1), 0)
    assert needs_reconciliation(policy, state, today=date(2026, 7, 10), force=False)


def test_explicit_reconcile_override(policy_file) -> None:
    policy = load_sync_policy(policy_file)
    state = SyncState(date(2026, 7, 1), 1)
    assert needs_reconciliation(policy, state, today=date(2026, 7, 10), force=True)


def test_sync_state_round_trip(storage) -> None:
    state = SyncState(date(2026, 7, 1), 1)
    write_sync_state_atomically(storage, state)
    loaded = read_sync_state_or_none(storage)
    assert loaded == state
