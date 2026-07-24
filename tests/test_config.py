from __future__ import annotations

from datetime import date
from pathlib import Path

from agsi_pipeline.config import Settings, load_sync_policy
from agsi_pipeline.storage import parse_storage_url


def test_load_sync_policy(policy_file: Path) -> None:
    policy = load_sync_policy(policy_file)
    assert policy.request_version == 1
    assert policy.history_start_date == date(2011, 1, 1)
    assert policy.recent_days == 30
    assert policy.reconciliation_interval_days == 31
    assert policy.reconciliation_resume_days == 3


def test_settings_defaults(monkeypatch) -> None:
    monkeypatch.delenv("AGSI_REQUESTS_PER_MINUTE", raising=False)
    settings = Settings()
    assert settings.api_base_url == "https://agsi.gie.eu"
    assert settings.requests_per_minute == 60
    assert "api_key" not in repr(settings)


def test_parse_storage_urls(storage_dirs: tuple[str, str]) -> None:
    artifacts = parse_storage_url(storage_dirs[0])
    public = parse_storage_url(storage_dirs[1])
    assert artifacts.root != public.root
