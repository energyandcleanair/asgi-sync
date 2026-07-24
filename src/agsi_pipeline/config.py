from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from agsi_pipeline.storage import StorageContext, open_storage_context


def default_policy_path() -> Path:
    return Path("sync-policy.toml")


@dataclass(frozen=True)
class SyncPolicy:
    request_version: int
    history_start_date: date
    recent_days: int
    reconciliation_interval_days: int
    reconciliation_resume_days: int

    @property
    def reconciliation_interval(self) -> timedelta:
        return timedelta(days=self.reconciliation_interval_days)


def load_sync_policy(path: Path | None = None) -> SyncPolicy:
    policy_path = path or default_policy_path()
    with policy_path.open("rb") as f:
        data = tomllib.load(f)
    return SyncPolicy(
        request_version=int(data["request_version"]),
        history_start_date=date.fromisoformat(str(data["history_start_date"])),
        recent_days=int(data["recent_days"]),
        reconciliation_interval_days=int(data["reconciliation_interval_days"]),
        reconciliation_resume_days=int(data["reconciliation_resume_days"]),
    )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AGSI_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_base_url: str = "https://agsi.gie.eu"
    api_key: SecretStr = Field(default_factory=lambda: SecretStr(""))
    artifacts_storage_url: str = "file://./data/artifacts"
    public_storage_url: str = "file://./data/public"
    requests_per_minute: int = Field(default=60, ge=1)

    def storage_context(self) -> StorageContext:
        return open_storage_context(
            self.artifacts_storage_url,
            self.public_storage_url,
        )

    def api_key_value(self) -> str:
        return self.api_key.get_secret_value()

    def __repr__(self) -> str:
        return (
            f"Settings(api_base_url={self.api_base_url!r}, "
            f"artifacts_storage_url={self.artifacts_storage_url!r}, "
            f"public_storage_url={self.public_storage_url!r}, "
            f"requests_per_minute={self.requests_per_minute})"
        )
