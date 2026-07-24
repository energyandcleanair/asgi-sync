from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum


class BucketTarget(Enum):
    ARTIFACTS = "artifacts"
    PUBLIC = "public"


def format_observed_at(dt: datetime) -> str:
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H%M%SZ")


def parse_observed_at(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H%M%SZ").replace(tzinfo=UTC)


def raw_response_path(request_version: int, gas_day: date, observed_at: datetime) -> str:
    ts = format_observed_at(observed_at)
    return (
        f"agsi/raw/request_version={request_version}/"
        f"date={gas_day.isoformat()}/observed_at={ts}/response.json.gz"
    )


def silver_history_partition_path(
    request_version: int,
    observed_at: datetime,
) -> str:
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    else:
        observed_at = observed_at.astimezone(UTC)
    return (
        f"agsi/silver/request_version={request_version}/country_history/"
        f"observed_year={observed_at.year:04d}/"
        f"observed_month={observed_at.month:02d}/data.parquet"
    )


def silver_history_prefix(request_version: int) -> str:
    return f"agsi/silver/request_version={request_version}/country_history/"


def build_current_path(request_version: int) -> str:
    return f"agsi/build/request_version={request_version}/country_daily.parquet"


def public_current_path(request_version: int) -> str:
    return f"agsi/current/request_version={request_version}/country_daily.parquet"


def sync_state_path() -> str:
    return "agsi/sync-state.json"


def bucket_for_path(path: str) -> BucketTarget:
    if path.startswith("agsi/current/"):
        return BucketTarget.PUBLIC
    return BucketTarget.ARTIFACTS
