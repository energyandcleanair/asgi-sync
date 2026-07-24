from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import typer

from agsi_pipeline.client import AgsiClient
from agsi_pipeline.config import Settings, load_sync_policy
from agsi_pipeline.datasets import build_as_of, build_current, build_history, publish_release
from agsi_pipeline.dates import latest_gas_day, parse_gas_day
from agsi_pipeline.fetching import RateLimiter, fetch_and_store_day, reconcile, refresh_recent
from agsi_pipeline.orchestrator import OrchestratorError, run_sync

app = typer.Typer(no_args_is_help=True, add_completion=False)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _settings() -> Settings:
    return Settings()


def _policy_path(policy_file: Path | None) -> Path | None:
    return policy_file


@app.command()
def sync(
    reconcile: bool = typer.Option(False, "--reconcile", help="Force full reconciliation"),
    force: bool = typer.Option(False, "--force", help="Re-fetch all days regardless of recent snapshots"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
    latest_gas_day_value: str | None = typer.Option(
        None, "--latest-gas-day", help="Override latest gas day (YYYY-MM-DD)"
    ),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    override = parse_gas_day(latest_gas_day_value) if latest_gas_day_value else None
    rate_limiter = RateLimiter(requests_per_minute=settings.requests_per_minute)
    with AgsiClient(base_url=settings.api_base_url, api_key=settings.api_key_value()) as client:
        try:
            run_sync(
                storage=settings.storage_context(),
                policy=policy,
                client=client,
                rate_limiter=rate_limiter,
                force_reconcile=reconcile,
                force_fetch=force,
                latest_gas_day_override=override,
            )
        except OrchestratorError as exc:
            logger.error("%s", exc)
            raise typer.Exit(code=1) from exc


@app.command("fetch-day")
def fetch_day_cmd(
    gas_day_value: str = typer.Argument(..., help="Gas day (YYYY-MM-DD)"),
    observed_at_value: str | None = typer.Option(
        None, "--observed-at", help="Observed at timestamp (YYYY-MM-DDTHH:MM:SSZ)"
    ),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    gas_day = parse_gas_day(gas_day_value)
    if observed_at_value:
        observed_at = datetime.fromisoformat(
            observed_at_value.replace("Z", "+00:00")
        ).astimezone(UTC)
    else:
        observed_at = datetime.now(UTC)

    rate_limiter = RateLimiter(requests_per_minute=settings.requests_per_minute)
    with AgsiClient(base_url=settings.api_base_url, api_key=settings.api_key_value()) as client:
        fetch_and_store_day(
            gas_day,
            policy.request_version,
            observed_at,
            client=client,
            storage=settings.storage_context(),
            rate_limiter=rate_limiter,
        )


@app.command("refresh-recent")
def refresh_recent_cmd(
    days: int | None = typer.Option(None, "--days"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
    latest_gas_day_value: str | None = typer.Option(None, "--latest-gas-day"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    window = days if days is not None else policy.recent_days
    today = datetime.now(UTC).date()
    override = parse_gas_day(latest_gas_day_value) if latest_gas_day_value else None
    end = latest_gas_day(today, override)
    observed_at = datetime.now(UTC)
    rate_limiter = RateLimiter(requests_per_minute=settings.requests_per_minute)
    with AgsiClient(base_url=settings.api_base_url, api_key=settings.api_key_value()) as client:
        result = refresh_recent(
            days=window,
            request_version=policy.request_version,
            observed_at=observed_at,
            end=end,
            client=client,
            storage=settings.storage_context(),
            rate_limiter=rate_limiter,
        )
    if not result.ok:
        logger.error("Failed dates: %s", result.failed_dates)
        raise typer.Exit(code=1)


@app.command("reconcile")
def reconcile_cmd(
    start_value: str | None = typer.Option(None, "--start"),
    end_value: str | None = typer.Option(None, "--end"),
    force: bool = typer.Option(False, "--force", help="Re-fetch all days regardless of recent snapshots"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
    latest_gas_day_value: str | None = typer.Option(None, "--latest-gas-day"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    today = datetime.now(UTC).date()
    start = parse_gas_day(start_value) if start_value else policy.history_start_date
    end = (
        parse_gas_day(end_value)
        if end_value
        else latest_gas_day(
            today,
            parse_gas_day(latest_gas_day_value) if latest_gas_day_value else None,
        )
    )
    observed_at = datetime.now(UTC)
    rate_limiter = RateLimiter(requests_per_minute=settings.requests_per_minute)
    with AgsiClient(base_url=settings.api_base_url, api_key=settings.api_key_value()) as client:
        result = reconcile(
            start=start,
            end=end,
            request_version=policy.request_version,
            observed_at=observed_at,
            client=client,
            storage=settings.storage_context(),
            rate_limiter=rate_limiter,
            force=force,
            resume_days=policy.reconciliation_resume_days,
        )
    if not result.ok:
        logger.error("Failed dates: %s", result.failed_dates)
        raise typer.Exit(code=1)


@app.command("build-history")
def build_history_cmd(
    request_version: int | None = typer.Option(None, "--request-version"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    version = request_version if request_version is not None else policy.request_version
    build_history(settings.storage_context(), version)


@app.command("build-current")
def build_current_cmd(
    request_version: int | None = typer.Option(None, "--request-version"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    version = request_version if request_version is not None else policy.request_version
    build_current(settings.storage_context(), version)


@app.command("publish-release")
def publish_release_cmd(
    request_version: int | None = typer.Option(None, "--request-version"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    version = request_version if request_version is not None else policy.request_version
    publish_release(settings.storage_context(), version)


@app.command("build-as-of")
def build_as_of_cmd(
    as_of_value: str = typer.Option(..., "--as-of"),
    output: str = typer.Option(..., "--output"),
    request_version: int | None = typer.Option(None, "--request-version"),
    policy_file: Path = typer.Option(Path("sync-policy.toml"), "--policy-file"),
) -> None:
    settings = _settings()
    policy = load_sync_policy(policy_file)
    version = request_version if request_version is not None else policy.request_version
    as_of = datetime.fromisoformat(as_of_value.replace("Z", "+00:00")).astimezone(UTC)
    build_as_of(
        settings.storage_context(),
        request_version=version,
        as_of=as_of,
        output=output,
    )
