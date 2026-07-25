from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import UTC, date, datetime
from io import BytesIO

import polars as pl
import pyarrow.parquet as pq

from agsi_pipeline.parsing import CountryRow, extract_countries
from agsi_pipeline.logging_config import log_progress
from agsi_pipeline.paths import (
    build_current_path,
    parse_observed_at,
    public_current_path,
    silver_history_partition_path,
    silver_history_prefix,
)
from agsi_pipeline.storage import (
    FsRoot,
    StorageContext,
    atomic_copy,
    atomic_write_bytes,
    exists,
    list_files,
    parse_storage_url,
    read_bytes,
    read_gzip_json,
)

logger = logging.getLogger(__name__)

PARQUET_ATTRIBUTION = "Source: GIE AGSI Transparency Platform"

COUNTRY_FRAME_SCHEMA = {
    "request_version": pl.Int32,
    "gas_day": pl.Date,
    "observed_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "source_updated_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "country_code": pl.Utf8,
    "country_name": pl.Utf8,
    "country_url": pl.Utf8,
    "gas_day_end": pl.Date,
    "gas_in_storage": pl.Float64,
    "consumption": pl.Float64,
    "consumption_full": pl.Float64,
    "injection": pl.Float64,
    "withdrawal": pl.Float64,
    "net_withdrawal": pl.Float64,
    "working_gas_volume": pl.Float64,
    "injection_capacity": pl.Float64,
    "withdrawal_capacity": pl.Float64,
    "contracted_capacity": pl.Float64,
    "available_capacity": pl.Float64,
    "covered_capacity": pl.Float64,
    "status": pl.Utf8,
    "trend": pl.Utf8,
    "full": pl.Float64,
}

RAW_PATH_RE = re.compile(
    r"agsi/raw/request_version=(?P<request_version>\d+)/"
    r"date=(?P<gas_day>\d{4}-\d{2}-\d{2})/"
    r"observed_at=(?P<observed_at>\d{4}-\d{2}-\d{2}T\d{6}Z)/response\.json\.gz"
)


def _country_rows_to_frame(rows: list[CountryRow]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=COUNTRY_FRAME_SCHEMA)

    records = [
        {
            "request_version": row.request_version,
            "gas_day": row.gas_day,
            "observed_at": row.observed_at,
            "source_updated_at": row.source_updated_at,
            "country_code": row.country_code,
            "country_name": row.country_name,
            "country_url": row.country_url,
            "gas_day_end": row.gas_day_end,
            "gas_in_storage": row.gas_in_storage,
            "consumption": row.consumption,
            "consumption_full": row.consumption_full,
            "injection": row.injection,
            "withdrawal": row.withdrawal,
            "net_withdrawal": row.net_withdrawal,
            "working_gas_volume": row.working_gas_volume,
            "injection_capacity": row.injection_capacity,
            "withdrawal_capacity": row.withdrawal_capacity,
            "contracted_capacity": row.contracted_capacity,
            "available_capacity": row.available_capacity,
            "covered_capacity": row.covered_capacity,
            "status": row.status,
            "trend": row.trend,
            "full": row.full,
        }
        for row in rows
    ]
    return pl.DataFrame(records, schema=COUNTRY_FRAME_SCHEMA)


def _write_parquet_atomic(fs_root: FsRoot, key: str, frame: pl.DataFrame) -> None:
    table = frame.to_arrow()
    metadata = {b"source": PARQUET_ATTRIBUTION.encode("utf-8")}
    existing = table.schema.metadata or {}
    merged = dict(existing)
    merged.update(metadata)
    table = table.replace_schema_metadata(merged)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    atomic_write_bytes(fs_root, key, buffer.getvalue())


def _group_raw_snapshots_by_partition(
    storage: StorageContext,
    request_version: int,
) -> dict[str, list[str]]:
    keys_by_partition: dict[str, list[str]] = defaultdict(list)
    for key in _iter_raw_snapshots(storage, request_version):
        _, _, observed_at = _parse_raw_key(key)
        partition_key = silver_history_partition_path(request_version, observed_at)
        keys_by_partition[partition_key].append(key)
    return keys_by_partition


def _remove_orphan_silver_partitions(
    storage: StorageContext,
    request_version: int,
    active_partitions: set[str],
) -> int:
    removed = 0
    existing_prefix = silver_history_prefix(request_version)
    for existing in list_files(storage.artifacts, existing_prefix):
        if existing.endswith(".parquet") and existing not in active_partitions:
            full = storage.artifacts.full_path(existing)
            if storage.artifacts.fs.exists(full):
                storage.artifacts.fs.rm(full)
                removed += 1
    return removed


def _build_history_partition(
    storage: StorageContext,
    partition_key: str,
    snapshot_keys: list[str],
) -> int:
    rows: list[CountryRow] = []
    for key in snapshot_keys:
        rv, gas_day, observed_at = _parse_raw_key(key)
        snapshot = read_gzip_json(storage.artifacts, key)
        rows.extend(
            extract_countries(
                snapshot,
                request_version=rv,
                gas_day=gas_day,
                observed_at=observed_at,
            )
        )
    frame = _country_rows_to_frame(rows)
    _write_parquet_atomic(storage.artifacts, partition_key, frame)
    return frame.height


def _iter_raw_snapshots(storage: StorageContext, request_version: int) -> list[str]:
    prefix = f"agsi/raw/request_version={request_version}/"
    return [
        key
        for key in list_files(storage.artifacts, prefix)
        if key.endswith("response.json.gz") and RAW_PATH_RE.search(key)
    ]


def _parse_raw_key(key: str) -> tuple[int, date, datetime]:
    match = RAW_PATH_RE.search(key)
    if match is None:
        raise ValueError(f"Invalid raw path: {key}")
    return (
        int(match.group("request_version")),
        date.fromisoformat(match.group("gas_day")),
        parse_observed_at(match.group("observed_at")),
    )


def latest_observed_at_by_gas_day(
    storage: StorageContext, request_version: int
) -> dict[date, datetime]:
    latest: dict[date, datetime] = {}
    for key in _iter_raw_snapshots(storage, request_version):
        _, gas_day, observed_at = _parse_raw_key(key)
        existing = latest.get(gas_day)
        if existing is None or observed_at > existing:
            latest[gas_day] = observed_at
    return latest


def build_history(storage: StorageContext, request_version: int) -> None:
    logger.info("Building silver history (request_version=%s)", request_version)
    keys_by_partition = _group_raw_snapshots_by_partition(storage, request_version)
    active_partitions = set(keys_by_partition.keys())
    raw_snapshot_count = sum(len(keys) for keys in keys_by_partition.values())
    logger.info(
        "Grouped %s raw snapshots into %s silver partitions",
        raw_snapshot_count,
        len(active_partitions),
    )
    removed = _remove_orphan_silver_partitions(storage, request_version, active_partitions)
    if removed:
        logger.info("Removed %s orphan silver partitions", removed)

    partition_items = sorted(keys_by_partition.items())
    total_partitions = len(partition_items)
    for index, (partition_key, snapshot_keys) in enumerate(partition_items, start=1):
        row_count = _build_history_partition(storage, partition_key, snapshot_keys)
        logger.info(
            "Built partition %s: %s rows from %s snapshots",
            partition_key,
            row_count,
            len(snapshot_keys),
        )
        log_progress(
            logger,
            index,
            total_partitions,
            10,
            "History build progress: %s/%s partitions",
            index,
            total_partitions,
        )

    logger.info(
        "Built %s silver partitions from %s raw snapshots",
        len(active_partitions),
        raw_snapshot_count,
    )


def _history_partition_keys(storage: StorageContext, request_version: int) -> list[str]:
    return sorted(_group_raw_snapshots_by_partition(storage, request_version).keys())


def _read_history_frame(storage: StorageContext, request_version: int) -> pl.DataFrame:
    parquet_keys = _history_partition_keys(storage, request_version)
    if not parquet_keys:
        logger.info("No history partitions found")
        return _country_rows_to_frame([])

    logger.info("Reading %s history partitions", len(parquet_keys))
    lazy_frames: list[pl.LazyFrame] = []
    for index, key in enumerate(parquet_keys, start=1):
        if not exists(storage.artifacts, key):
            raise FileNotFoundError(
                f"Missing silver history partition {key!r}; run build-history before build-current"
            )
        lazy_frames.append(pl.scan_parquet(BytesIO(read_bytes(storage.artifacts, key))))
        log_progress(
            logger,
            index,
            len(parquet_keys),
            10,
            "History read progress: %s/%s partitions",
            index,
            len(parquet_keys),
        )

    frame = pl.concat(lazy_frames, how="vertical_relaxed").collect(engine="streaming")
    logger.info("Loaded history frame: %s rows", frame.height)
    return frame


def _select_complete_snapshots(frame: pl.DataFrame, as_of: datetime | None = None) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    working = frame
    if as_of is not None:
        as_of_utc = as_of.astimezone(UTC) if as_of.tzinfo else as_of.replace(tzinfo=UTC)
        working = working.filter(pl.col("observed_at") <= as_of_utc)

    selected = (
        working.group_by(["request_version", "gas_day"])
        .agg(pl.col("observed_at").max().alias("observed_at"))
    )
    return working.join(selected, on=["request_version", "gas_day", "observed_at"], how="inner")


def build_current(storage: StorageContext, request_version: int) -> None:
    partition_count = len(_history_partition_keys(storage, request_version))
    logger.info("Building current dataset from %s history partitions", partition_count)
    history = _read_history_frame(storage, request_version)
    logger.info("Selecting latest complete snapshot per gas day")
    current = _select_complete_snapshots(history)
    key = build_current_path(request_version)
    logger.info("Writing current dataset to %s", key)
    _write_parquet_atomic(storage.artifacts, key, current)
    gas_day_count = current.select("gas_day").n_unique() if not current.is_empty() else 0
    logger.info(
        "Wrote current dataset: %s rows, %s gas days -> %s",
        current.height,
        gas_day_count,
        key,
    )


def publish_release(storage: StorageContext, request_version: int) -> None:
    src_key = build_current_path(request_version)
    dst_key = public_current_path(request_version)
    logger.info("Publishing %s -> %s", src_key, dst_key)
    logger.info("Copying artifact to public bucket")
    atomic_copy(storage.artifacts, src_key, storage.public, dst_key)
    logger.info("Published release for request_version=%s", request_version)


def build_as_of(
    storage: StorageContext,
    *,
    request_version: int,
    as_of: datetime,
    output: str,
) -> None:
    logger.info(
        "Building as-of dataset (as_of=%s, output=%s)",
        as_of.isoformat(),
        output,
    )
    history = _read_history_frame(storage, request_version)
    logger.info("Selecting complete snapshots as of %s", as_of.isoformat())
    as_of_frame = _select_complete_snapshots(history, as_of=as_of)
    logger.info("As-of dataset has %s rows", as_of_frame.height)

    if output.startswith("gs://") or output.startswith("file://"):
        fs_root = parse_storage_url(output)
        key = "dataset.parquet"
        if output.startswith("gs://"):
            without_scheme = output.removeprefix("gs://")
            bucket, _, prefix = without_scheme.partition("/")
            fs_root = parse_storage_url(f"gs://{bucket}")
            key = prefix or "dataset.parquet"
        elif output.startswith("file://"):
            path = output.removeprefix("file://")
            if path.endswith(".parquet"):
                from pathlib import Path

                p = Path(path)
                fs_root = parse_storage_url(f"file://{p.parent}")
                key = p.name
            else:
                fs_root = parse_storage_url(output)
                key = "dataset.parquet"
        _write_parquet_atomic(fs_root, key, as_of_frame)
        return

    as_of_frame.write_parquet(output, compression="zstd")


def snapshot_observed_at_from_key(key: str) -> datetime:
    return _parse_raw_key(key)[2]


def snapshot_gas_day_from_key(key: str) -> date:
    return _parse_raw_key(key)[1]
