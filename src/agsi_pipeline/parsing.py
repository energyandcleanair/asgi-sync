from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any

from agsi_pipeline.models import DuplicateKeyError


class HierarchyLevel(Enum):
    AGGREGATE = "aggregate"
    COUNTRY = "country"
    OPERATOR = "operator"
    FACILITY = "facility"


@dataclass(frozen=True)
class ParsedRecord:
    level: HierarchyLevel
    code: str
    name: str | None
    url: str | None
    country_code: str | None
    operator_code: str | None
    facility_code: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class CountryRow:
    request_version: int
    gas_day: date
    observed_at: datetime
    source_updated_at: datetime | None
    country_code: str
    country_name: str | None
    country_url: str | None
    gas_day_end: date | None
    gas_in_storage: float | None
    consumption: float | None
    consumption_full: float | None
    injection: float | None
    withdrawal: float | None
    net_withdrawal: float | None
    working_gas_volume: float | None
    injection_capacity: float | None
    withdrawal_capacity: float | None
    contracted_capacity: float | None
    available_capacity: float | None
    covered_capacity: float | None
    status: str | None
    trend: str | None
    full: float | None


def parse_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text in {"", "-"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _infer_level(
    node: dict[str, Any],
    *,
    parent_level: HierarchyLevel | None,
) -> HierarchyLevel:
    code = str(node.get("code", ""))
    if parent_level is None:
        lowered = code.lower()
        if lowered in {"eu", "ne", "ai"}:
            return HierarchyLevel.AGGREGATE
        if len(code) == 2 and code.isalpha():
            return HierarchyLevel.COUNTRY
        return HierarchyLevel.AGGREGATE
    if parent_level == HierarchyLevel.AGGREGATE:
        return HierarchyLevel.COUNTRY
    if parent_level == HierarchyLevel.COUNTRY:
        return HierarchyLevel.OPERATOR
    return HierarchyLevel.FACILITY


def walk_hierarchy(
    nodes: list[dict[str, Any]],
    *,
    parent_level: HierarchyLevel | None = None,
    country_code: str | None = None,
    operator_code: str | None = None,
) -> list[ParsedRecord]:
    records: list[ParsedRecord] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        level = _infer_level(node, parent_level=parent_level)
        code = str(node.get("code", ""))
        name = node.get("name")
        url = node.get("url")

        current_country = country_code
        current_operator = operator_code
        current_facility: str | None = None

        if level == HierarchyLevel.COUNTRY:
            current_country = code.upper()
        elif level == HierarchyLevel.OPERATOR:
            current_operator = code
        elif level == HierarchyLevel.FACILITY:
            current_facility = code

        records.append(
            ParsedRecord(
                level=level,
                code=code,
                name=str(name) if name is not None else None,
                url=str(url) if url is not None else None,
                country_code=current_country,
                operator_code=current_operator,
                facility_code=current_facility,
                payload=node,
            )
        )

        children = node.get("children")
        if isinstance(children, list) and children:
            records.extend(
                walk_hierarchy(
                    children,
                    parent_level=level,
                    country_code=current_country,
                    operator_code=current_operator,
                )
            )
    return records


def _parse_gas_day_field(payload: dict[str, Any], field: str) -> date | None:
    value = payload.get(field)
    if value is None:
        return None
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _country_row_from_node(
    node: dict[str, Any],
    *,
    request_version: int,
    gas_day: date,
    observed_at: datetime,
    country_code: str,
    country_name: str | None,
    country_url: str | None,
) -> CountryRow:
    return CountryRow(
        request_version=request_version,
        gas_day=gas_day,
        observed_at=observed_at,
        source_updated_at=parse_timestamp(node.get("updatedAt")),
        country_code=country_code,
        country_name=country_name,
        country_url=country_url,
        gas_day_end=_parse_gas_day_field(node, "gasDayEnd"),
        gas_in_storage=parse_value(node.get("gasInStorage")),
        consumption=parse_value(node.get("consumption")),
        consumption_full=parse_value(node.get("consumptionFull")),
        injection=parse_value(node.get("injection")),
        withdrawal=parse_value(node.get("withdrawal")),
        net_withdrawal=parse_value(node.get("netWithdrawal")),
        working_gas_volume=parse_value(node.get("workingGasVolume")),
        injection_capacity=parse_value(node.get("injectionCapacity")),
        withdrawal_capacity=parse_value(node.get("withdrawalCapacity")),
        contracted_capacity=parse_value(node.get("contractedCapacity")),
        available_capacity=parse_value(node.get("availableCapacity")),
        covered_capacity=parse_value(node.get("coveredCapacity")),
        status=str(node["status"]) if node.get("status") is not None else None,
        trend=str(node["trend"]) if node.get("trend") is not None else None,
        full=parse_value(node.get("full")),
    )


def _country_row_from_record(
    record: ParsedRecord,
    *,
    request_version: int,
    gas_day: date,
    observed_at: datetime,
) -> CountryRow:
    return _country_row_from_node(
        record.payload,
        request_version=request_version,
        gas_day=gas_day,
        observed_at=observed_at,
        country_code=record.country_code or record.code.upper(),
        country_name=record.name,
        country_url=record.url,
    )


def _append_country_rows(
    nodes: list[dict[str, Any]],
    rows: list[CountryRow],
    *,
    parent_level: HierarchyLevel | None = None,
    request_version: int,
    gas_day: date,
    observed_at: datetime,
) -> None:
    for node in nodes:
        if not isinstance(node, dict):
            continue
        level = _infer_level(node, parent_level=parent_level)
        code = str(node.get("code", ""))
        name = node.get("name")
        url = node.get("url")

        if level == HierarchyLevel.COUNTRY:
            rows.append(
                _country_row_from_node(
                    node,
                    request_version=request_version,
                    gas_day=gas_day,
                    observed_at=observed_at,
                    country_code=code.upper(),
                    country_name=str(name) if name is not None else None,
                    country_url=str(url) if url is not None else None,
                )
            )

        children = node.get("children")
        if isinstance(children, list) and children:
            _append_country_rows(
                children,
                rows,
                parent_level=level,
                request_version=request_version,
                gas_day=gas_day,
                observed_at=observed_at,
            )


def collect_snapshot_gas_days(
    nodes: list[dict[str, Any]],
    *,
    parent_level: HierarchyLevel | None = None,
) -> tuple[set[str], set[str]]:
    starts: set[str] = set()
    ends: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        level = _infer_level(node, parent_level=parent_level)
        if node.get("gasDayStart") is not None:
            starts.add(str(node["gasDayStart"])[:10])
        if node.get("gasDayEnd") is not None:
            ends.add(str(node["gasDayEnd"])[:10])
        children = node.get("children")
        if isinstance(children, list) and children:
            child_starts, child_ends = collect_snapshot_gas_days(
                children,
                parent_level=level,
            )
            starts.update(child_starts)
            ends.update(child_ends)
    return starts, ends


def extract_countries(
    snapshot: dict[str, Any],
    *,
    request_version: int,
    gas_day: date,
    observed_at: datetime,
) -> list[CountryRow]:
    data = snapshot.get("data", [])
    if not isinstance(data, list):
        raise ValueError("snapshot data must be a list")

    rows: list[CountryRow] = []
    _append_country_rows(
        data,
        rows,
        request_version=request_version,
        gas_day=gas_day,
        observed_at=observed_at,
    )

    seen: set[tuple[int, date, datetime, str]] = set()
    for row in rows:
        key = (row.request_version, row.gas_day, row.observed_at, row.country_code)
        if key in seen:
            raise DuplicateKeyError(f"Duplicate country key: {key}")
        seen.add(key)
    return rows
