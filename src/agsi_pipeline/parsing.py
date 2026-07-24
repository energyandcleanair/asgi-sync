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


def _country_row_from_record(
    record: ParsedRecord,
    *,
    request_version: int,
    gas_day: date,
    observed_at: datetime,
) -> CountryRow:
    payload = record.payload
    return CountryRow(
        request_version=request_version,
        gas_day=gas_day,
        observed_at=observed_at,
        source_updated_at=parse_timestamp(payload.get("updatedAt")),
        country_code=record.country_code or record.code.upper(),
        country_name=record.name,
        country_url=record.url,
        gas_day_end=_parse_gas_day_field(payload, "gasDayEnd"),
        gas_in_storage=parse_value(payload.get("gasInStorage")),
        consumption=parse_value(payload.get("consumption")),
        consumption_full=parse_value(payload.get("consumptionFull")),
        injection=parse_value(payload.get("injection")),
        withdrawal=parse_value(payload.get("withdrawal")),
        net_withdrawal=parse_value(payload.get("netWithdrawal")),
        working_gas_volume=parse_value(payload.get("workingGasVolume")),
        injection_capacity=parse_value(payload.get("injectionCapacity")),
        withdrawal_capacity=parse_value(payload.get("withdrawalCapacity")),
        contracted_capacity=parse_value(payload.get("contractedCapacity")),
        available_capacity=parse_value(payload.get("availableCapacity")),
        covered_capacity=parse_value(payload.get("coveredCapacity")),
        status=str(payload["status"]) if payload.get("status") is not None else None,
        trend=str(payload["trend"]) if payload.get("trend") is not None else None,
        full=parse_value(payload.get("full")),
    )


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

    records = walk_hierarchy(data)
    countries = [r for r in records if r.level == HierarchyLevel.COUNTRY]
    rows = [
        _country_row_from_record(
            record,
            request_version=request_version,
            gas_day=gas_day,
            observed_at=observed_at,
        )
        for record in countries
    ]

    seen: set[tuple[int, date, datetime, str]] = set()
    for row in rows:
        key = (row.request_version, row.gas_day, row.observed_at, row.country_code)
        if key in seen:
            raise DuplicateKeyError(f"Duplicate country key: {key}")
        seen.add(key)
    return rows
