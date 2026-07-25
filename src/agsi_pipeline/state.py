from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

from agsi_pipeline.paths import sync_state_path
from agsi_pipeline.storage import StorageContext, atomic_write_bytes, read_bytes

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncState:
    last_successful_reconciliation_date: date
    last_reconciled_request_version: int

    def to_dict(self) -> dict[str, object]:
        return {
            "last_successful_reconciliation_date": (
                self.last_successful_reconciliation_date.isoformat()
            ),
            "last_reconciled_request_version": self.last_reconciled_request_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SyncState:
        return cls(
            last_successful_reconciliation_date=date.fromisoformat(
                str(data["last_successful_reconciliation_date"])
            ),
            last_reconciled_request_version=int(str(data["last_reconciled_request_version"])),
        )


def read_sync_state_or_none(storage: StorageContext) -> SyncState | None:
    key = sync_state_path()
    full = storage.artifacts.full_path(key)
    if not storage.artifacts.fs.exists(full):
        logger.info("No sync state found")
        return None
    raw = read_bytes(storage.artifacts, key)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("sync state must be a JSON object")
    state = SyncState.from_dict(data)
    logger.info(
        "Loaded sync state: last_reconciliation=%s, request_version=%s",
        state.last_successful_reconciliation_date,
        state.last_reconciled_request_version,
    )
    return state


def write_sync_state_atomically(storage: StorageContext, state: SyncState) -> None:
    key = sync_state_path()
    payload = json.dumps(state.to_dict(), indent=2).encode("utf-8")
    atomic_write_bytes(storage.artifacts, key, payload)
    logger.info("Wrote sync state to %s", key)
