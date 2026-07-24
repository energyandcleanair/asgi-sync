from __future__ import annotations

from pathlib import Path

import pytest

from agsi_pipeline.storage import open_storage_context


@pytest.fixture
def policy_file(tmp_path: Path) -> Path:
    path = tmp_path / "sync-policy.toml"
    path.write_text(
        'request_version = 1\n'
        'history_start_date = "2011-01-01"\n'
        'recent_days = 30\n'
        'reconciliation_interval_days = 31\n',
        encoding="utf-8",
    )
    return path


@pytest.fixture
def storage_dirs(tmp_path: Path) -> tuple[str, str]:
    artifacts = tmp_path / "artifacts"
    public = tmp_path / "public"
    artifacts.mkdir()
    public.mkdir()
    return f"file://{artifacts}", f"file://{public}"


@pytest.fixture
def storage(storage_dirs: tuple[str, str]):
    return open_storage_context(storage_dirs[0], storage_dirs[1])


@pytest.fixture(autouse=True)
def _env_storage(
    storage_dirs: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    if request.node.get_closest_marker("integration") or request.node.get_closest_marker("record"):
        return
    monkeypatch.setenv("AGSI_ARTIFACTS_STORAGE_URL", storage_dirs[0])
    monkeypatch.setenv("AGSI_PUBLIC_STORAGE_URL", storage_dirs[1])
    monkeypatch.setenv("AGSI_API_KEY", "test-key")
