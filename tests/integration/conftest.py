from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.cassettes import CASSETTE_DIR, load_manifest


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, object]:
    return {
        "cassette_library_dir": str(CASSETTE_DIR),
        "filter_headers": [("x-key", "REDACTED")],
        "decode_compressed_response": True,
        "match_on": ["method", "scheme", "host", "port", "path", "query"],
        "allow_playback_repeats": True,
    }


@pytest.fixture
def local_storage(storage_dirs: tuple[str, str]):
    from agsi_pipeline.storage import open_storage_context

    return open_storage_context(storage_dirs[0], storage_dirs[1])


@pytest.fixture
def integration_policy(tmp_path: Path, cassette_manifest: dict[str, object]) -> Path:
    recent_days = int(cassette_manifest["request_count"])
    path = tmp_path / "sync-policy.toml"
    path.write_text(
        f"request_version = 1\n"
        f'history_start_date = "2011-01-01"\n'
        f"recent_days = {recent_days}\n"
        f"reconciliation_interval_days = 31\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture(scope="module")
def cassette_manifest() -> dict[str, object]:
    try:
        return load_manifest()
    except FileNotFoundError as exc:
        pytest.skip(str(exc))


@pytest.fixture
def integration_env(storage_dirs: tuple[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.integration.cassettes import REPLAY_API_KEY

    monkeypatch.setenv("AGSI_ARTIFACTS_STORAGE_URL", storage_dirs[0])
    monkeypatch.setenv("AGSI_PUBLIC_STORAGE_URL", storage_dirs[1])
    monkeypatch.setenv("AGSI_API_KEY", REPLAY_API_KEY)
