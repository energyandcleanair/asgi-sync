from __future__ import annotations

from pathlib import Path


def test_dockerfile_contains_runtime_files() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    assert "sync-policy.toml" in dockerfile
    assert 'ENTRYPOINT ["agsi"]' in dockerfile
    assert "poetry.lock" in dockerfile
