from __future__ import annotations

import pytest

from tests.integration.cassettes import SECRET_PATTERNS, X_KEY_LEAK_RE, iter_cassette_files


@pytest.mark.integration
def test_cassettes_contain_no_secrets() -> None:
    files = iter_cassette_files()
    assert files, "expected committed cassette files under tests/fixtures/cassettes/"

    for path in files:
        if path.name == "manifest.json":
            continue
        content = path.read_text(encoding="utf-8")
        for pattern in SECRET_PATTERNS:
            if pattern.lower() in content.lower():
                pytest.fail(f"Possible secret in cassette {path}: matched {pattern!r}")
        if X_KEY_LEAK_RE.search(content):
            pytest.fail(f"Possible API key value in cassette {path} x-key header")
