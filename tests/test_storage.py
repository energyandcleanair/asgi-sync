from __future__ import annotations

import json

from agsi_pipeline.storage import (
    atomic_copy,
    atomic_write_bytes,
    exists,
    parse_storage_url,
    read_bytes,
)


def test_atomic_write_and_copy(storage_dirs: tuple[str, str]) -> None:
    artifacts = parse_storage_url(storage_dirs[0])
    public = parse_storage_url(storage_dirs[1])
    atomic_write_bytes(artifacts, "agsi/test.txt", b"hello")
    assert exists(artifacts, "agsi/test.txt")
    atomic_copy(artifacts, "agsi/test.txt", public, "agsi/published.txt")
    assert read_bytes(public, "agsi/published.txt") == b"hello"


def test_gzip_round_trip(storage_dirs: tuple[str, str]) -> None:
    import gzip

    artifacts = parse_storage_url(storage_dirs[0])
    payload = json.dumps({"ok": True}).encode("utf-8")
    atomic_write_bytes(artifacts, "agsi/raw/test.json.gz", gzip.compress(payload))
    raw = read_bytes(artifacts, "agsi/raw/test.json.gz")
    assert json.loads(gzip.decompress(raw)) == {"ok": True}
