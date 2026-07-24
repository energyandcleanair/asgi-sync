from __future__ import annotations

import gzip
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import fsspec
from fsspec.spec import AbstractFileSystem


@dataclass(frozen=True)
class FsRoot:
    fs: AbstractFileSystem
    root: str

    def full_path(self, key: str) -> str:
        if self.root:
            return f"{self.root.rstrip('/')}/{key.lstrip('/')}"
        return key.lstrip("/")


@dataclass(frozen=True)
class StorageContext:
    artifacts: FsRoot
    public: FsRoot


def parse_storage_url(storage_url: str) -> FsRoot:
    if storage_url.startswith("file://"):
        path = storage_url.removeprefix("file://")
        local_path = str(Path(path).resolve())
        fs = fsspec.filesystem("file")
        fs.makedirs(local_path, exist_ok=True)
        return FsRoot(fs=fs, root=local_path)

    if storage_url.startswith("gs://"):
        without_scheme = storage_url.removeprefix("gs://")
        bucket, _, prefix = without_scheme.partition("/")
        fs = fsspec.filesystem("gcs")
        return FsRoot(fs=fs, root=f"{bucket}/{prefix}".rstrip("/") if prefix else bucket)

    raise ValueError(f"Unsupported storage URL: {storage_url!r}")


def open_storage_context(artifacts_url: str, public_url: str) -> StorageContext:
    return StorageContext(
        artifacts=parse_storage_url(artifacts_url),
        public=parse_storage_url(public_url),
    )


def exists(fs_root: FsRoot, key: str) -> bool:
    return bool(fs_root.fs.exists(fs_root.full_path(key)))


def read_bytes(fs_root: FsRoot, key: str) -> bytes:
    with fs_root.fs.open(fs_root.full_path(key), "rb") as f:
        return cast(bytes, f.read())


def read_gzip_json(fs_root: FsRoot, key: str) -> dict[str, Any]:
    data = read_bytes(fs_root, key)
    return cast(dict[str, Any], json.loads(gzip.decompress(data).decode("utf-8")))


def atomic_write_bytes(fs_root: FsRoot, key: str, data: bytes) -> None:
    full = fs_root.full_path(key)
    parent = str(Path(full).parent)
    if hasattr(fs_root.fs, "makedirs"):
        fs_root.fs.makedirs(parent, exist_ok=True)
    tmp = f"{full}.tmp.{uuid.uuid4().hex}"
    with fs_root.fs.open(tmp, "wb") as f:
        f.write(data)
    fs_root.fs.mv(tmp, full)


_COPY_CHUNK_SIZE = 1024 * 1024


def atomic_copy(src: FsRoot, src_key: str, dst: FsRoot, dst_key: str) -> None:
    src_full = src.full_path(src_key)
    dst_full = dst.full_path(dst_key)
    parent = str(Path(dst_full).parent)
    if hasattr(dst.fs, "makedirs"):
        dst.fs.makedirs(parent, exist_ok=True)
    tmp = f"{dst_full}.tmp.{uuid.uuid4().hex}"
    with src.fs.open(src_full, "rb") as reader, dst.fs.open(tmp, "wb") as writer:
        while True:
            chunk = reader.read(_COPY_CHUNK_SIZE)
            if not chunk:
                break
            writer.write(chunk)
    dst.fs.mv(tmp, dst_full)


def list_files(fs_root: FsRoot, prefix: str) -> list[str]:
    full_prefix = fs_root.full_path(prefix)
    if not fs_root.fs.exists(full_prefix):
        return []
    paths = fs_root.fs.find(full_prefix)
    root_prefix = fs_root.root.rstrip("/") + "/"
    keys: list[str] = []
    for path in paths:
        if path.startswith(root_prefix):
            keys.append(path.removeprefix(root_prefix))
        elif path.startswith(fs_root.root):
            remainder = path[len(fs_root.root) :].lstrip("/")
            keys.append(remainder)
        else:
            keys.append(path)
    return sorted(keys)
