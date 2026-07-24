from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import vcr

CASSETTE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "cassettes"
CASSETTE_NAME = "refresh_recent.yaml"
MANIFEST_NAME = "manifest.json"
PLACEHOLDER_API_KEY = "your-api-key-here"
REPLAY_API_KEY = "replay-not-a-real-key"

SECRET_PATTERNS = (
    "AGSI_API_KEY=",
    PLACEHOLDER_API_KEY,
)

X_KEY_LEAK_RE = re.compile(
    r"x-key:\s*\n\s*-\s*(?!REDACTED(?:\s|$))(\S+)",
    re.IGNORECASE | re.MULTILINE,
)


def make_vcr(*, record_mode: str = "none") -> vcr.VCR:
    return vcr.VCR(
        cassette_library_dir=str(CASSETTE_DIR),
        filter_headers=[("x-key", "REDACTED")],
        decode_compressed_response=True,
        match_on=["method", "scheme", "host", "port", "path", "query"],
        record_mode=record_mode,
    )


def manifest_path() -> Path:
    return CASSETTE_DIR / MANIFEST_NAME


def cassette_path() -> Path:
    return CASSETTE_DIR / CASSETTE_NAME


def load_manifest() -> dict[str, object]:
    path = manifest_path()
    if not path.exists():
        raise FileNotFoundError(f"Missing cassette manifest: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("manifest must be a JSON object")
    return data


def save_manifest(
    *,
    base_url: str,
    gas_days: list[date],
    recorded_at: str,
) -> None:
    CASSETTE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "base_url": base_url,
        "recorded_at": recorded_at,
        "gas_days": [d.isoformat() for d in gas_days],
        "request_count": len(gas_days),
    }
    manifest_path().write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def gas_days_from_cassette() -> list[date]:
    import yaml

    path = cassette_path()
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    interactions = data.get("interactions", [])
    days: list[date] = []
    seen: set[str] = set()
    for interaction in interactions:
        uri = str(interaction["request"]["uri"])
        parsed = parse_qs(urlparse(uri).query)
        raw = parsed.get("date", [None])[0]
        if raw is None or raw in seen:
            continue
        seen.add(raw)
        days.append(date.fromisoformat(raw))
    return days


def normalize_cassette_file(*, max_interactions: int | None = None) -> int:
    import yaml

    path = cassette_path()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    seen: set[str] = set()
    unique: list[dict[str, object]] = []
    for interaction in data.get("interactions", []):
        uri = str(interaction["request"]["uri"])
        if uri in seen:
            continue
        seen.add(uri)
        unique.append(interaction)
        if max_interactions is not None and len(unique) >= max_interactions:
            break
    data["interactions"] = unique
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return len(unique)


def gas_days_from_manifest(manifest: dict[str, object]) -> list[date]:
    raw_days = manifest.get("gas_days")
    if not isinstance(raw_days, list):
        raise ValueError("manifest gas_days must be a list")
    return [date.fromisoformat(str(value)) for value in raw_days]


def iter_cassette_files() -> list[Path]:
    if not CASSETTE_DIR.exists():
        return []
    patterns = ("*.yaml", "*.yml", "*.json")
    files: list[Path] = []
    for pattern in patterns:
        files.extend(CASSETTE_DIR.glob(pattern))
    return sorted(files)
