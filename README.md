# AGSI pipeline

Dockerised ELT pipeline that downloads, stores, and republishes [GIE AGSI](https://agsi.gie.eu) gas-storage data using cloud object storage only.

## Local setup

```bash
poetry install
cp .env.example .env
# edit .env with your API key and bucket URLs
poetry run agsi --help
poetry run pytest
```

## Integration tests (recorded API)

Offline end-to-end tests replay committed vcr cassettes captured from real AGSI responses:

```bash
poetry run pytest -m integration
```

Committed fixture location (safe to version-control):

- `tests/fixtures/cassettes/` — vcr YAML cassettes used for offline HTTP replay

Local pipeline runs still write to `data/` (gitignored).

To record or refresh cassettes locally (~10 API calls, requires a valid key in `.env`):

```bash
poetry run python scripts/record_cassettes.py
# or overwrite an existing cassette:
poetry run python scripts/record_cassettes.py --rewrite
```

### Security

- Never commit `.env`
- Cassettes redact the `x-key` header before persistence (`REDACTED`)
- Replay tests use a dummy API key; no live network calls
- `test_cassette_secrets` scans committed cassettes for accidental secret leakage
- Re-record cassettes after API shape changes, not on every test run

## Docker usage

```bash
docker build -t agsi-pipeline .
docker run --rm --env-file .env agsi-pipeline sync
docker run --rm --env-file .env agsi-pipeline reconcile
```

Smoke check (no network):

```bash
docker run --rm agsi-pipeline --help
```

## Configuration

Environment variables:

| Variable | Description |
|----------|-------------|
| `AGSI_API_BASE_URL` | API base URL (default `https://agsi.gie.eu`) |
| `AGSI_API_KEY` | GIE API key (`x-key` header) |
| `AGSI_ARTIFACTS_STORAGE_URL` | Pipeline bucket (`gs://...` or `file://...`) |
| `AGSI_PUBLIC_STORAGE_URL` | Public release bucket |
| `AGSI_REQUESTS_PER_MINUTE` | Rate limit (default `60`) |

Repository-root [`sync-policy.toml`](sync-policy.toml):

- `request_version` — raw request strategy version
- `history_start_date` — reconciliation start
- `recent_days` — rolling refresh window
- `reconciliation_interval_days` — days between full reconciliations

## API authentication

Register at [agsi.gie.eu/account](https://agsi.gie.eu/account) for a free API key. The pipeline sends it as the `x-key` header. Never log or persist the key.

## Bucket layout

**Artifacts bucket** (internal):

```text
agsi/raw/request_version=1/date=YYYY-MM-DD/observed_at=YYYY-MM-DDTHHMMSSZ/response.json.gz
agsi/silver/request_version=1/country_history/observed_year=YYYY/observed_month=MM/data.parquet
agsi/build/request_version=1/country_daily.parquet
agsi/sync-state.json
```

**Public bucket** (released):

```text
agsi/current/request_version=1/country_daily.parquet
```

GCS authentication uses Application Default Credentials (service account / Workload Identity). Buckets must exist; the pipeline does not create them.

## Request versions

Increment `request_version` in `sync-policy.toml` when the raw fetch strategy changes (endpoint, parameters, or what constitutes a complete snapshot). Parsing-only changes do not require a bump; rebuild derived datasets from existing raw data instead.

## Bitemporal semantics

| Field | Meaning |
|-------|---------|
| `gas_day` | Valid time — the gas day represented by the source data |
| `observed_at` | System time — when the pipeline observed the API response (one timestamp per job) |
| `source_updated_at` | Source `updatedAt` field, preserved separately |

Complete-snapshot semantics: for each gas day, select one `observed_at` (latest, or latest ≤ as-of), then take **all** country rows from that snapshot.

## Synchronisation decisions

`agsi sync` reads `sync-policy.toml` and `agsi/sync-state.json` from the artifacts bucket, then:

- **Full reconciliation** when: no state, request version changed, reconciliation interval elapsed, or `--reconcile`
- **Recent refresh** otherwise (last `recent_days` gas days)

After a successful fetch: `build-history` → `build-current` → `publish-release`. Reconciliation state updates only after a complete successful reconciliation including publish.

## Scheduling

Run daily:

```bash
agsi sync
```

The command chooses refresh vs reconciliation automatically.

## CLI commands

```text
agsi sync [--reconcile]
agsi fetch-day YYYY-MM-DD [--observed-at ...]
agsi refresh-recent [--days N]
agsi reconcile [--start ...] [--end ...]
agsi build-history
agsi build-current
agsi publish-release
agsi build-as-of --as-of 2026-06-01T00:00:00Z --output output.parquet
```

## Source attribution

Published Parquet files include metadata: `Source: GIE AGSI Transparency Platform`.

When republishing data, credit **GIE (Gas Infrastructure Europe), AGSI** as the source.

