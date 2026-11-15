# Football Data Engineering Platform

A portfolio project demonstrating reliable data ingestion, data quality,
incremental processing, analytical data storage, and orchestration using
public football event data.

## Data source

[StatsBomb Open Data](https://github.com/statsbomb/open-data). This project is
for educational and portfolio purposes, and StatsBomb's data usage and
attribution requirements apply.

## Status

| Layer | Technology | Status |
|---|---|---|
| Bronze | Python, boto3, SeaweedFS (S3 API) | Implemented |
| Silver | PySpark, Parquet | Planned |
| Gold | dbt, SQL, PostgreSQL | Planned |
| Orchestration | Apache Airflow | Planned |

### Bronze (implemented)

- Downloads original StatsBomb event JSON for a match, retrying transient network errors and
  HTTP 5xx responses (3 attempts, exponential backoff).
- Validates it: a non-empty JSON array of objects, each with the essential event fields
  (`id`, `index`, `period`, `timestamp`, `minute`, `second`, `type`, `possession`, `team`).
  Optional fields such as `location` and `player` may be absent or null.
- Saves it unchanged and atomically to `data/raw/statsbomb/events/<match_id>.json`. A valid local
  file that matches its recorded SHA-256 is reused; anything else is downloaded again.
- Uploads it to S3-compatible storage at `statsbomb/events/<match_id>.json` only when the object
  is missing or its SHA-256 differs, then downloads it again to verify the checksum. ETags are
  never used for comparison.
- Records one manifest object per match in S3 at `statsbomb/manifests/events/<match_id>.json`,
  and mirrors it in a local `manifest.json` for development (writes are file-locked). Both
  manifests are validated before anything is written.
- Manifest timestamps: `retrieved_at` is when the file was last downloaded from the source, and
  `s3_uploaded_at` is when it was last uploaded to S3. Neither changes when an existing file or
  object is only checked.

Five Bundesliga 2023/24 matches are ingested by `scripts/ingest_sample_matches.py`.

## Local setup

Requires Python 3.12+ and Docker.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Configuration and secrets

Secrets are never committed. Create your own local files from the templates:

1. `cp .env.example .env` and fill in the values.
2. `cp s3-config.example.json s3-config.json` and use the same access and secret keys as in `.env`.

`.env` and `s3-config.json` are ignored by Git. Docker Compose reads
`WEED_JWT_FILER_SIGNING_KEY` from `.env` and refuses to start without it.

The S3 identity has only `Read`, `Write` and `List`. If the bucket does not exist yet,
`create_bucket()` may need `Admin`; create the bucket once with an admin identity instead of
granting it to the ingestion identity.

#### Rotating credentials

1. Generate new values: `openssl rand -hex 16` (access key), `openssl rand -hex 32` (secret key
   and JWT signing key).
2. Update `.env` and `s3-config.json` (same access and secret key in both).
3. Restart: `docker compose down && docker compose up -d`.
4. Run `python scripts/s3_smoke_test.py` to confirm the new keys work.

Rotating invalidates old keys, including any that remain in Git history.

### Start storage

```bash
docker compose up -d
```

SeaweedFS exposes its S3 API on `127.0.0.1:8333` and its master on `127.0.0.1:9333`.
Both are reachable from the local machine only.

### Run

```bash
python scripts/s3_smoke_test.py          # check the S3 connection (writes under test/)
python scripts/ingest_sample_matches.py  # ingest the five sample matches
```

## Development

```bash
ruff check .
pytest -q
```

The tests in `tests/` run offline. They mock the network and S3 and never touch real storage.
`scripts/` holds manual scripts that need a running SeaweedFS and a populated `.env`.

## Repository layout

```
src/football_platform/   ingestion, manifest, storage, config, checksums
scripts/                 manual scripts (sample ingestion, S3 smoke test)
tests/                   offline pytest suite
docker-compose.yml       local SeaweedFS
```
