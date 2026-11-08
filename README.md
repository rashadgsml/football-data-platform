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

- Downloads original StatsBomb event JSON for a match and validates that it is a JSON array.
- Saves it atomically to `data/raw/statsbomb/events/<match_id>.json`.
- Uploads it unchanged to S3-compatible storage at `statsbomb/events/<match_id>.json`.
- Records source URL, SHA-256 checksum, event count, S3 location and retrieval time in a
  local `manifest.json`, with one entry per match.
- Skips the download when a valid local file already exists.

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
