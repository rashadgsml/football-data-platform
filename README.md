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

- **Pinned source.** A run resolves StatsBomb's `master` branch to a commit SHA once (one
  GitHub API call, only if a download is needed) and downloads every file from that commit, so
  the bytes can be traced to an exact upstream version. Transient network errors and HTTP 5xx
  are retried (3 attempts, exponential backoff); 4xx errors fail immediately.
- Validates each file: a non-empty JSON array of objects, each with the essential event fields
  (`id`, `index`, `period`, `timestamp`, `minute`, `second`, `type`, `possession`, `team`).
  Optional fields such as `location` and `player` may be absent or null.
- Saves it unchanged and atomically to `data/raw/statsbomb/events/<match_id>.json`, a local
  cache of the current version. A valid file that matches its recorded SHA-256 is reused with no
  network access; `--refresh` forces a new download.
- **Immutable versions in S3.** Each version is stored at the write-once key
  `statsbomb/events/<match_id>/<sha256>.json` and uploaded only if missing or different, then
  downloaded again to verify the checksum (ETags are never used). If upstream content changes,
  the new bytes get a new key and the old object stays; nothing is overwritten or deleted.
- **Manifest per match** at `statsbomb/manifests/events/<match_id>.json`, mirrored in a local
  `manifest.json` for development (file-locked). It holds the current version
  (`sha256`, `s3_key`, `source_commit`, `source_url`) and a `history` of superseded versions.
  `source_commit` is `null` for data ingested before commits were tracked: unknown, not guessed.
- Timestamps: `retrieved_at` is the last download from the source and `s3_uploaded_at` the last
  upload to S3. Neither changes when an existing file or object is only checked.
- **Run record** at `statsbomb/runs/<run_id>.json` (S3 runs only): source commit, per-match
  status (`downloaded` / `cached`), and, for a failed run, the failing match and error. A failure
  still stops the batch and is re-raised.

Limitations: the local cache holds only the current version (older versions live in S3); legacy
objects at `statsbomb/events/<match_id>.json` are left in place after migration and can be
deleted manually once you are sure they are no longer needed.

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
python scripts/ingest_sample_matches.py --refresh  # re-download and record changes
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
src/football_platform/   bronze (shared ingestion mechanics), pipeline (runs), source (pinned
                         upstream access), manifest, runs, storage, config, checksums
src/football_platform/datasets/   explicit per-dataset rules (events today)
scripts/                 manual scripts (sample ingestion, S3 smoke test)
tests/                   offline pytest suite
docker-compose.yml       local SeaweedFS
```
