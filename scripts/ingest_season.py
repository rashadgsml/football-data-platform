"""Ingest a StatsBomb competition season into local storage and S3.

Reads competitions and the season's match list, then lineups and events for
every available match. All files come from one pinned upstream commit, are
validated and reconciled against the match records, and are stored at
write-once keys. The run is recorded under ``statsbomb/runs/``. Valid local
files are reused; ``--refresh`` downloads again and records a new version if
upstream changed. Requires the variables in ``.env``.

Usage:
    python scripts/ingest_season.py --competition 9 --season 281
    python scripts/ingest_season.py --competition 9 --season 281 --limit 5
    python scripts/ingest_season.py --competition 9 --season 281 --match-id 3895158
"""

import argparse
import logging
import sys
from pathlib import Path

from football_platform.pipeline import ingest_season
from football_platform.runs import new_run_id
from football_platform.storage import S3Storage

DATA_ROOT = Path("data/raw/statsbomb")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--competition", type=int, required=True, help="competition_id")
    parser.add_argument("--season", type=int, required=True, help="season_id")
    parser.add_argument(
        "--match-id", type=int, action="append", dest="match_ids",
        help="ingest only this match (repeatable)",
    )
    parser.add_argument("--limit", type=int, help="ingest at most this many matches")
    parser.add_argument(
        "--refresh", action="store_true",
        help="download again even if a valid local file exists",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    run_id = new_run_id()

    try:
        record = ingest_season(
            args.competition,
            args.season,
            DATA_ROOT,
            storage=S3Storage(),
            run_id=run_id,
            refresh=args.refresh,
            match_ids=args.match_ids,
            limit=args.limit,
        )
    except Exception as error:  # noqa: BLE001 - report any failure and exit non-zero
        print(f"Run {run_id} FAILED: {error}", file=sys.stderr)
        return 1

    downloaded = sum(
        1
        for match in record["matches"]
        for dataset in ("lineups", "events")
        if match[dataset]["status"] == "downloaded"
    )
    print(f"Run {run_id} succeeded")
    print(f"  source commit : {record['source_commit'] or 'none (everything was cached)'}")
    print(f"  matches       : {len(record['matches'])} ingested, {len(record['skipped_matches'])} skipped")
    print(f"  downloaded    : {downloaded} lineup/event files")
    print(f"  stale matches : {record['stale_match_ids'] or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
