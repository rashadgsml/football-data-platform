"""Ingest the five sample StatsBomb matches into local storage and S3.

Downloads Bundesliga 2023/24 event files (Bayer Leverkusen matches) to
``data/raw/statsbomb/events/``, uploads them to the configured S3 bucket and
records them in ``manifest.json``. Requires the variables in ``.env``.

Files are downloaded from one pinned upstream commit per run, stored in S3 at
write-once content-addressed keys, and the run is recorded under
``statsbomb/runs/``. Valid local files are reused; pass ``--refresh`` to
download again and record a new version if upstream changed.

Usage:
    python scripts/ingest_sample_matches.py [--refresh]
"""

import argparse
from pathlib import Path

from football_platform.pipeline import ingest_matches
from football_platform.runs import new_run_id
from football_platform.storage import S3Storage

SAMPLE_MATCH_IDS = [
    3895107,
    3895158,
    3895292,
    3895320,
    3895340,
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="download again even if a valid local file exists",
    )
    args = parser.parse_args()

    events_dir = Path("data/raw/statsbomb/events")
    run_id = new_run_id()
    print("run_id:", run_id)

    results = ingest_matches(
        match_ids=SAMPLE_MATCH_IDS,
        output_dir=events_dir,
        manifest_path=events_dir / "manifest.json",
        storage=S3Storage(),
        run_id=run_id,
        refresh=args.refresh,
    )

    for result in results:
        print(result)


if __name__ == "__main__":
    main()
