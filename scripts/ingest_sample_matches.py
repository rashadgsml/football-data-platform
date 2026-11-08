"""Ingest the five sample StatsBomb matches into local storage and S3.

Downloads Bundesliga 2023/24 event files (Bayer Leverkusen matches) to
``data/raw/statsbomb/events/``, uploads them to the configured S3 bucket and
records them in ``manifest.json``. Requires the variables in ``.env``.

Usage:
    python scripts/ingest_sample_matches.py
"""

from pathlib import Path

from football_platform.ingestion import ingest_matches
from football_platform.storage import S3Storage

SAMPLE_MATCH_IDS = [
    3895107,
    3895158,
    3895292,
    3895320,
    3895340,
]


def main() -> None:
    events_dir = Path("data/raw/statsbomb/events")

    results = ingest_matches(
        match_ids=SAMPLE_MATCH_IDS,
        output_dir=events_dir,
        manifest_path=events_dir / "manifest.json",
        storage=S3Storage(),
    )

    for result in results:
        print(result)


if __name__ == "__main__":
    main()
