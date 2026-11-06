from pathlib import Path

from football_platform.ingestion import ingest_matches
from football_platform.storage import S3Storage

events_dir = Path("data/raw/statsbomb/events")

results = ingest_matches(
    match_ids=[
        3895107,
        3895158,
        3895292,
        3895320,
        3895340,
    ],
    output_dir=events_dir,
    manifest_path=events_dir / "manifest.json",
    storage=S3Storage(),
)

for result in results:
    print(result)