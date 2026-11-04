from pathlib import Path

from football_platform.ingestion import ingest_match
from football_platform.storage import S3Storage

result = ingest_match(
    match_id=3895158,
    output_dir=Path("data/raw/statsbomb/events"),
    storage=S3Storage(),
)

print(result)