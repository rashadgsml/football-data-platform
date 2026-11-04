from pathlib import Path
from unittest.mock import Mock

from football_platform.ingestion import ingest_match


def test_ingest_match_uploads_events_to_s3(
    tmp_path: Path,
    monkeypatch,
) -> None:
    content = b'[{"id": 1, "type": {"name": "Pass"}}]'

    monkeypatch.setattr(
        "football_platform.ingestion.fetch_match_events",
        lambda match_id: content,
    )

    storage = Mock()
    storage.bucket = "football-raw"

    result = ingest_match(
        match_id=3895158,
        output_dir=tmp_path / "raw",
        storage=storage,
    )

    storage.create_bucket.assert_called_once()

    storage.upload_bytes.assert_called_once_with(
        "statsbomb/events/3895158.json",
        content,
    )

    assert result["match_id"] == 3895158
    assert result["event_count"] == 1
    assert result["s3_key"] == "statsbomb/events/3895158.json"
    assert result["s3_uri"] == (
        "s3://football-raw/statsbomb/events/3895158.json"
    )
