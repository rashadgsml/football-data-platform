import json

import pytest

from football_platform.ingestion import ingest_matches, save_raw_events


def test_save_raw_events(tmp_path):
    events = [
        {"id": "event-1", "type": {"name": "Pass"}},
        {"id": "event-2", "type": {"name": "Shot"}},
    ]
    content = json.dumps(events).encode("utf-8")

    result = save_raw_events(3895158, content, tmp_path)

    saved_file = tmp_path / "3895158.json"

    assert saved_file.exists()
    assert saved_file.read_bytes() == content
    assert result["match_id"] == 3895158
    assert result["event_count"] == 2
    assert len(result["sha256"]) == 64


def test_save_raw_events_rejects_invalid_json(tmp_path):
    with pytest.raises(ValueError, match="valid JSON"):
        save_raw_events(3895158, b"not valid JSON", tmp_path)


def test_save_raw_events_rejects_non_array_json(tmp_path):
    with pytest.raises(TypeError, match="JSON array"):
        save_raw_events(3895158, b'{"event": "Pass"}', tmp_path)


from unittest.mock import patch

from football_platform.ingestion import ingest_match


def test_ingest_match(tmp_path):
    events = [{"id": "event-1", "type": {"name": "Pass"}}]
    content = json.dumps(events).encode("utf-8")

    with patch(
        "football_platform.ingestion.fetch_match_events",
        return_value=content,
    ):
        result = ingest_match(3895158, tmp_path)

    assert result["match_id"] == 3895158
    assert result["event_count"] == 1
    assert (tmp_path / "3895158.json").exists()


def test_ingest_match_skips_existing_valid_file(tmp_path):
    existing_content = b'[{"id": "existing-event"}]'
    file_path = tmp_path / "3895158.json"
    file_path.write_bytes(existing_content)

    with patch(
        "football_platform.ingestion.fetch_match_events"
    ) as mock_fetch:
        result = ingest_match(3895158, tmp_path)

    mock_fetch.assert_not_called()
    assert result["skipped"] is True
    assert result["event_count"] == 1


def test_ingest_match_replaces_corrupted_file(tmp_path):
    file_path = tmp_path / "3895158.json"
    file_path.write_text("corrupted data")

    new_content = b'[{"id": "new-event"}]'

    with patch(
        "football_platform.ingestion.fetch_match_events",
        return_value=new_content,
    ) as mock_fetch:
        result = ingest_match(3895158, tmp_path)

    mock_fetch.assert_called_once_with(3895158)
    assert result["skipped"] is False
    assert json.loads(file_path.read_bytes())[0]["id"] == "new-event"


def test_ingest_match_creates_manifest(tmp_path):
    events = [{"id": "event-1", "type": {"name": "Pass"}}]
    content = json.dumps(events).encode("utf-8")
    manifest_path = tmp_path / "manifest.json"

    with patch(
        "football_platform.ingestion.fetch_match_events",
        return_value=content,
    ):
        result = ingest_match(
            3895158,
            tmp_path / "events",
            manifest_path,
        )

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(saved_manifest) == 1
    assert saved_manifest[0]["match_id"] == 3895158
    assert saved_manifest[0]["event_count"] == 1
    assert saved_manifest[0]["source"] == "StatsBomb Open Data"
    assert saved_manifest[0]["source_url"].endswith("/3895158.json")
    assert "retrieved_at" in saved_manifest[0]
    assert saved_manifest[0]["sha256"] == result["sha256"]


def test_ingest_match_does_not_duplicate_manifest(tmp_path):
    content = b'[{"id": "event-1"}]'
    manifest_path = tmp_path / "manifest.json"

    with patch(
        "football_platform.ingestion.fetch_match_events",
        return_value=content,
    ):
        ingest_match(3895158, tmp_path / "events", manifest_path)
        ingest_match(3895158, tmp_path / "events", manifest_path)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(manifest) == 1


def test_ingest_match_rejects_corrupted_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("invalid JSON")

    with patch(
        "football_platform.ingestion.fetch_match_events",
        return_value=b'[{"id": "event-1"}]',
    ), pytest.raises(ValueError, match="invalid JSON"):
        ingest_match(3895158, tmp_path / "events", manifest_path)


def test_ingest_matches_processes_multiple_matches(tmp_path):
    content = b'[{"id": "event-1"}]'

    with patch(
        "football_platform.ingestion.fetch_match_events",
        return_value=content,
    ):
        results = ingest_matches(
            [3895158, 3895159],
            tmp_path / "events",
            tmp_path / "manifest.json",
        )

    assert len(results) == 2
    assert [result["match_id"] for result in results] == [3895158, 3895159]


def test_ingest_matches_rejects_duplicate_ids(tmp_path):
    with pytest.raises(ValueError, match="unique"):
        ingest_matches(
            [3895158, 3895158],
            tmp_path / "events",
        )

