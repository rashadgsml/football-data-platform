import json
import os
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from factories import COMMIT, events_bytes, make_events

from football_platform.ingestion import (
    ingest_match,
    ingest_matches,
    save_raw_events,
)

FETCH = "football_platform.ingestion.fetch_match_events"


def test_save_raw_events(tmp_path):
    content = json.dumps(make_events(2)).encode("utf-8")

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


@pytest.mark.parametrize("content", [b"[]", b"[1, 2, 3]", b'[{"id": "only-an-id"}]'])
def test_save_raw_events_rejects_empty_or_malformed_events(tmp_path, content):
    with pytest.raises((ValueError, TypeError)):
        save_raw_events(3895158, content, tmp_path)

    assert not (tmp_path / "3895158.json").exists()


def test_ingest_match(tmp_path):
    content = events_bytes(1)

    with patch(FETCH, return_value=content):
        result = ingest_match(3895158, tmp_path)

    assert result["match_id"] == 3895158
    assert result["event_count"] == 1
    assert (tmp_path / "3895158.json").exists()


def test_ingest_match_skips_existing_valid_file(tmp_path):
    file_path = tmp_path / "3895158.json"
    file_path.write_bytes(events_bytes(1))

    with patch(FETCH) as mock_fetch:
        result = ingest_match(3895158, tmp_path)

    mock_fetch.assert_not_called()
    assert result["skipped"] is True
    assert result["event_count"] == 1


def test_ingest_match_replaces_corrupted_file(tmp_path):
    file_path = tmp_path / "3895158.json"
    file_path.write_text("corrupted data")

    new_content = events_bytes(1, label="new-")

    with patch(FETCH, return_value=new_content) as mock_fetch:
        result = ingest_match(3895158, tmp_path)

    mock_fetch.assert_called_once_with(3895158, COMMIT)
    assert result["skipped"] is False
    assert json.loads(file_path.read_bytes())[0]["id"] == "event-new-1"


@pytest.mark.parametrize(
    "cached",
    [b'{"not": "an array"}', b"[]", b"[1, 2]", b'[{"id": "no-other-fields"}]', b"\xff\xfe"],
)
def test_ingest_match_redownloads_invalid_cached_data(tmp_path, cached):
    file_path = tmp_path / "3895158.json"
    file_path.write_bytes(cached)
    fresh = events_bytes(2)

    with patch(FETCH, return_value=fresh) as mock_fetch:
        result = ingest_match(3895158, tmp_path)

    mock_fetch.assert_called_once_with(3895158, COMMIT)
    assert result["skipped"] is False
    assert file_path.read_bytes() == fresh


def test_ingest_match_creates_manifest(tmp_path):
    content = events_bytes(1)
    manifest_path = tmp_path / "manifest.json"

    with patch(FETCH, return_value=content):
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
    content = events_bytes(1)
    manifest_path = tmp_path / "manifest.json"

    with patch(FETCH, return_value=content):
        ingest_match(3895158, tmp_path / "events", manifest_path)
        ingest_match(3895158, tmp_path / "events", manifest_path)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(manifest) == 1


def test_ingest_match_rejects_corrupted_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("invalid JSON")

    with patch(FETCH, return_value=events_bytes(1)), pytest.raises(
        ValueError, match="invalid JSON"
    ):
        ingest_match(3895158, tmp_path / "events", manifest_path)


def test_corrupted_manifest_is_detected_before_any_data_is_written(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("invalid JSON")

    with patch(FETCH) as mock_fetch, pytest.raises(ValueError):
        ingest_match(3895158, tmp_path / "events", manifest_path)

    mock_fetch.assert_not_called()
    assert not (tmp_path / "events").exists()


def test_ingest_matches_processes_multiple_matches(tmp_path):
    content = events_bytes(1)

    with patch(FETCH, return_value=content):
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


def test_ingest_match_rejects_empty_upstream_response(tmp_path):
    with patch(FETCH, return_value=b"[]"), pytest.raises(ValueError, match="empty"):
        ingest_match(3895158, tmp_path / "events")

    assert not (tmp_path / "events" / "3895158.json").exists()


def test_retrieved_at_is_preserved_when_nothing_is_downloaded(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    times = iter(["2026-01-01T00:00:00+00:00", "2026-02-02T00:00:00+00:00"])
    monkeypatch.setattr("football_platform.ingestion.utc_now", lambda: next(times))

    with patch(FETCH, return_value=events_bytes(1)) as mock_fetch:
        ingest_match(3895158, tmp_path / "events", manifest_path)
        second = ingest_match(3895158, tmp_path / "events", manifest_path)

    entry = json.loads(manifest_path.read_text())[0]

    mock_fetch.assert_called_once()
    assert second["skipped"] is True
    assert entry["retrieved_at"] == "2026-01-01T00:00:00+00:00"
    assert entry["retrieved_at_basis"] == "download"


def test_manifest_file_is_not_rewritten_when_nothing_changed(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    with patch(FETCH, return_value=events_bytes(1)):
        ingest_match(3895158, tmp_path / "events", manifest_path)

    os.utime(manifest_path, (1, 1))

    ingest_match(3895158, tmp_path / "events", manifest_path)

    assert manifest_path.stat().st_mtime == 1


def test_retrieved_at_is_updated_when_a_new_download_happens(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    events_dir = tmp_path / "events"
    times = iter(["2026-01-01T00:00:00+00:00", "2026-02-02T00:00:00+00:00"])
    monkeypatch.setattr("football_platform.ingestion.utc_now", lambda: next(times))

    with patch(FETCH, return_value=events_bytes(1)):
        ingest_match(3895158, events_dir, manifest_path)

    (events_dir / "3895158.json").write_text("corrupted")

    with patch(FETCH, return_value=events_bytes(1)):
        ingest_match(3895158, events_dir, manifest_path)

    entry = json.loads(manifest_path.read_text())[0]
    assert entry["retrieved_at"] == "2026-02-02T00:00:00+00:00"


def test_cached_file_that_differs_from_recorded_checksum_is_redownloaded(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    events_dir = tmp_path / "events"
    original = events_bytes(2)

    with patch(FETCH, return_value=original):
        ingest_match(3895158, events_dir, manifest_path)

    # Valid JSON, valid events, but not what was recorded: local tampering.
    (events_dir / "3895158.json").write_bytes(events_bytes(2, label="tampered-"))

    with patch(FETCH, return_value=original) as mock_fetch:
        result = ingest_match(3895158, events_dir, manifest_path)

    mock_fetch.assert_called_once_with(3895158, COMMIT)
    assert result["skipped"] is False
    assert (events_dir / "3895158.json").read_bytes() == original


def test_unrecorded_local_file_uses_its_mtime_as_retrieved_at(tmp_path):
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    file_path = events_dir / "3895158.json"
    file_path.write_bytes(events_bytes(1))
    os.utime(file_path, (1_700_000_000, 1_700_000_000))

    with patch(FETCH) as mock_fetch:
        ingest_match(3895158, events_dir)

    entry = json.loads((events_dir / "manifest.json").read_text())[0]

    mock_fetch.assert_not_called()
    assert entry["retrieved_at"] == datetime.fromtimestamp(1_700_000_000, UTC).isoformat()
    assert entry["retrieved_at_basis"] == "file_mtime"
