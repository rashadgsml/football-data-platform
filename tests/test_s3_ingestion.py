import json
from pathlib import Path

import pytest
from factories import FakeStorage, events_bytes

from football_platform.checksums import calculate_sha256
from football_platform.datasets.events import ingest_match, save_raw_events

FETCH = "football_platform.datasets.events.fetch_match_events"


def key_for(content: bytes, match_id: int = 3895158) -> str:
    """Write-once, content-addressed S3 key for one version of a match."""

    return f"statsbomb/events/{match_id}/{calculate_sha256(content)}.json"


MANIFEST_KEY = "statsbomb/manifests/events/3895158.json"


@pytest.fixture
def upstream(monkeypatch):
    """Serve fixed content as the source and count downloads."""

    class Upstream:
        content = events_bytes(1)
        downloads = 0

    def fetch(match_id, commit):
        Upstream.downloads += 1
        return Upstream.content

    monkeypatch.setattr(FETCH, fetch)
    return Upstream


@pytest.fixture
def clock(monkeypatch):
    """Control the time reported to ingestion: clock.set('T2')."""

    class Clock:
        now = "T1"

        def set(self, value):
            Clock.now = value

    monkeypatch.setattr("football_platform.bronze.utc_now", lambda: Clock.now)
    return Clock()


def stored_manifest(storage: FakeStorage) -> dict:
    return json.loads(storage.objects[MANIFEST_KEY])


def test_ingest_match_uploads_events_to_s3(tmp_path: Path, upstream) -> None:
    storage = FakeStorage()

    result = ingest_match(
        match_id=3895158,
        output_dir=tmp_path / "raw",
        storage=storage,
    )

    assert storage.create_bucket_calls == 1
    key = key_for(upstream.content)
    assert storage.uploads.count(key) == 1
    assert storage.objects[key] == upstream.content

    assert result["match_id"] == 3895158
    assert result["event_count"] == 1
    assert result["s3_key"] == key
    assert result["s3_uri"] == f"s3://football-raw/{key}"
    assert result["s3_uploaded"] is True


def test_unchanged_object_is_not_uploaded_again(tmp_path, upstream):
    storage = FakeStorage()

    ingest_match(3895158, tmp_path, storage=storage)
    uploads_after_first_run = list(storage.uploads)

    result = ingest_match(3895158, tmp_path, storage=storage)

    assert storage.uploads == uploads_after_first_run
    assert result["s3_uploaded"] is False


def test_local_file_is_skipped_but_still_uploaded_when_missing_from_s3(tmp_path):
    """The regression that motivated the redesign: skipped != already in S3."""

    content = events_bytes(3)
    save_raw_events(3895158, content, tmp_path)
    storage = FakeStorage()

    result = ingest_match(3895158, tmp_path, storage=storage)  # fetch is not patched

    assert result["skipped"] is True
    assert result["s3_uploaded"] is True
    assert storage.objects[key_for(content)] == content


def test_different_s3_object_is_replaced(tmp_path, upstream):
    storage = FakeStorage()
    storage.objects[key_for(upstream.content)] = b"stale content"

    result = ingest_match(3895158, tmp_path, storage=storage)

    assert result["s3_uploaded"] is True
    assert storage.objects[key_for(upstream.content)] == upstream.content


def test_comparison_uses_sha256_of_content_not_etag(tmp_path, upstream):
    """Same length, different bytes: only a content hash can tell them apart."""

    storage = FakeStorage()
    same_length = bytes(len(upstream.content))
    storage.objects[key_for(upstream.content)] = same_length

    result = ingest_match(3895158, tmp_path, storage=storage)

    assert result["s3_uploaded"] is True
    assert calculate_sha256(storage.objects[key_for(upstream.content)]) == result["sha256"]


def test_upload_is_verified_and_a_mismatch_fails_loudly(tmp_path, upstream):
    storage = FakeStorage(corrupt_on_upload=True)

    with pytest.raises(RuntimeError, match="verification failed"):
        ingest_match(3895158, tmp_path, storage=storage)

    # No manifest may claim an object that failed verification.
    assert MANIFEST_KEY not in storage.objects
    assert not (tmp_path / "manifest.json").exists()


def test_tampered_local_file_does_not_overwrite_s3(tmp_path, upstream):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)
    original = upstream.content

    (tmp_path / "3895158.json").write_bytes(events_bytes(1, label="tampered-"))

    ingest_match(3895158, tmp_path, storage=storage)

    assert storage.objects[key_for(original)] == original
    assert (tmp_path / "3895158.json").read_bytes() == original


def test_one_manifest_object_is_written_per_match(tmp_path, upstream, clock):
    storage = FakeStorage()

    ingest_match(3895158, tmp_path, storage=storage)
    ingest_match(3895159, tmp_path, storage=storage)

    manifest = stored_manifest(storage)
    assert sorted(k for k in storage.objects if "manifests" in k) == [
        "statsbomb/manifests/events/3895158.json",
        "statsbomb/manifests/events/3895159.json",
    ]
    assert manifest["match_id"] == 3895158
    assert manifest["sha256"] == calculate_sha256(upstream.content)
    assert manifest["s3_key"] == key_for(upstream.content)
    assert manifest["source"] == "StatsBomb Open Data"


def test_manifests_do_not_live_under_the_events_prefix(tmp_path, upstream):
    storage = FakeStorage()

    ingest_match(3895158, tmp_path, storage=storage)

    event_objects = [k for k in storage.objects if k.startswith("statsbomb/events/")]
    assert event_objects == [key_for(upstream.content)]


def test_first_ingest_records_matching_download_and_upload_times(tmp_path, upstream, clock):
    storage = FakeStorage()

    ingest_match(3895158, tmp_path, storage=storage)

    manifest = stored_manifest(storage)
    assert manifest["retrieved_at"] == "T1"
    assert manifest["s3_uploaded_at"] == "T1"


def test_timestamps_and_manifests_are_untouched_when_nothing_changes(tmp_path, upstream, clock):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)
    uploads = list(storage.uploads)
    local_manifest = (tmp_path / "manifest.json").read_text()

    clock.set("T2")
    ingest_match(3895158, tmp_path, storage=storage)

    assert upstream.downloads == 1
    assert storage.uploads == uploads  # neither the event nor the manifest
    assert stored_manifest(storage)["retrieved_at"] == "T1"
    assert stored_manifest(storage)["s3_uploaded_at"] == "T1"
    assert (tmp_path / "manifest.json").read_text() == local_manifest


def test_source_retrieval_and_s3_upload_times_are_distinct(tmp_path, upstream, clock):
    ingest_match(3895158, tmp_path)  # local only, at T1

    clock.set("T2")
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)  # S3 added later

    manifest = stored_manifest(storage)
    assert upstream.downloads == 1
    assert manifest["retrieved_at"] == "T1"
    assert manifest["s3_uploaded_at"] == "T2"


def test_existing_object_without_recorded_upload_time_keeps_it_unknown(tmp_path, upstream, clock):
    """Adopting a pre-existing identical S3 object must not invent an upload time."""

    ingest_match(3895158, tmp_path)
    storage = FakeStorage()
    storage.objects[key_for(upstream.content)] = upstream.content

    clock.set("T2")
    result = ingest_match(3895158, tmp_path, storage=storage)

    assert result["s3_uploaded"] is False
    assert key_for(upstream.content) not in storage.uploads
    assert stored_manifest(storage)["s3_uploaded_at"] is None
    assert stored_manifest(storage)["retrieved_at"] == "T1"


def test_run_without_s3_keeps_previously_recorded_s3_fields(tmp_path, upstream, clock):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)

    clock.set("T2")
    ingest_match(3895158, tmp_path)  # no storage

    entry = json.loads((tmp_path / "manifest.json").read_text())[0]
    assert entry["s3_key"] == key_for(upstream.content)
    assert entry["s3_uploaded_at"] == "T1"


def test_invalid_s3_manifest_stops_ingestion_before_any_write(tmp_path, monkeypatch):
    storage = FakeStorage()
    storage.objects[MANIFEST_KEY] = b"not json"

    def fail(*args):
        raise AssertionError("must not download")

    monkeypatch.setattr(FETCH, fail)

    with pytest.raises(ValueError, match="Manifest for match 3895158"):
        ingest_match(3895158, tmp_path / "events", storage=storage)

    assert storage.uploads == []
    assert storage.create_bucket_calls == 0
    assert not (tmp_path / "events").exists()


@pytest.mark.parametrize(
    "manifest",
    [b"[]", b'{"match_id": 1, "sha256": "' + b"0" * 64 + b'"}', b'{"match_id": 3895158}'],
)
def test_structurally_invalid_s3_manifest_is_rejected(tmp_path, upstream, manifest):
    storage = FakeStorage()
    storage.objects[MANIFEST_KEY] = manifest

    with pytest.raises((ValueError, TypeError), match="Manifest for match 3895158"):
        ingest_match(3895158, tmp_path, storage=storage)

    assert upstream.downloads == 0
    assert storage.uploads == []


def test_invalid_local_manifest_stops_ingestion_before_upload(tmp_path, upstream):
    storage = FakeStorage()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("invalid JSON")

    with pytest.raises(ValueError, match="invalid JSON"):
        ingest_match(3895158, tmp_path / "events", manifest_path, storage)

    assert upstream.downloads == 0
    assert storage.uploads == []
    assert not (tmp_path / "events").exists()
