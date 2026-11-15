"""Source pinning, immutable Bronze versions and run records."""

import json

import pytest
from factories import COMMIT, OTHER_COMMIT, FakeStorage, events_bytes

from football_platform.checksums import calculate_sha256
from football_platform.datasets.events import ingest_match
from football_platform.manifest import parse_entry
from football_platform.pipeline import ingest_matches

FETCH = "football_platform.datasets.events.fetch_match_events"
MANIFEST_KEY = "statsbomb/manifests/events/3895158.json"


def key_for(content: bytes, match_id: int = 3895158) -> str:
    return f"statsbomb/events/{match_id}/{calculate_sha256(content)}.json"


@pytest.fixture
def upstream(monkeypatch):
    class Upstream:
        content = events_bytes(1)

    Upstream.fetched_commits = []

    def fetch(match_id, commit):
        Upstream.fetched_commits.append(commit)
        return Upstream.content

    monkeypatch.setattr(FETCH, fetch)
    return Upstream


def manifest(storage):
    return json.loads(storage.objects[MANIFEST_KEY])


# --- source pinning -------------------------------------------------------


def test_download_uses_and_records_the_resolved_commit(tmp_path, upstream):
    storage = FakeStorage()

    result = ingest_match(3895158, tmp_path, storage=storage)

    assert upstream.fetched_commits == [COMMIT]
    assert result["source_commit"] == COMMIT
    assert manifest(storage)["source_commit"] == COMMIT
    assert COMMIT in manifest(storage)["source_url"]


def test_fully_cached_run_does_not_resolve_the_commit(tmp_path, upstream, upstream_commit):
    ingest_match(3895158, tmp_path)
    assert upstream_commit.calls == 1

    ingest_match(3895158, tmp_path)

    assert upstream_commit.calls == 1


def test_a_batch_resolves_the_commit_once(tmp_path, upstream, upstream_commit):
    ingest_matches([1, 2, 3], tmp_path)

    assert upstream_commit.calls == 1
    assert upstream.fetched_commits == [COMMIT] * 3


# --- immutability and history ----------------------------------------------


def test_changed_upstream_content_is_stored_as_a_new_version(tmp_path, upstream, upstream_commit):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)
    original = upstream.content

    upstream.content = events_bytes(1, label="corrected-")
    upstream_commit.value = OTHER_COMMIT
    result = ingest_match(3895158, tmp_path, storage=storage, refresh=True)

    # The old bytes are still there, byte for byte, under their own key.
    assert storage.objects[key_for(original)] == original
    assert storage.objects[key_for(upstream.content)] == upstream.content

    current = manifest(storage)
    assert current["sha256"] == calculate_sha256(upstream.content)
    assert current["s3_key"] == key_for(upstream.content)
    assert current["source_commit"] == OTHER_COMMIT
    assert result["s3_uploaded"] is True

    assert len(current["history"]) == 1
    superseded = current["history"][0]
    assert superseded["sha256"] == calculate_sha256(original)
    assert superseded["s3_key"] == key_for(original)
    assert superseded["source_commit"] == COMMIT


def test_event_object_is_never_written_twice_for_the_same_content(tmp_path, upstream):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)

    ingest_match(3895158, tmp_path, storage=storage, refresh=True)

    assert storage.uploads.count(key_for(upstream.content)) == 1


def test_refresh_with_identical_content_updates_provenance_but_not_history(
    tmp_path, upstream, upstream_commit, monkeypatch
):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)

    upstream_commit.value = OTHER_COMMIT
    ingest_match(3895158, tmp_path, storage=storage, refresh=True)

    current = manifest(storage)
    assert current["source_commit"] == OTHER_COMMIT
    assert current["history"] == []


def test_without_refresh_a_changed_upstream_is_not_even_requested(tmp_path, upstream):
    storage = FakeStorage()
    ingest_match(3895158, tmp_path, storage=storage)
    upstream.content = events_bytes(1, label="corrected-")

    ingest_match(3895158, tmp_path, storage=storage)

    assert upstream.fetched_commits == [COMMIT]
    assert manifest(storage)["sha256"] == calculate_sha256(events_bytes(1))


def test_history_accumulates_across_versions(tmp_path, upstream, upstream_commit):
    storage = FakeStorage()
    versions = [events_bytes(1, label=f"v{n}-") for n in range(3)]

    for content in versions:
        upstream.content = content
        ingest_match(3895158, tmp_path, storage=storage, refresh=True)

    current = manifest(storage)
    assert [v["sha256"] for v in current["history"]] == [
        calculate_sha256(versions[0]),
        calculate_sha256(versions[1]),
    ]
    assert all(key_for(v) in storage.objects for v in versions)


# --- migration of data ingested before commits were tracked ----------------


def test_legacy_object_is_copied_to_a_versioned_key_and_left_in_place(tmp_path, upstream):
    content = events_bytes(2)
    legacy_key = "statsbomb/events/3895158.json"
    legacy_entry = {
        "match_id": 3895158,
        "source": "StatsBomb Open Data",
        "source_url": "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/3895158.json",
        "file_path": str(tmp_path / "3895158.json"),
        "event_count": 2,
        "sha256": calculate_sha256(content),
        "retrieved_at": "T0",
        "retrieved_at_basis": "download",
        "s3_key": legacy_key,
        "s3_uri": f"s3://football-raw/{legacy_key}",
        "s3_uploaded_at": None,
    }
    (tmp_path / "3895158.json").write_bytes(content)
    storage = FakeStorage()
    storage.objects[legacy_key] = content
    storage.objects[MANIFEST_KEY] = json.dumps(legacy_entry).encode()

    result = ingest_match(3895158, tmp_path, storage=storage)

    assert result["skipped"] is True
    assert upstream.fetched_commits == []
    assert storage.objects[legacy_key] == content  # never deleted
    assert storage.objects[key_for(content)] == content

    migrated = manifest(storage)
    assert migrated["s3_key"] == key_for(content)
    assert migrated["source_commit"] is None  # unknown, not invented
    assert migrated["source_url"] == legacy_entry["source_url"]
    assert migrated["retrieved_at"] == "T0"
    assert migrated["history"] == []


# --- manifest validation ----------------------------------------------------


def entry(**overrides):
    return json.dumps({"match_id": 1, "sha256": "0" * 64, **overrides}).encode()


def test_manifest_accepts_entries_without_the_new_fields():
    assert parse_entry(entry(), 1)["match_id"] == 1


@pytest.mark.parametrize("commit", ["master", "abc", 5])
def test_manifest_rejects_an_invalid_source_commit(commit):
    with pytest.raises(ValueError, match="source_commit"):
        parse_entry(entry(source_commit=commit), 1)


@pytest.mark.parametrize("history", ["x", [1], [{"sha256": "short"}], {"a": 1}])
def test_manifest_rejects_an_invalid_history(history):
    with pytest.raises(ValueError, match="history"):
        parse_entry(entry(history=history), 1)


# --- run records ------------------------------------------------------------


def stored_runs(storage):
    return {k: json.loads(v) for k, v in storage.objects.items() if "/runs/" in k}


def test_successful_run_writes_a_run_record(tmp_path, upstream):
    storage = FakeStorage()

    ingest_matches([1, 2], tmp_path, storage=storage, run_id="run-1")

    record = stored_runs(storage)["statsbomb/runs/run-1.json"]
    assert record["status"] == "succeeded"
    assert record["source_commit"] == COMMIT
    assert [m["match_id"] for m in record["matches"]] == [1, 2]
    assert {m["status"] for m in record["matches"]} == {"downloaded"}


def test_fully_cached_run_records_no_source_commit(tmp_path, upstream):
    storage = FakeStorage()
    ingest_matches([1], tmp_path, storage=storage, run_id="run-1")

    ingest_matches([1], tmp_path, storage=storage, run_id="run-2")

    record = stored_runs(storage)["statsbomb/runs/run-2.json"]
    assert record["source_commit"] is None
    assert record["matches"][0]["status"] == "cached"


def test_failed_run_is_recorded_and_the_error_is_still_raised(tmp_path, monkeypatch):
    def fetch(match_id, commit):
        if match_id == 2:
            raise RuntimeError("source unavailable")
        return events_bytes(1)

    monkeypatch.setattr(FETCH, fetch)
    storage = FakeStorage()

    with pytest.raises(RuntimeError, match="source unavailable"):
        ingest_matches([1, 2, 3], tmp_path, storage=storage, run_id="run-1")

    record = stored_runs(storage)["statsbomb/runs/run-1.json"]
    assert record["status"] == "failed"
    assert record["failed_match_id"] == 2
    assert "source unavailable" in record["error"]
    assert [m["match_id"] for m in record["matches"]] == [1]  # match 3 never started


def test_run_record_failure_does_not_mask_the_original_error(tmp_path, monkeypatch):
    def fetch(match_id, commit):
        raise RuntimeError("source unavailable")

    class RecordsBroken(FakeStorage):
        def upload_bytes(self, key, content):
            if "/runs/" in key:
                raise OSError("storage down")
            super().upload_bytes(key, content)

    monkeypatch.setattr(FETCH, fetch)

    with pytest.raises(RuntimeError, match="source unavailable") as raised:
        ingest_matches([1], tmp_path, storage=RecordsBroken(), run_id="run-1")

    assert any("storage down" in note for note in raised.value.__notes__)


def test_no_run_record_without_storage(tmp_path, upstream):
    results = ingest_matches([1], tmp_path)

    assert len(results) == 1


def test_unusable_run_id_is_rejected_before_any_work(tmp_path, upstream):
    with pytest.raises(ValueError, match="run_id"):
        ingest_matches([1], tmp_path, run_id="../escape")

    assert upstream.fetched_commits == []
    assert not (tmp_path / "1.json").exists()
