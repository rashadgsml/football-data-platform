import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from football_platform.manifest import (
    find_entry,
    parse_entry,
    save_manifest,
    serialize_entry,
)


def test_save_manifest_creates_file(tmp_path):
    manifest_path = tmp_path / "data" / "manifest.json"

    entry = {
        "match_id": 3895158,
        "event_count": 3866,
        "sha256": "abc123",
    }

    save_manifest(entry, manifest_path)

    assert manifest_path.exists()

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert saved_manifest == [entry]


def test_save_manifest_appends_entries(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    first_entry = {"match_id": 3895158}
    second_entry = {"match_id": 3895159}

    save_manifest(first_entry, manifest_path)
    save_manifest(second_entry, manifest_path)

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert saved_manifest == [first_entry, second_entry]


def test_save_manifest_replaces_entry_for_same_match(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    save_manifest({"match_id": 1, "sha256": "old"}, manifest_path)
    save_manifest({"match_id": 1, "sha256": "new"}, manifest_path)

    assert json.loads(manifest_path.read_text()) == [{"match_id": 1, "sha256": "new"}]


def test_save_manifest_concurrent_writers_do_not_lose_entries(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    match_ids = range(1, 61)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(lambda i: save_manifest({"match_id": i}, manifest_path), match_ids))

    saved = json.loads(manifest_path.read_text())
    assert sorted(entry["match_id"] for entry in saved) == list(match_ids)


def test_save_manifest_leaves_no_temporary_files(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    save_manifest({"match_id": 1}, manifest_path)

    assert not list(tmp_path.glob("*.tmp"))


def test_save_manifest_does_not_replace_a_corrupted_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("invalid JSON")

    with pytest.raises(ValueError, match="invalid JSON"):
        save_manifest({"match_id": 1}, manifest_path)

    assert manifest_path.read_text() == "invalid JSON"
    assert not list(tmp_path.glob("*.tmp"))


def test_find_entry():
    manifest = [{"match_id": 1}, {"match_id": 2, "sha256": "x"}]

    assert find_entry(manifest, 2) == {"match_id": 2, "sha256": "x"}
    assert find_entry(manifest, 3) is None


def test_serialize_and_parse_entry_round_trip():
    entry = {"match_id": 7, "sha256": "a" * 64, "retrieved_at": "T1", "s3_uploaded_at": None}

    assert parse_entry(serialize_entry(entry), 7) == entry


@pytest.mark.parametrize(
    "content",
    [
        b"not json",
        b"\xff\xfe",
        b"[]",
        b'{"match_id": 8, "sha256": "' + b"a" * 64 + b'"}',
        b'{"match_id": 7}',
        b'{"match_id": 7, "sha256": "short"}',
        b'{"match_id": 7, "sha256": 5}',
    ],
)
def test_parse_entry_rejects_invalid_manifests(content):
    with pytest.raises((ValueError, TypeError), match="Manifest for match 7"):
        parse_entry(content, 7)