import json

import pytest
from factories import COMMIT, OTHER_COMMIT, FakeStorage, competitions_bytes, make_competition

from football_platform.checksums import calculate_sha256
from football_platform.datasets.competitions import (
    find_season,
    ingest_competitions,
    parse_competitions,
)

FETCH = "football_platform.datasets.competitions.fetch_competitions"
MANIFEST_KEY = "statsbomb/manifests/competitions/all.json"


@pytest.fixture
def upstream(monkeypatch):
    class Upstream:
        content = competitions_bytes()
        downloads = 0

    def fetch(commit):
        Upstream.downloads += 1
        return Upstream.content

    monkeypatch.setattr(FETCH, fetch)
    return Upstream


def key_for(content: bytes) -> str:
    return f"statsbomb/competitions/{calculate_sha256(content)}.json"


# --- validation -------------------------------------------------------------


def test_valid_competitions_are_parsed():
    rows = parse_competitions(competitions_bytes(make_competition(), make_competition(11, 90)))

    assert [(r["competition_id"], r["season_id"]) for r in rows] == [(9, 281), (11, 90)]


def test_unknown_and_missing_optional_fields_are_tolerated():
    row = {"competition_id": 1, "season_id": 2, "competition_name": "A", "season_name": "B", "new": 1}

    assert parse_competitions(json.dumps([row]).encode()) == [row]


@pytest.mark.parametrize(
    ("content", "error", "message"),
    [
        (b"nope", ValueError, "not valid JSON"),
        (b"\xff", ValueError, "not valid JSON"),
        (b'{"a": 1}', TypeError, "JSON array"),
        (b"[]", ValueError, "empty"),
        (b"[1]", TypeError, "not a JSON object"),
    ],
)
def test_malformed_competitions_are_rejected(content, error, message):
    with pytest.raises(error, match=message):
        parse_competitions(content)


@pytest.mark.parametrize(
    "override",
    [
        {"competition_id": "9"},
        {"season_id": True},
        {"season_id": None},
        {"competition_name": ""},
        {"season_name": 2023},
    ],
)
def test_invalid_rows_are_rejected(override):
    with pytest.raises(ValueError, match="Competition row 0 is invalid"):
        parse_competitions(competitions_bytes(make_competition(**override)))


def test_missing_required_field_is_rejected():
    row = make_competition()
    del row["season_id"]

    with pytest.raises(ValueError, match="season_id"):
        parse_competitions(competitions_bytes(row))


def test_duplicate_seasons_are_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        parse_competitions(competitions_bytes(make_competition(), make_competition()))


def test_find_season():
    rows = parse_competitions(competitions_bytes(make_competition()))

    assert find_season(rows, 9, 281)["season_name"] == "2023/2024"
    with pytest.raises(ValueError, match="Competition 9 season 1 is not listed"):
        find_season(rows, 9, 1)


# --- ingestion --------------------------------------------------------------


def test_ingest_stores_locally_and_in_s3_with_a_manifest(tmp_path, upstream):
    storage = FakeStorage()

    result = ingest_competitions(tmp_path, storage=storage)

    assert (tmp_path / "competitions.json").read_bytes() == upstream.content
    assert storage.objects[key_for(upstream.content)] == upstream.content
    assert result["record_count"] == 1
    assert result["s3_uploaded"] is True

    entry = json.loads(storage.objects[MANIFEST_KEY])
    assert entry["dataset"] == "competitions"
    assert entry["unit"] == "all"
    assert entry["source_commit"] == COMMIT
    assert entry["sha256"] == calculate_sha256(upstream.content)
    assert json.loads((tmp_path / "manifest.json").read_text())[0] == entry


def test_rerun_downloads_and_uploads_nothing(tmp_path, upstream):
    storage = FakeStorage()
    ingest_competitions(tmp_path, storage=storage)
    uploads = list(storage.uploads)
    manifest = (tmp_path / "manifest.json").read_text()

    result = ingest_competitions(tmp_path, storage=storage)

    assert upstream.downloads == 1
    assert result["skipped"] is True
    assert storage.uploads == uploads
    assert (tmp_path / "manifest.json").read_text() == manifest


def test_invalid_upstream_content_writes_nothing(tmp_path, upstream):
    upstream.content = b"[]"
    storage = FakeStorage()

    with pytest.raises(ValueError, match="empty"):
        ingest_competitions(tmp_path / "out", storage=storage)

    assert storage.uploads == []
    assert not (tmp_path / "out").exists()


def test_changed_upstream_is_a_new_version_and_history_keeps_the_old(
    tmp_path, upstream, upstream_commit
):
    storage = FakeStorage()
    ingest_competitions(tmp_path, storage=storage)
    original = upstream.content

    upstream.content = competitions_bytes(make_competition(), make_competition(11, 90))
    upstream_commit.value = OTHER_COMMIT
    ingest_competitions(tmp_path, storage=storage, refresh=True)

    assert storage.objects[key_for(original)] == original
    entry = json.loads(storage.objects[MANIFEST_KEY])
    assert entry["record_count"] == 2
    assert entry["source_commit"] == OTHER_COMMIT
    assert entry["history"][0]["sha256"] == calculate_sha256(original)
