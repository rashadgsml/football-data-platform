import json

import pytest
from factories import (
    COMMIT,
    FakeStorage,
    competitions_bytes,
    make_competition,
    make_match,
    matches_bytes,
)

from football_platform.checksums import calculate_sha256
from football_platform.datasets.competitions import parse_competitions
from football_platform.datasets.matches import ingest_matches_for_season, parse_matches

FETCH = "football_platform.datasets.matches.fetch_matches"
MANIFEST_KEY = "statsbomb/manifests/matches/9/281.json"
COMPETITIONS = parse_competitions(competitions_bytes(make_competition()))


@pytest.fixture
def upstream(monkeypatch):
    class Upstream:
        content = matches_bytes(make_match(1), make_match(2))

    Upstream.calls = []

    def fetch(competition_id, season_id, commit):
        Upstream.calls.append((competition_id, season_id, commit))
        return Upstream.content

    monkeypatch.setattr(FETCH, fetch)
    return Upstream


def key_for(content: bytes) -> str:
    return f"statsbomb/matches/9/281/{calculate_sha256(content)}.json"


# --- validation -------------------------------------------------------------


def test_valid_matches_are_parsed():
    assert [m["match_id"] for m in parse_matches(matches_bytes(make_match(1), make_match(2)), 9, 281)] == [1, 2]


def test_optional_and_unknown_fields_are_tolerated():
    match = make_match(1, referee={"id": 1}, brand_new_field=[1, 2])

    assert parse_matches(matches_bytes(match), 9, 281) == [match]


@pytest.mark.parametrize(
    ("content", "error", "message"),
    [
        (b"nope", ValueError, "not valid JSON"),
        (b'{"a": 1}', TypeError, "JSON array"),
        (b"[]", ValueError, "empty"),
        (b"[1]", TypeError, "not a JSON object"),
    ],
)
def test_malformed_match_lists_are_rejected(content, error, message):
    with pytest.raises(error, match=message):
        parse_matches(content, 9, 281)


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"match_id": "1"}, "match_id"),
        ({"home_score": None}, "home_score"),
        ({"away_score": "2"}, "away_score"),
        ({"last_updated": ""}, "last_updated"),
        ({"match_date": None}, "match_date"),
        ({"home_team": {}}, "home_team"),
        ({"away_team": {"away_team_id": "x"}}, "away_team"),
        ({"competition": {"competition_id": 11}}, "competition.competition_id must be 9"),
        ({"season": {"season_id": 90}}, "season.season_id must be 281"),
        ({"season": None}, "season.season_id must be 281"),
    ],
)
def test_invalid_matches_are_rejected(override, message):
    with pytest.raises(ValueError, match=message):
        parse_matches(matches_bytes(make_match(**{"match_id": 1, **override})), 9, 281)


def test_match_with_the_same_team_on_both_sides_is_rejected():
    with pytest.raises(ValueError, match="same team"):
        parse_matches(matches_bytes(make_match(1, home_team_id=5, away_team_id=5)), 9, 281)


def test_duplicate_match_ids_are_rejected():
    with pytest.raises(ValueError, match="Duplicate match_id 1"):
        parse_matches(matches_bytes(make_match(1), make_match(1)), 9, 281)


# --- ingestion --------------------------------------------------------------


def test_ingest_stores_a_season_with_a_manifest(tmp_path, upstream):
    storage = FakeStorage()

    result = ingest_matches_for_season(9, 281, COMPETITIONS, tmp_path, storage=storage)

    assert upstream.calls == [(9, 281, COMMIT)]
    assert (tmp_path / "9_281.json").read_bytes() == upstream.content
    assert storage.objects[key_for(upstream.content)] == upstream.content
    assert result["record_count"] == 2

    entry = json.loads(storage.objects[MANIFEST_KEY])
    assert (entry["dataset"], entry["unit"]) == ("matches", "9/281")
    assert (entry["competition_id"], entry["season_id"]) == (9, 281)
    assert entry["source_commit"] == COMMIT
    assert "/matches/9/281.json" in entry["source_url"]


def test_season_missing_from_competitions_is_rejected_before_any_download(tmp_path, upstream):
    storage = FakeStorage()

    with pytest.raises(ValueError, match="not listed"):
        ingest_matches_for_season(9, 1, COMPETITIONS, tmp_path / "out", storage=storage)

    assert upstream.calls == []
    assert storage.uploads == [] and storage.create_bucket_calls == 0
    assert not (tmp_path / "out").exists()


def test_match_list_for_the_wrong_season_is_rejected_and_not_stored(tmp_path, upstream):
    upstream.content = matches_bytes(make_match(1, season_id=90))
    storage = FakeStorage()

    with pytest.raises(ValueError, match="season.season_id must be 281"):
        ingest_matches_for_season(9, 281, COMPETITIONS, tmp_path / "out", storage=storage)

    assert storage.uploads == []
    assert not (tmp_path / "out").exists()


def test_rerun_is_idempotent(tmp_path, upstream):
    storage = FakeStorage()
    ingest_matches_for_season(9, 281, COMPETITIONS, tmp_path, storage=storage)
    uploads = list(storage.uploads)
    manifest = (tmp_path / "manifest.json").read_text()

    result = ingest_matches_for_season(9, 281, COMPETITIONS, tmp_path, storage=storage)

    assert len(upstream.calls) == 1
    assert result["skipped"] is True
    assert storage.uploads == uploads
    assert (tmp_path / "manifest.json").read_text() == manifest


def test_seasons_have_independent_manifests_in_one_local_file(tmp_path, upstream):
    competitions = parse_competitions(competitions_bytes(make_competition(), make_competition(9, 27)))
    storage = FakeStorage()
    ingest_matches_for_season(9, 281, competitions, tmp_path, storage=storage)
    upstream.content = matches_bytes(make_match(7, season_id=27))

    ingest_matches_for_season(9, 27, competitions, tmp_path, storage=storage)

    local = json.loads((tmp_path / "manifest.json").read_text())
    assert sorted(e["unit"] for e in local) == ["9/27", "9/281"]
    assert "statsbomb/manifests/matches/9/27.json" in storage.objects
