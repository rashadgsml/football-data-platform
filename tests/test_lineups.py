import json

import pytest
from factories import (
    COMMIT,
    FakeStorage,
    consistent_match,
    lineups_bytes,
    make_lineup_team,
    make_lineups,
)

from football_platform.checksums import calculate_sha256
from football_platform.datasets.lineups import ingest_lineups, parse_lineups
from football_platform.reconcile import ReconciliationError

FETCH = "football_platform.datasets.lineups.fetch_lineups"
MATCH = consistent_match()
MANIFEST_KEY = "statsbomb/manifests/lineups/3895158.json"


@pytest.fixture
def upstream(monkeypatch):
    class Upstream:
        content = lineups_bytes()
        downloads = 0

    def fetch(match_id, commit):
        Upstream.downloads += 1
        return Upstream.content

    monkeypatch.setattr(FETCH, fetch)
    return Upstream


def key_for(content: bytes) -> str:
    return f"statsbomb/lineups/3895158/{calculate_sha256(content)}.json"


# --- validation -------------------------------------------------------------


def test_valid_lineups_are_parsed():
    assert len(parse_lineups(lineups_bytes())) == 2


def test_optional_player_fields_are_tolerated():
    lineups = make_lineups()
    lineups[0]["lineup"][0].update(cards=[], positions=[], jersey_number=9, extra="x")

    assert parse_lineups(json.dumps(lineups).encode())[0]["lineup"][0]["jersey_number"] == 9


@pytest.mark.parametrize(
    ("content", "error", "message"),
    [
        (b"nope", ValueError, "not valid JSON"),
        (b'{"a": 1}', TypeError, "JSON array"),
        (b"[]", ValueError, "exactly 2"),
        (json.dumps(make_lineups()[:1]).encode(), ValueError, "exactly 2"),
        (b"[1, 2]", TypeError, "not a JSON object"),
    ],
)
def test_malformed_lineups_are_rejected(content, error, message):
    with pytest.raises(error, match=message):
        parse_lineups(content)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda t: t.update(team_id="1"), "team_id"),
        (lambda t: t.update(team_name=""), "team_name"),
        (lambda t: t.update(lineup=[]), "non-empty list"),
        (lambda t: t.update(lineup=None), "non-empty list"),
        (lambda t: t["lineup"][0].update(player_id="1"), "player_id"),
        (lambda t: t["lineup"][0].pop("player_name"), "player_name"),
        (lambda t: t["lineup"].append(dict(t["lineup"][0])), "duplicate player_id"),
    ],
)
def test_invalid_team_lineups_are_rejected(mutate, message):
    lineups = make_lineups()
    mutate(lineups[0])

    with pytest.raises(ValueError, match=message):
        parse_lineups(lineups_bytes(lineups))


def test_two_teams_with_the_same_id_are_rejected():
    lineups = [make_lineup_team(5, [1, 2]), make_lineup_team(5, [3, 4])]

    with pytest.raises(ValueError, match="same team_id"):
        parse_lineups(lineups_bytes(lineups))


# --- ingestion --------------------------------------------------------------


def test_ingest_stores_lineups_with_a_manifest(tmp_path, upstream):
    storage = FakeStorage()

    result = ingest_lineups(MATCH, tmp_path, storage=storage)

    assert result["match_id"] == 3895158
    assert result["record_count"] == 24
    assert (tmp_path / "3895158.json").read_bytes() == upstream.content
    assert storage.objects[key_for(upstream.content)] == upstream.content

    entry = json.loads(storage.objects[MANIFEST_KEY])
    assert (entry["dataset"], entry["unit"], entry["match_id"]) == ("lineups", "3895158", 3895158)
    assert entry["source_commit"] == COMMIT


def test_lineups_for_the_wrong_teams_are_rejected_and_not_stored(tmp_path, upstream):
    other = [make_lineup_team(1, [1, 2]), make_lineup_team(2, [3, 4])]
    upstream.content = lineups_bytes(other)
    storage = FakeStorage()

    with pytest.raises(ReconciliationError, match="lineup teams"):
        ingest_lineups(MATCH, tmp_path / "out", storage=storage)

    assert storage.uploads == []
    assert not (tmp_path / "out").exists()


def test_rerun_is_idempotent(tmp_path, upstream):
    storage = FakeStorage()
    ingest_lineups(MATCH, tmp_path, storage=storage)
    uploads, manifest = list(storage.uploads), (tmp_path / "manifest.json").read_text()

    result = ingest_lineups(MATCH, tmp_path, storage=storage)

    assert upstream.downloads == 1 and result["skipped"] is True
    assert storage.uploads == uploads
    assert (tmp_path / "manifest.json").read_text() == manifest


# --- provenance recorded at download time ----------------------------------


def test_the_match_last_updated_seen_at_download_is_recorded(tmp_path, upstream):
    storage = FakeStorage()

    result = ingest_lineups(MATCH, tmp_path, storage=storage)

    assert json.loads(storage.objects[MANIFEST_KEY])["match_last_updated"] == MATCH["last_updated"]
    assert result["extra"] == {"match_last_updated": MATCH["last_updated"]}


def test_a_cached_run_keeps_the_value_from_download_time(tmp_path, upstream):
    storage = FakeStorage()
    ingest_lineups(MATCH, tmp_path, storage=storage)
    newer = {**MATCH, "last_updated": "2030-01-01T00:00:00"}

    result = ingest_lineups(newer, tmp_path, storage=storage)

    assert result["skipped"] is True
    assert result["extra"]["match_last_updated"] == MATCH["last_updated"]


def test_refresh_records_the_current_value(tmp_path, upstream):
    storage = FakeStorage()
    ingest_lineups(MATCH, tmp_path, storage=storage)
    newer = {**MATCH, "last_updated": "2030-01-01T00:00:00"}

    ingest_lineups(newer, tmp_path, storage=storage, refresh=True)

    assert json.loads(storage.objects[MANIFEST_KEY])["match_last_updated"] == "2030-01-01T00:00:00"


def test_entries_without_the_field_are_never_backfilled(tmp_path, upstream):
    ingest_lineups(MATCH, tmp_path)
    manifest_path = tmp_path / "manifest.json"
    entries = json.loads(manifest_path.read_text())
    del entries[0]["match_last_updated"]
    manifest_path.write_text(json.dumps(entries))

    result = ingest_lineups({**MATCH, "last_updated": "2030-01-01T00:00:00"}, tmp_path)

    assert result["extra"] == {"match_last_updated": None}
    assert "match_last_updated" not in json.loads(manifest_path.read_text())[0]
