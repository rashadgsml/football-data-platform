import json
import logging

import pytest
from factories import (
    OTHER_COMMIT,
    FakeStorage,
    competitions_bytes,
    consistent_match,
    lineups_bytes,
    make_competition,
    make_lineup_team,
    make_match_events,
    matches_bytes,
)

from football_platform.datasets.events import ingest_match
from football_platform.pipeline import ingest_season
from football_platform.reconcile import ReconciliationError

RUN_KEY = "statsbomb/runs/run-1.json"


@pytest.fixture
def upstream(monkeypatch):
    """A small consistent season (matches 1-3) that records every download."""

    class Upstream:
        lineups = lineups_bytes()

    Upstream.matches = [
        consistent_match(1, 1, match_id=1),
        consistent_match(2, 0, match_id=2),
        consistent_match(1, 1, match_id=3),
    ]
    Upstream.events = {
        1: make_match_events(1, 1),
        2: make_match_events(2, 0),
        3: make_match_events(1, 1),
    }
    Upstream.calls = []

    def competitions(commit):
        Upstream.calls.append("competitions")
        return competitions_bytes(make_competition(9, 281))

    def matches(competition_id, season_id, commit):
        Upstream.calls.append("matches")
        return matches_bytes(*Upstream.matches)

    def lineups(match_id, commit):
        Upstream.calls.append(f"lineups:{match_id}")
        return Upstream.lineups

    def events(match_id, commit):
        Upstream.calls.append(f"events:{match_id}")
        return json.dumps(Upstream.events[match_id]).encode()

    base = "football_platform.datasets"
    monkeypatch.setattr(f"{base}.competitions.fetch_competitions", competitions)
    monkeypatch.setattr(f"{base}.matches.fetch_matches", matches)
    monkeypatch.setattr(f"{base}.lineups.fetch_lineups", lineups)
    monkeypatch.setattr(f"{base}.events.fetch_match_events", events)
    return Upstream


def run(tmp_path, storage=None, **kwargs):
    kwargs.setdefault("run_id", "run-1")
    return ingest_season(9, 281, tmp_path, storage=storage, **kwargs)


def stored_record(storage, run_id="run-1"):
    return json.loads(storage.objects[f"statsbomb/runs/{run_id}.json"])


# --- the happy path ---------------------------------------------------------


def test_run_ingests_every_dataset_in_dependency_order(tmp_path, upstream, upstream_commit):
    storage = FakeStorage()

    record = run(tmp_path, storage)

    assert upstream.calls == [
        "competitions", "matches",
        "lineups:1", "events:1", "lineups:2", "events:2", "lineups:3", "events:3",
    ]
    assert upstream_commit.calls == 1  # one commit for the whole run
    assert record["status"] == "succeeded"
    assert record["scope"] == {"competition_id": 9, "season_id": 281}
    assert set(record["datasets"]) == {"competitions", "matches"}
    assert [m["match_id"] for m in record["matches"]] == [1, 2, 3]

    for name in ("competitions/competitions", "matches/9_281", "lineups/2", "events/3"):
        assert (tmp_path / f"{name}.json").exists()


def test_every_stored_object_has_a_versioned_key_and_a_manifest(tmp_path, upstream):
    storage = FakeStorage()

    run(tmp_path, storage)

    prefixes = ("competitions/", "matches/9/281/", "lineups/1/", "events/1/")
    for prefix in prefixes:
        assert any(k.startswith(f"statsbomb/{prefix}") for k in storage.objects), prefix
    assert "statsbomb/manifests/events/3.json" in storage.objects
    assert "statsbomb/manifests/lineups/3.json" in storage.objects
    assert "statsbomb/manifests/matches/9/281.json" in storage.objects


def test_run_record_is_returned_and_stored_identically(tmp_path, upstream):
    storage = FakeStorage()

    record = run(tmp_path, storage)

    assert stored_record(storage) == record
    assert record["schema_version"] == 2
    first = record["matches"][0]
    assert first["lineups"]["status"] == "downloaded"
    assert first["events"]["records"] == len(upstream.events[1])
    assert first["stale"] is False


def test_rerun_downloads_and_uploads_nothing_but_its_own_record(tmp_path, upstream, upstream_commit):
    storage = FakeStorage()
    run(tmp_path, storage)
    uploads, calls = list(storage.uploads), len(upstream.calls)

    record = run(tmp_path, storage, run_id="run-2")

    assert len(upstream.calls) == calls
    assert upstream_commit.calls == 1  # all cached: the API was not called again
    assert storage.uploads == [*uploads, "statsbomb/runs/run-2.json"]
    assert record["source_commit"] is None
    assert {m["events"]["status"] for m in record["matches"]} == {"cached"}


def test_run_without_s3_works_locally(tmp_path, upstream):
    record = run(tmp_path)

    assert record["status"] == "succeeded"
    assert (tmp_path / "events" / "1.json").exists()


# --- selection and skipping ------------------------------------------------


def test_unavailable_matches_are_skipped_and_reported(tmp_path, upstream):
    upstream.matches[1] = consistent_match(2, 0, match_id=2, match_status="scheduled")

    record = run(tmp_path, FakeStorage())

    assert "lineups:2" not in upstream.calls and "events:2" not in upstream.calls
    assert [m["match_id"] for m in record["matches"]] == [1, 3]
    assert record["skipped_matches"] == [{"match_id": 2, "reason": "match_status=scheduled"}]


def test_match_ids_select_a_subset_in_season_order(tmp_path, upstream):
    record = run(tmp_path, match_ids=[3, 1])

    assert [m["match_id"] for m in record["matches"]] == [1, 3]
    assert "lineups:2" not in upstream.calls


def test_limit_caps_the_number_of_matches(tmp_path, upstream):
    assert [m["match_id"] for m in run(tmp_path, limit=2)["matches"]] == [1, 2]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [({"match_ids": [1, 1]}, "unique"), ({"match_ids": [99]}, "not in competition 9 season 281")],
)
def test_invalid_match_selection_fails_the_run_before_any_match_is_ingested(
    tmp_path, upstream, kwargs, message
):
    storage = FakeStorage()

    with pytest.raises(ValueError, match=message):
        run(tmp_path, storage, **kwargs)

    assert not any(call.startswith(("lineups", "events")) for call in upstream.calls)
    assert stored_record(storage)["status"] == "failed"


def test_invalid_arguments_are_rejected_before_any_work(tmp_path, upstream):
    with pytest.raises(ValueError, match="limit"):
        run(tmp_path, limit=0)
    with pytest.raises(ValueError, match="run_id"):
        run(tmp_path, run_id="../escape")

    assert upstream.calls == []


# --- failures ---------------------------------------------------------------


def test_season_not_in_competitions_fails_before_matches_are_downloaded(tmp_path, upstream):
    storage = FakeStorage()

    with pytest.raises(ValueError, match="Competition 9 season 1 is not listed"):
        ingest_season(9, 1, tmp_path, storage=storage, run_id="run-1")

    assert upstream.calls == ["competitions"]
    failure = stored_record(storage)["failure"]
    assert (failure["dataset"], failure["unit"]) == ("matches", "9/1")


def test_events_contradicting_the_match_stop_the_run_and_are_not_stored(tmp_path, upstream):
    upstream.events[2] = make_match_events(5, 0)  # match 2 ended 2-0
    storage = FakeStorage()

    with pytest.raises(ReconciliationError, match="goals in events 5-0"):
        run(tmp_path, storage)

    record = stored_record(storage)
    assert record["status"] == "failed"
    assert record["failure"]["dataset"] == "events" and record["failure"]["unit"] == "2"
    assert [m["match_id"] for m in record["matches"]] == [1]  # only the finished match
    assert not any(k.startswith("statsbomb/events/2/") for k in storage.objects)
    assert "lineups:3" not in upstream.calls  # nothing after the failure


def test_lineups_for_the_wrong_teams_stop_the_run_at_lineups(tmp_path, upstream):
    upstream.lineups = lineups_bytes([make_lineup_team(1, [1, 2]), make_lineup_team(2, [3, 4])])
    storage = FakeStorage()

    with pytest.raises(ReconciliationError, match="lineup teams"):
        run(tmp_path, storage)

    failure = stored_record(storage)["failure"]
    assert (failure["dataset"], failure["unit"]) == ("lineups", "1")
    assert "events:1" not in upstream.calls


def test_failure_to_write_the_run_record_does_not_mask_the_real_error(tmp_path, upstream):
    class RecordsBroken(FakeStorage):
        def upload_bytes(self, key, content):
            if "/runs/" in key:
                raise OSError("storage down")
            super().upload_bytes(key, content)

    upstream.events[1] = make_match_events(9, 9)

    with pytest.raises(ReconciliationError) as raised:
        run(tmp_path, RecordsBroken())

    assert any("storage down" in note for note in raised.value.__notes__)


# --- stale detection --------------------------------------------------------


def test_matches_changed_upstream_since_download_are_reported_stale(tmp_path, upstream):
    storage = FakeStorage()
    run(tmp_path, storage)

    # Upstream metadata moves on; losing the local match list forces a fresh copy.
    upstream.matches[1] = consistent_match(2, 0, match_id=2, last_updated="2030-01-01T00:00:00")
    (tmp_path / "matches" / "9_281.json").write_text("garbage")
    record = run(tmp_path, storage, run_id="run-2")

    assert record["datasets"]["matches"]["status"] == "downloaded"
    assert record["stale_match_ids"] == [2]
    assert [m["stale"] for m in record["matches"]] == [False, True, False]
    # Reported only: the stale match's files were not downloaded again.
    assert record["matches"][1]["events"]["status"] == "cached"


def test_refresh_clears_staleness(tmp_path, upstream):
    storage = FakeStorage()
    run(tmp_path, storage)
    upstream.matches[1] = consistent_match(2, 0, match_id=2, last_updated="2030-01-01T00:00:00")
    (tmp_path / "matches" / "9_281.json").write_text("garbage")
    run(tmp_path, storage, run_id="run-2")

    record = run(tmp_path, storage, run_id="run-3", refresh=True)

    assert record["stale_match_ids"] == []
    manifest = json.loads(storage.objects["statsbomb/manifests/events/2.json"])
    assert manifest["match_last_updated"] == "2030-01-01T00:00:00"


def test_events_ingested_before_tracking_are_not_reported_stale(tmp_path, upstream):
    ingest_match(1, tmp_path / "events", source=None)  # no match record: nothing tracked

    record = run(tmp_path)

    assert record["matches"][0]["events"]["status"] == "cached"
    assert record["matches"][0]["events"]["match_last_updated"] is None
    assert record["matches"][0]["stale"] is False
    entry = json.loads((tmp_path / "events" / "manifest.json").read_text())
    assert "match_last_updated" not in next(e for e in entry if e["match_id"] == 1)


def test_refresh_downloads_everything_again_from_the_current_commit(
    tmp_path, upstream, upstream_commit
):
    storage = FakeStorage()
    run(tmp_path, storage)
    event_uploads = [k for k in storage.uploads if k.startswith("statsbomb/events/")]
    upstream.calls.clear()
    upstream_commit.value = OTHER_COMMIT

    record = run(tmp_path, storage, run_id="run-2", refresh=True)

    assert len(upstream.calls) == 8
    assert record["source_commit"] == OTHER_COMMIT
    assert {m["events"]["status"] for m in record["matches"]} == {"downloaded"}
    # Identical content: same keys, so no event object is written twice.
    assert [k for k in storage.uploads if k.startswith("statsbomb/events/")] == event_uploads


# --- logging ----------------------------------------------------------------


def test_run_is_logged_without_leaking_connection_details(tmp_path, upstream, caplog):
    upstream.matches[1] = consistent_match(2, 0, match_id=2, match_status="scheduled")

    with caplog.at_level(logging.INFO):
        run(tmp_path, FakeStorage())

    text = caplog.text
    assert "Run run-1: ingesting competition 9 season 281" in text
    assert "lineups 1: downloaded, uploaded to S3" in text
    assert "Match 2 skipped" in text
    assert "Run run-1 succeeded: 2 matches, 1 skipped, 0 stale" in text
    assert "http" not in text.lower()  # no endpoints or URLs in logs


def test_failure_is_logged_as_an_error(tmp_path, upstream, caplog):
    upstream.events[1] = make_match_events(9, 9)

    with caplog.at_level(logging.INFO), pytest.raises(ReconciliationError):
        run(tmp_path, FakeStorage())

    assert any(r.levelno == logging.ERROR and "failed in events 1" in r.message for r in caplog.records)
