import json

import pytest

from football_platform.runs import (
    build_run_record,
    new_run_id,
    outcome,
    run_record_key,
    serialize_run_record,
)


def result(match_id=1, skipped=False):
    return {
        "match_id": match_id,
        "skipped": skipped,
        "event_count": 3,
        "sha256": "0" * 64,
        "s3_key": f"statsbomb/events/{match_id}/{'0' * 64}.json",
        "s3_uploaded": not skipped,
    }


def test_run_ids_are_unique_and_valid_key_components():
    first, second = new_run_id(), new_run_id()

    assert first != second
    assert run_record_key(first) == f"statsbomb/runs/{first}.json"


@pytest.mark.parametrize("bad", ["", "../escape", "a/b", "has space"])
def test_run_record_key_rejects_unsafe_run_ids(bad):
    with pytest.raises(ValueError, match="run_id"):
        run_record_key(bad)


def test_outcome_summarises_one_stored_object():
    assert outcome(result(7))["status"] == "downloaded"
    assert outcome(result(7, skipped=True))["status"] == "cached"
    assert outcome(result())["records"] == 3
    assert outcome({**result(), "extra": {"match_last_updated": "T"}})["match_last_updated"] == "T"


def test_outcome_uses_record_count_for_non_event_datasets():
    summary = {**result(), "record_count": 80}
    del summary["event_count"]

    assert outcome(summary)["records"] == 80


def test_successful_record_has_a_schema_version_and_all_sections():
    record = build_run_record(
        "r1", "T1", "T2", "succeeded", "a" * 40,
        scope={"competition_id": 9, "season_id": 281},
        datasets={"matches": outcome(result())},
        matches=[{"match_id": 1, "events": outcome(result(1))}],
        skipped_matches=[{"match_id": 2, "reason": "match_status=scheduled"}],
        stale_match_ids=[3],
    )

    assert record["schema_version"] == 2
    assert record["status"] == "succeeded"
    assert record["source_commit"] == "a" * 40
    assert record["scope"] == {"competition_id": 9, "season_id": 281}
    assert record["matches"][0]["events"]["status"] == "downloaded"
    assert record["skipped_matches"][0]["match_id"] == 2
    assert record["stale_match_ids"] == [3]
    assert "failure" not in record


def test_failed_record_names_the_failed_dataset_unit_and_error():
    failure = {"dataset": "events", "unit": "9", "error": "boom"}

    record = build_run_record("r1", "T1", "T2", "failed", None, failure=failure)

    assert record["status"] == "failed"
    assert record["failure"] == failure
    assert record["source_commit"] is None
    assert record["matches"] == [] and record["datasets"] == {}


def test_serialised_record_is_valid_json():
    record = build_run_record("r1", "T1", "T2", "succeeded", None)

    assert json.loads(serialize_run_record(record)) == record
