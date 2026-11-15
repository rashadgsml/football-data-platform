import json

import pytest

from football_platform.runs import (
    build_run_record,
    new_run_id,
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


def test_successful_record_summarises_each_match():
    record = build_run_record(
        "r1", "T1", "T2", "succeeded", "a" * 40, [result(1), result(2, skipped=True)]
    )

    assert record["status"] == "succeeded"
    assert record["source_commit"] == "a" * 40
    assert [m["status"] for m in record["matches"]] == ["downloaded", "cached"]
    assert "error" not in record


def test_failed_record_names_the_failed_match_and_error():
    record = build_run_record(
        "r1", "T1", "T2", "failed", None, [], failed_match_id=9, error="boom"
    )

    assert record["status"] == "failed"
    assert record["failed_match_id"] == 9
    assert record["error"] == "boom"
    assert record["source_commit"] is None


def test_serialised_record_is_valid_json():
    record = build_run_record("r1", "T1", "T2", "succeeded", None, [])

    assert json.loads(serialize_run_record(record)) == record
