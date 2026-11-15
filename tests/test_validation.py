import json

import pytest
from factories import make_event, make_events

from football_platform.datasets.events import validate_events


def encode(value) -> bytes:
    return json.dumps(value).encode("utf-8")


def test_validate_events_accepts_valid_events():
    assert len(validate_events(encode(make_events(3)))) == 3


def test_validate_events_rejects_empty_array():
    with pytest.raises(ValueError, match="empty"):
        validate_events(b"[]")


@pytest.mark.parametrize("record", [1, "event", None, [1, 2], True])
def test_validate_events_rejects_non_object_records(record):
    with pytest.raises(TypeError, match="position 1 is not a JSON object"):
        validate_events(encode([make_event(), record]))


@pytest.mark.parametrize(
    "field", ["id", "index", "period", "timestamp", "minute", "second", "type", "possession", "team"]
)
def test_validate_events_rejects_missing_essential_field(field):
    event = make_event()
    del event[field]

    with pytest.raises(ValueError, match=rf"position 0 is invalid.*{field}"):
        validate_events(encode([event]))


@pytest.mark.parametrize(
    "overrides",
    [
        {"id": ""},
        {"id": 7},
        {"index": "1"},
        {"index": True},
        {"period": None},
        {"type": {"name": "Pass"}},
        {"type": {"id": 30}},
        {"team": "Bayer Leverkusen"},
        {"team": {"id": "904", "name": "x"}},
    ],
)
def test_validate_events_rejects_wrong_types(overrides):
    with pytest.raises(ValueError, match="invalid"):
        validate_events(encode([make_event(**overrides)]))


def test_validate_events_reports_position_of_first_bad_event():
    events = make_events(3)
    del events[2]["team"]

    with pytest.raises(ValueError, match="position 2"):
        validate_events(encode(events))


def test_validate_events_accepts_absent_and_null_optional_fields():
    """Team-level events have no player or location; other fields may be null."""

    events = [
        make_event(1),  # no player, location or duration at all
        make_event(2, player=None, location=None, duration=None),
        make_event(3, related_events=[], under_pressure=True),
    ]

    assert len(validate_events(encode(events))) == 3


def test_validate_events_still_rejects_non_array_and_invalid_json():
    with pytest.raises(TypeError, match="JSON array"):
        validate_events(b'{"event": "Pass"}')

    with pytest.raises(ValueError, match="valid JSON"):
        validate_events(b"not valid JSON")
