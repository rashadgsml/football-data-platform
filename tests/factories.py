"""Shared test helpers: valid StatsBomb-shaped events and an in-memory S3."""

import json

# A syntactically valid upstream commit SHA used throughout the tests.
COMMIT = "a" * 40
OTHER_COMMIT = "b" * 40


def make_event(index: int = 1, **overrides) -> dict:
    """Return a minimal event that satisfies ingestion validation."""

    event = {
        "id": f"event-{index}",
        "index": index,
        "period": 1,
        "timestamp": "00:00:00.000",
        "minute": 0,
        "second": 0,
        "type": {"id": 30, "name": "Pass"},
        "possession": 1,
        "team": {"id": 904, "name": "Bayer Leverkusen"},
    }
    event.update(overrides)
    return event


def make_events(count: int = 1) -> list[dict]:
    return [make_event(index) for index in range(1, count + 1)]


def events_bytes(count: int = 1, label: str = "") -> bytes:
    """Serialised events. A different ``label`` gives different content."""

    events = [make_event(index, id=f"event-{label}{index}") for index in range(1, count + 1)]
    return json.dumps(events).encode("utf-8")


class FakeStorage:
    """In-memory stand-in for S3Storage that records what was uploaded."""

    def __init__(self, bucket: str = "football-raw", corrupt_on_upload: bool = False):
        self.bucket = bucket
        self.corrupt_on_upload = corrupt_on_upload
        self.objects: dict[str, bytes] = {}
        self.uploads: list[str] = []
        self.create_bucket_calls = 0

    def create_bucket(self) -> None:
        self.create_bucket_calls += 1

    def upload_bytes(self, key: str, content: bytes) -> None:
        self.uploads.append(key)
        self.objects[key] = content + b" " if self.corrupt_on_upload else content

    def download_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def download_bytes_if_exists(self, key: str) -> bytes | None:
        return self.objects.get(key)
