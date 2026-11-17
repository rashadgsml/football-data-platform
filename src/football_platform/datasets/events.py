"""StatsBomb match events: one JSON array per match.

Everything event-specific lives here: where the file is, how it is
downloaded, and what makes it valid. The shared mechanics are in ``bronze``.
"""

import json
import time
from collections.abc import Callable
from pathlib import Path

from football_platform import bronze
from football_platform.bronze import BronzeTarget
from football_platform.reconcile import (
    check_events_against_lineups,
    check_events_against_match,
    require_consistent,
)
from football_platform.source import (
    MAX_FETCH_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
    UNPINNED_BASE_URL,
    SourceRef,
    events_url,
    get_with_retries,
)
from football_platform.storage import S3Storage

S3_PREFIX = "statsbomb/events"
S3_MANIFEST_PREFIX = "statsbomb/manifests/events"


def event_object_key(match_id: int, sha256: str) -> str:
    """Write-once S3 key for one version of a match's events.

    The key contains the content hash, so different bytes can never occupy the
    same key: a changed source file becomes a new object and the old version
    stays available.
    """

    return f"{S3_PREFIX}/{match_id}/{sha256}.json"


def fetch_match_events(
    match_id: int,
    commit: str,
    max_attempts: int = MAX_FETCH_ATTEMPTS,
    backoff_seconds: float = RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Download the original event JSON for a match at a pinned upstream commit.

    Retry behaviour is described in ``source.get_with_retries``.
    """

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    return get_with_retries(
        events_url(commit, match_id),
        f"match {match_id}",
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        sleep=sleep,
    )


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_event(event: object, position: int) -> None:
    """Raise if an event is not an object or lacks the fields every event must have.

    Only identity, ordering and attribution fields are required. Optional
    fields (location, player, duration, event-specific details) may be absent
    or null and are deliberately not checked.
    """

    if not isinstance(event, dict):
        raise TypeError(f"Event at position {position} is not a JSON object.")

    problems = []

    if not isinstance(event.get("id"), str) or not event["id"]:
        problems.append("id must be a non-empty string")

    for name in ("index", "period", "minute", "second", "possession"):
        if not _is_int(event.get(name)):
            problems.append(f"{name} must be an integer")

    if not isinstance(event.get("timestamp"), str):
        problems.append("timestamp must be a string")

    for name in ("type", "team"):
        reference = event.get(name)
        if not (
            isinstance(reference, dict)
            and _is_int(reference.get("id"))
            and isinstance(reference.get("name"), str)
            and reference["name"]
        ):
            problems.append(f"{name} must be an object with an integer id and a name")

    if problems:
        raise ValueError(
            f"Event at position {position} is invalid: {'; '.join(problems)}."
        )


def validate_events(content: bytes) -> list:
    """Validate that content is a non-empty JSON array of valid events."""

    try:
        events = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Downloaded content is not valid JSON.") from error

    if not isinstance(events, list):
        raise TypeError("Expected a JSON array of match events.")

    if not events:
        raise ValueError("Event array is empty.")

    for position, event in enumerate(events):
        _check_event(event, position)

    return events


def _summarise(content: bytes) -> dict:
    return {"event_count": len(validate_events(content))}


def save_raw_events(match_id: int, content: bytes, output_dir: Path) -> dict:
    """Validate and atomically save original match events locally."""

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    result = bronze.save_raw(content, output_dir / f"{match_id}.json", _summarise)

    return {"match_id": match_id, **result}


def _reconciling_validator(
    match_id: int, match: dict | None, lineups: list[dict] | None
) -> Callable[[bytes], dict]:
    """Structural validation, plus cross-checks when reference data is given."""

    def validate(content: bytes) -> dict:
        events = validate_events(content)

        problems = []
        if match is not None:
            problems += check_events_against_match(events, match)
        if lineups is not None:
            problems += check_events_against_lineups(events, lineups)
        require_consistent(f"Events for match {match_id}", problems)

        return {"event_count": len(events)}

    return validate


def events_target(
    match_id: int,
    output_dir: Path,
    manifest_path: Path,
    match: dict | None = None,
    lineups: list[dict] | None = None,
) -> BronzeTarget:
    return BronzeTarget(
        unit=str(match_id),
        label=f"match {match_id}",
        identity={"match_id": match_id},
        local_path=output_dir / f"{match_id}.json",
        manifest_path=manifest_path,
        manifest_key=f"{S3_MANIFEST_PREFIX}/{match_id}.json",
        object_key=lambda sha256: event_object_key(match_id, sha256),
        source_url=lambda commit: events_url(commit, match_id),
        unpinned_url=f"{UNPINNED_BASE_URL}/{match_id}.json",
        # Looked up at call time so tests can replace fetch_match_events.
        fetch=lambda commit: fetch_match_events(match_id, commit),
        validate=_reconciling_validator(match_id, match, lineups),
        extra={"match_last_updated": match["last_updated"]} if match else {},
    )


def ingest_match(
    match_id: int,
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
    source: SourceRef | None = None,
    refresh: bool = False,
    match: dict | None = None,
    lineups: list[dict] | None = None,
) -> dict:
    """Ingest one match's events locally and optionally sync them to S3.

    If the match record (from the matches dataset) and/or the match's parsed
    lineups are given, the events must be consistent with them or nothing is
    stored; without them only structural validation applies.
    """

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    if manifest_path is None:
        manifest_path = output_dir / "manifest.json"

    result = bronze.ingest_object(
        events_target(match_id, output_dir, manifest_path, match, lineups),
        storage,
        source or SourceRef(),
        refresh,
    )

    return {"match_id": match_id, **result}
