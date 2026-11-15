import http.client
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from football_platform.checksums import calculate_sha256
from football_platform.manifest import (
    find_entry,
    load_manifest,
    parse_entry,
    save_manifest,
    serialize_entry,
)
from football_platform.storage import S3Storage

BASE_URL = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data/events"
)

S3_PREFIX = "statsbomb/events"
S3_MANIFEST_PREFIX = "statsbomb/manifests/events"

FETCH_TIMEOUT_SECONDS = 30
MAX_FETCH_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 1.0

# Network failures worth retrying. HTTPError is a URLError subclass and is
# handled separately so that only 5xx responses are retried.
TRANSIENT_ERRORS = (
    URLError,
    TimeoutError,
    ConnectionError,
    http.client.HTTPException,
)


def utc_now() -> str:
    """Return the current UTC time as an ISO 8601 string."""

    return datetime.now(UTC).isoformat()


def fetch_match_events(
    match_id: int,
    max_attempts: int = MAX_FETCH_ATTEMPTS,
    backoff_seconds: float = RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Download the original event JSON for a StatsBomb match.

    Transient failures (connection errors, timeouts, truncated reads and
    HTTP 5xx) are retried with exponential backoff, up to ``max_attempts``
    attempts in total. Other HTTP errors, such as 404, fail immediately.
    """

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    url = f"{BASE_URL}/{match_id}.json"
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as response:
                return response.read()
        except HTTPError as error:
            if error.code < 500:
                raise RuntimeError(
                    f"Could not download match {match_id}: {error}"
                ) from error
            last_error = error
        except TRANSIENT_ERRORS as error:
            last_error = error

        if attempt < max_attempts:
            sleep(backoff_seconds * 2 ** (attempt - 1))

    raise RuntimeError(
        f"Could not download match {match_id} "
        f"after {max_attempts} attempts: {last_error}"
    ) from last_error


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


def save_raw_events(
    match_id: int,
    content: bytes,
    output_dir: Path,
) -> dict:
    """Validate and atomically save original match events locally."""

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    events = validate_events(content)

    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"{match_id}.json"
    temporary_path = file_path.with_suffix(".tmp")

    try:
        temporary_path.write_bytes(content)
        temporary_path.replace(file_path)
    finally:
        temporary_path.unlink(missing_ok=True)

    return {
        "match_id": match_id,
        "file_path": str(file_path),
        "event_count": len(events),
        "sha256": calculate_sha256(content),
    }


def _load_cached_events(file_path: Path, previous: dict | None) -> dict | None:
    """Return a summary of a usable local file, or None if it must be replaced.

    A cached file is unusable if it cannot be read, fails validation, or does
    not match the checksum recorded when it was ingested.
    """

    try:
        content = file_path.read_bytes()
        events = validate_events(content)
    except (OSError, ValueError, TypeError):
        return None

    sha256 = calculate_sha256(content)
    recorded = previous.get("sha256") if previous else None

    if recorded and recorded != sha256:
        return None

    return {
        "content": content,
        "result": {
            "match_id": None,
            "file_path": str(file_path),
            "event_count": len(events),
            "sha256": sha256,
        },
    }


def _sync_event_object(
    storage: S3Storage,
    key: str,
    content: bytes,
    sha256: str,
) -> bool:
    """Upload content only if S3 differs, then verify. Return True if uploaded.

    Comparison and verification use SHA-256 of the object bytes, never ETags.
    """

    existing = storage.download_bytes_if_exists(key)

    if existing is not None and calculate_sha256(existing) == sha256:
        return False

    storage.upload_bytes(key, content)

    uploaded = storage.download_bytes(key)
    if calculate_sha256(uploaded) != sha256:
        raise RuntimeError(
            f"Upload verification failed for {key}: "
            "downloaded content does not match the SHA-256 of the source."
        )

    return True


def _build_entry(
    match_id: int,
    result: dict,
    previous: dict | None,
    downloaded: bool,
    uploaded: bool,
    now: str,
) -> dict:
    """Build the manifest entry, preserving timestamps when nothing happened.

    ``retrieved_at`` is when the content was last downloaded from the source.
    ``s3_uploaded_at`` is when it was last uploaded to S3. Neither changes
    merely because an existing file or object was checked.
    """

    if downloaded:
        retrieved_at, basis = now, "download"
    elif previous and previous.get("retrieved_at"):
        retrieved_at = previous["retrieved_at"]
        basis = previous.get("retrieved_at_basis", "download")
    else:
        # Local file adopted without a record: file mtime is the best evidence.
        modified = Path(result["file_path"]).stat().st_mtime
        retrieved_at = datetime.fromtimestamp(modified, UTC).isoformat()
        basis = "file_mtime"

    entry = {
        "match_id": match_id,
        "source": "StatsBomb Open Data",
        "source_url": f"{BASE_URL}/{match_id}.json",
        "file_path": result["file_path"],
        "event_count": result["event_count"],
        "sha256": result["sha256"],
        "retrieved_at": retrieved_at,
        "retrieved_at_basis": basis,
    }

    same_content = bool(previous) and previous.get("sha256") == result["sha256"]

    if "s3_key" in result:
        entry["s3_key"] = result["s3_key"]
        entry["s3_uri"] = result["s3_uri"]
        # None means the upload time is unknown (object predates this field).
        entry["s3_uploaded_at"] = (
            now
            if uploaded
            else previous.get("s3_uploaded_at") if same_content else None
        )
    elif same_content:
        # A run without S3 must not erase what an earlier run recorded.
        for name in ("s3_key", "s3_uri", "s3_uploaded_at"):
            if name in previous:
                entry[name] = previous[name]

    return entry


def ingest_match(
    match_id: int,
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
) -> dict:
    """Ingest one match locally and optionally sync it to S3.

    Existing manifests are read and validated before anything is written or
    uploaded. A valid local file is reused without downloading; if S3 is
    configured it is uploaded only when the S3 object differs, and the
    upload is verified by SHA-256.
    """

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    if manifest_path is None:
        manifest_path = output_dir / "manifest.json"

    now = utc_now()
    manifest_key = f"{S3_MANIFEST_PREFIX}/{match_id}.json"

    # Validate manifests first: nothing is written until both are readable.
    local_entry = find_entry(load_manifest(manifest_path), match_id)
    s3_entry = None

    if storage is not None:
        raw_manifest = storage.download_bytes_if_exists(manifest_key)
        if raw_manifest is not None:
            s3_entry = parse_entry(raw_manifest, match_id)

    previous = s3_entry or local_entry

    file_path = output_dir / f"{match_id}.json"
    cached = _load_cached_events(file_path, previous) if file_path.exists() else None

    if cached is not None:
        content = cached["content"]
        result = {**cached["result"], "match_id": match_id}
        downloaded = False
    else:
        content = fetch_match_events(match_id)
        result = save_raw_events(match_id, content, output_dir)
        downloaded = True

    uploaded = False

    if storage is not None:
        storage.create_bucket()

        object_key = f"{S3_PREFIX}/{match_id}.json"
        uploaded = _sync_event_object(
            storage, object_key, content, result["sha256"]
        )

        result["s3_key"] = object_key
        result["s3_uri"] = f"s3://{storage.bucket}/{object_key}"

    entry = _build_entry(match_id, result, previous, downloaded, uploaded, now)

    # Rewrite manifests only when their content actually changed.
    if storage is not None and entry != s3_entry:
        storage.upload_bytes(manifest_key, serialize_entry(entry))

    if entry != local_entry:
        save_manifest(entry, manifest_path)

    result["skipped"] = not downloaded
    result["s3_uploaded"] = uploaded
    return result


def ingest_matches(
    match_ids: list[int],
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
) -> list[dict]:
    """Ingest multiple matches sequentially."""

    if len(match_ids) != len(set(match_ids)):
        raise ValueError("Match IDs must be unique.")

    return [
        ingest_match(match_id, output_dir, manifest_path, storage)
        for match_id in match_ids
    ]
