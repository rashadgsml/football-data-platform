import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from football_platform.checksums import calculate_sha256
from football_platform.manifest import load_manifest, save_manifest

BASE_URL = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data/events"
)


def fetch_match_events(match_id: int) -> bytes:
    """Download the original event JSON for a StatsBomb match."""

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    url = f"{BASE_URL}/{match_id}.json"

    try:
        with urlopen(url, timeout=30) as response:
            content = response.read()
    except (HTTPError, URLError) as error:
        raise RuntimeError(
            f"Could not download match {match_id}: {error}"
        ) from error

    return content


def save_raw_events(
    match_id: int,
    content: bytes,
    output_dir: Path,
) -> dict:
    """Validate and atomically save original match events."""

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    try:
        events = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Downloaded content is not valid JSON.") from error

    if not isinstance(events, list):
        raise TypeError("Expected a JSON array of match events.")

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


def ingest_match(
    match_id: int,
    output_dir: Path,
    manifest_path: Path | None = None,
) -> dict:
    """Download or reuse match events and record ingestion metadata."""

    if match_id <= 0:
        raise ValueError("Match ID must be a positive integer.")

    if manifest_path is None:
        manifest_path = output_dir / "manifest.json"

    file_path = output_dir / f"{match_id}.json"
    skipped = False

    if file_path.exists():
        try:
            content = file_path.read_bytes()
            events = json.loads(content)

            if not isinstance(events, list):
                raise TypeError("Existing file is not a JSON array.")

            result = {
                "match_id": match_id,
                "file_path": str(file_path),
                "event_count": len(events),
                "sha256": calculate_sha256(content),
            }
            skipped = True

        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            content = fetch_match_events(match_id)
            result = save_raw_events(match_id, content, output_dir)
    else:
        content = fetch_match_events(match_id)
        result = save_raw_events(match_id, content, output_dir)

    manifest = load_manifest(manifest_path)
    existing_entry = next(
        (entry for entry in manifest if entry.get("match_id") == match_id),
        None,
    )

    if existing_entry is None or not skipped:
        manifest_entry = {
            **result,
            "source": "StatsBomb Open Data",
            "source_url": f"{BASE_URL}/{match_id}.json",
            "retrieved_at": datetime.now(UTC).isoformat(),
        }
        save_manifest(manifest_entry, manifest_path)

    result["skipped"] = skipped
    return result


def ingest_matches(
    match_ids: list[int],
    output_dir: Path,
    manifest_path: Path | None = None,
) -> list[dict]:
    """Ingest multiple matches sequentially."""

    if len(match_ids) != len(set(match_ids)):
        raise ValueError("Match IDs must be unique.")

    return [
        ingest_match(match_id, output_dir, manifest_path)
        for match_id in match_ids
    ]