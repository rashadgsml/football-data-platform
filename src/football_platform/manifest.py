"""Ingestion manifests.

Two forms exist:

* One JSON object per match, stored in S3 (the source of truth). Each match
  has its own object, so concurrent runs for different matches never touch
  the same file.
* A local ``manifest.json`` array, kept as a development convenience. Its
  read-modify-write cycle is serialised with an exclusive file lock.
"""

import fcntl
import json
import os
import tempfile
from pathlib import Path


def load_manifest(manifest_path: Path) -> list[dict]:
    """Load and validate the ingestion manifest."""

    if not manifest_path.exists():
        return []

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Manifest contains invalid JSON.") from error

    if not isinstance(manifest, list):
        raise TypeError("Manifest must contain a JSON array.")

    if not all(isinstance(entry, dict) for entry in manifest):
        raise ValueError("Every manifest entry must be a JSON object.")

    return manifest


def find_entry(manifest: list[dict], match_id: int) -> dict | None:
    """Return the entry for a match, or None."""

    return next(
        (entry for entry in manifest if entry.get("match_id") == match_id),
        None,
    )


def save_manifest(entry: dict, manifest_path: Path) -> None:
    """Insert or update an entry in the local manifest, avoiding duplicates.

    The whole read-modify-write cycle holds an exclusive lock, and the file
    is replaced atomically through a uniquely named temporary file, so
    concurrent writers on the same machine cannot lose each other's updates.
    """

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = manifest_path.with_name(manifest_path.name + ".lock")

    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        manifest = [
            existing
            for existing in load_manifest(manifest_path)
            if existing.get("match_id") != entry.get("match_id")
        ]
        manifest.append(entry)

        descriptor, temporary_name = tempfile.mkstemp(
            dir=manifest_path.parent,
            prefix=manifest_path.name + ".",
            suffix=".tmp",
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
                temporary.write(json.dumps(manifest, indent=2))
            os.replace(temporary_name, manifest_path)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise


def serialize_entry(entry: dict) -> bytes:
    """Serialise one per-match manifest entry for object storage."""

    return json.dumps(entry, indent=2, sort_keys=True).encode("utf-8")


def parse_entry(content: bytes, match_id: int) -> dict:
    """Parse and validate a per-match manifest object."""

    try:
        entry = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(
            f"Manifest for match {match_id} contains invalid JSON."
        ) from error

    if not isinstance(entry, dict):
        raise TypeError(f"Manifest for match {match_id} must be a JSON object.")

    if entry.get("match_id") != match_id:
        raise ValueError(
            f"Manifest for match {match_id} has a different match_id: "
            f"{entry.get('match_id')!r}."
        )

    sha256 = entry.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise ValueError(f"Manifest for match {match_id} has an invalid sha256.")

    return entry
