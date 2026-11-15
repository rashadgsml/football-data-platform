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

from football_platform.source import is_commit_sha


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


def entry_unit(entry: dict) -> str | None:
    """Identify what an entry describes within its dataset.

    New datasets store an explicit ``unit`` (for example ``"9/281"`` for the
    matches of one competition season). Event entries predate that field and are
    identified by ``match_id``; they stay valid unchanged.
    """

    if "unit" in entry:
        return str(entry["unit"])

    if "match_id" in entry:
        return str(entry["match_id"])

    return None


def find_entry(manifest: list[dict], unit: int | str) -> dict | None:
    """Return the entry for a unit (a match id for events), or None."""

    return next(
        (entry for entry in manifest if entry_unit(entry) == str(unit)),
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
            if entry_unit(existing) != entry_unit(entry)
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


def parse_entry(content: bytes, unit: int | str, label: str | None = None) -> dict:
    """Parse and validate a per-object manifest stored in object storage.

    ``label`` names the object in error messages; it defaults to a match.
    """

    label = label or f"match {unit}"

    try:
        entry = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"Manifest for {label} contains invalid JSON.") from error

    if not isinstance(entry, dict):
        raise TypeError(f"Manifest for {label} must be a JSON object.")

    if entry_unit(entry) != str(unit):
        raise ValueError(
            f"Manifest for {label} describes a different object: {entry_unit(entry)!r}."
        )

    sha256 = entry.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise ValueError(f"Manifest for {label} has an invalid sha256.")

    # Optional: entries written before commit tracking lack these fields.
    source_commit = entry.get("source_commit")
    if source_commit is not None and not is_commit_sha(source_commit):
        raise ValueError(f"Manifest for {label} has an invalid source_commit.")

    history = entry.get("history", [])
    if not isinstance(history, list) or not all(
        isinstance(version, dict)
        and isinstance(version.get("sha256"), str)
        and len(version["sha256"]) == 64
        for version in history
    ):
        raise ValueError(f"Manifest for {label} has an invalid history.")

    return entry
