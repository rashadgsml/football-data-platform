import json
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


def save_manifest(entry: dict, manifest_path: Path) -> None:
    """Insert or update an entry, avoiding duplicate match IDs."""

    manifest = load_manifest(manifest_path)

    manifest = [
        existing
        for existing in manifest
        if existing.get("match_id") != entry.get("match_id")
    ]

    manifest.append(entry)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = manifest_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(manifest_path)
