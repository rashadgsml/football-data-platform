"""StatsBomb competitions: one file listing every (competition, season) pair.

``competitions.json`` is the root of the data hierarchy. Each row points to a
``matches/<competition_id>/<season_id>.json`` file, so a season can only be
ingested if it appears here.
"""

import json
from pathlib import Path

from football_platform import bronze
from football_platform.bronze import BronzeTarget
from football_platform.source import (
    UNPINNED_DATA_URL,
    SourceRef,
    competitions_url,
    get_with_retries,
)
from football_platform.storage import S3Storage

DATASET = "competitions"
UNIT = "all"
S3_PREFIX = "statsbomb/competitions"
S3_MANIFEST_KEY = "statsbomb/manifests/competitions/all.json"


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_competitions(content: bytes) -> list[dict]:
    """Validate competitions.json and return its rows.

    Each row needs the identifiers that link to matches and a human-readable
    name. Other fields (country, gender, update timestamps) are optional and
    tolerated, so additive upstream changes do not break ingestion.
    """

    try:
        rows = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Competitions content is not valid JSON.") from error

    if not isinstance(rows, list):
        raise TypeError("Expected a JSON array of competition seasons.")

    if not rows:
        raise ValueError("Competitions array is empty.")

    seen = set()

    for position, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"Competition row {position} is not a JSON object.")

        problems = []

        for name in ("competition_id", "season_id"):
            if not _is_int(row.get(name)):
                problems.append(f"{name} must be an integer")

        for name in ("competition_name", "season_name"):
            if not isinstance(row.get(name), str) or not row[name]:
                problems.append(f"{name} must be a non-empty string")

        if problems:
            raise ValueError(f"Competition row {position} is invalid: {'; '.join(problems)}.")

        pair = (row["competition_id"], row["season_id"])
        if pair in seen:
            raise ValueError(f"Duplicate competition season {pair} at row {position}.")
        seen.add(pair)

    return rows


def _summarise(content: bytes) -> dict:
    return {"record_count": len(parse_competitions(content))}


def fetch_competitions(commit: str) -> bytes:
    return get_with_retries(competitions_url(commit), "competitions")


def competitions_object_key(sha256: str) -> str:
    return f"{S3_PREFIX}/{sha256}.json"


def competitions_target(output_dir: Path, manifest_path: Path) -> BronzeTarget:
    return BronzeTarget(
        unit=UNIT,
        label="competitions",
        identity={"dataset": DATASET, "unit": UNIT},
        local_path=output_dir / "competitions.json",
        manifest_path=manifest_path,
        manifest_key=S3_MANIFEST_KEY,
        object_key=competitions_object_key,
        source_url=competitions_url,
        unpinned_url=f"{UNPINNED_DATA_URL}/competitions.json",
        fetch=lambda commit: fetch_competitions(commit),
        validate=_summarise,
    )


def ingest_competitions(
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
    source: SourceRef | None = None,
    refresh: bool = False,
) -> dict:
    """Ingest competitions.json locally and optionally sync it to S3."""

    if manifest_path is None:
        manifest_path = output_dir / "manifest.json"

    return bronze.ingest_object(
        competitions_target(output_dir, manifest_path),
        storage,
        source or SourceRef(),
        refresh,
    )


def load_competitions(path: Path) -> list[dict]:
    """Read and validate a stored competitions.json."""

    return parse_competitions(path.read_bytes())


def find_season(competitions: list[dict], competition_id: int, season_id: int) -> dict:
    """Return the competitions row for a season, or raise if it does not exist."""

    for row in competitions:
        if row["competition_id"] == competition_id and row["season_id"] == season_id:
            return row

    raise ValueError(
        f"Competition {competition_id} season {season_id} is not listed in competitions.json."
    )
