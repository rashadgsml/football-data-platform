"""StatsBomb matches: one JSON array per (competition, season).

The match list is the driving table of the platform: it names every match in
a season, its teams and its final score. Event and lineup files are later
checked against it, and match ids come from here rather than a hardcoded list.
"""

import json
from pathlib import Path

from football_platform import bronze
from football_platform.bronze import BronzeTarget
from football_platform.datasets.competitions import find_season
from football_platform.source import (
    UNPINNED_DATA_URL,
    SourceRef,
    get_with_retries,
    matches_url,
)
from football_platform.storage import S3Storage

DATASET = "matches"
S3_PREFIX = "statsbomb/matches"
S3_MANIFEST_PREFIX = "statsbomb/manifests/matches"


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_match(match: object, position: int, competition_id: int, season_id: int) -> None:
    """Raise unless a match has the fields that later checks and joins depend on.

    Scores are required integers: a match without a final score cannot be
    reconciled against its events. Everything else (referee, stadium, stage,
    360 flags) is optional and not checked.
    """

    if not isinstance(match, dict):
        raise TypeError(f"Match at position {position} is not a JSON object.")

    problems = []

    if not _is_int(match.get("match_id")):
        problems.append("match_id must be an integer")

    for name in ("match_date", "match_status", "last_updated"):
        if not isinstance(match.get(name), str) or not match[name]:
            problems.append(f"{name} must be a non-empty string")

    for name in ("home_score", "away_score"):
        if not _is_int(match.get(name)):
            problems.append(f"{name} must be an integer")

    for side in ("home", "away"):
        team = match.get(f"{side}_team")
        if not (isinstance(team, dict) and _is_int(team.get(f"{side}_team_id"))):
            problems.append(f"{side}_team must be an object with an integer {side}_team_id")

    for name, expected in (("competition", competition_id), ("season", season_id)):
        reference = match.get(name)
        actual = reference.get(f"{name}_id") if isinstance(reference, dict) else None
        if actual != expected:
            problems.append(f"{name}.{name}_id must be {expected}, got {actual!r}")

    if problems:
        raise ValueError(f"Match at position {position} is invalid: {'; '.join(problems)}.")

    teams = {match["home_team"]["home_team_id"], match["away_team"]["away_team_id"]}
    if len(teams) != 2:
        raise ValueError(f"Match at position {position} has the same team as home and away.")


def parse_matches(content: bytes, competition_id: int, season_id: int) -> list[dict]:
    """Validate a season's match list and return its matches.

    Every match must belong to the requested competition season and appear
    only once.
    """

    try:
        matches = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Matches content is not valid JSON.") from error

    if not isinstance(matches, list):
        raise TypeError("Expected a JSON array of matches.")

    if not matches:
        raise ValueError("Matches array is empty.")

    seen = set()

    for position, match in enumerate(matches):
        _check_match(match, position, competition_id, season_id)

        if match["match_id"] in seen:
            raise ValueError(f"Duplicate match_id {match['match_id']} at position {position}.")
        seen.add(match["match_id"])

    return matches


def fetch_matches(competition_id: int, season_id: int, commit: str) -> bytes:
    return get_with_retries(
        matches_url(commit, competition_id, season_id),
        f"matches {competition_id}/{season_id}",
    )


def matches_object_key(competition_id: int, season_id: int, sha256: str) -> str:
    return f"{S3_PREFIX}/{competition_id}/{season_id}/{sha256}.json"


def matches_target(
    competition_id: int, season_id: int, output_dir: Path, manifest_path: Path
) -> BronzeTarget:
    unit = f"{competition_id}/{season_id}"

    return BronzeTarget(
        unit=unit,
        label=f"matches {unit}",
        identity={
            "dataset": DATASET,
            "unit": unit,
            "competition_id": competition_id,
            "season_id": season_id,
        },
        local_path=output_dir / f"{competition_id}_{season_id}.json",
        manifest_path=manifest_path,
        manifest_key=f"{S3_MANIFEST_PREFIX}/{competition_id}/{season_id}.json",
        object_key=lambda sha256: matches_object_key(competition_id, season_id, sha256),
        source_url=lambda commit: matches_url(commit, competition_id, season_id),
        unpinned_url=f"{UNPINNED_DATA_URL}/matches/{competition_id}/{season_id}.json",
        fetch=lambda commit: fetch_matches(competition_id, season_id, commit),
        validate=lambda content: {
            "record_count": len(parse_matches(content, competition_id, season_id))
        },
    )


def ingest_matches_for_season(
    competition_id: int,
    season_id: int,
    competitions: list[dict],
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
    source: SourceRef | None = None,
    refresh: bool = False,
) -> dict:
    """Ingest the match list of one competition season.

    ``competitions`` is the validated content of competitions.json. The season
    must be listed there; this is checked before anything is downloaded.
    """

    find_season(competitions, competition_id, season_id)

    if manifest_path is None:
        manifest_path = output_dir / "manifest.json"

    return bronze.ingest_object(
        matches_target(competition_id, season_id, output_dir, manifest_path),
        storage,
        source or SourceRef(),
        refresh,
    )


def load_matches(path: Path, competition_id: int, season_id: int) -> list[dict]:
    """Read and validate a stored match list."""

    return parse_matches(path.read_bytes(), competition_id, season_id)
