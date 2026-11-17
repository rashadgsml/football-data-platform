"""StatsBomb lineups: one JSON array per match with the two squads.

Lineups name every player in a team's match-day squad, including substitutes
who never played. They are reconciled against the match record before storage.
"""

import json
from pathlib import Path

from football_platform import bronze
from football_platform.bronze import BronzeTarget
from football_platform.reconcile import check_lineups_against_match, require_consistent
from football_platform.source import (
    UNPINNED_DATA_URL,
    SourceRef,
    get_with_retries,
    lineups_url,
)
from football_platform.storage import S3Storage

DATASET = "lineups"
S3_PREFIX = "statsbomb/lineups"
S3_MANIFEST_PREFIX = "statsbomb/manifests/lineups"


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_lineups(content: bytes) -> list[dict]:
    """Validate a lineups file and return its two teams.

    Each team needs an id, a name and a non-empty roster of players with an
    integer id and a name. Optional per-player fields (cards, positions,
    nickname) are tolerated.
    """

    try:
        teams = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Lineups content is not valid JSON.") from error

    if not isinstance(teams, list):
        raise TypeError("Expected a JSON array of team lineups.")

    if len(teams) != 2:
        raise ValueError(f"Expected exactly 2 team lineups, found {len(teams)}.")

    for position, team in enumerate(teams):
        if not isinstance(team, dict):
            raise TypeError(f"Team lineup {position} is not a JSON object.")

        problems = []

        if not _is_int(team.get("team_id")):
            problems.append("team_id must be an integer")

        if not isinstance(team.get("team_name"), str) or not team["team_name"]:
            problems.append("team_name must be a non-empty string")

        roster = team.get("lineup")
        if not isinstance(roster, list) or not roster:
            problems.append("lineup must be a non-empty list")
        else:
            seen = set()
            for player in roster:
                if not (
                    isinstance(player, dict)
                    and _is_int(player.get("player_id"))
                    and isinstance(player.get("player_name"), str)
                    and player["player_name"]
                ):
                    problems.append("every player needs an integer player_id and a player_name")
                    break
                if player["player_id"] in seen:
                    problems.append(f"duplicate player_id {player['player_id']}")
                    break
                seen.add(player["player_id"])

        if problems:
            raise ValueError(f"Team lineup {position} is invalid: {'; '.join(problems)}.")

    if teams[0]["team_id"] == teams[1]["team_id"]:
        raise ValueError("Both team lineups have the same team_id.")

    return teams


def fetch_lineups(match_id: int, commit: str) -> bytes:
    return get_with_retries(lineups_url(commit, match_id), f"lineups {match_id}")


def lineups_object_key(match_id: int, sha256: str) -> str:
    return f"{S3_PREFIX}/{match_id}/{sha256}.json"


def lineups_target(
    match: dict, output_dir: Path, manifest_path: Path
) -> BronzeTarget:
    match_id = match["match_id"]

    def validate(content: bytes) -> dict:
        teams = parse_lineups(content)
        require_consistent(
            f"Lineups for match {match_id}", check_lineups_against_match(teams, match)
        )
        return {"record_count": sum(len(team["lineup"]) for team in teams)}

    return BronzeTarget(
        unit=str(match_id),
        label=f"lineups {match_id}",
        identity={"dataset": DATASET, "unit": str(match_id), "match_id": match_id},
        local_path=output_dir / f"{match_id}.json",
        manifest_path=manifest_path,
        manifest_key=f"{S3_MANIFEST_PREFIX}/{match_id}.json",
        object_key=lambda sha256: lineups_object_key(match_id, sha256),
        source_url=lambda commit: lineups_url(commit, match_id),
        unpinned_url=f"{UNPINNED_DATA_URL}/lineups/{match_id}.json",
        fetch=lambda commit: fetch_lineups(match_id, commit),
        validate=validate,
        extra={"match_last_updated": match["last_updated"]},
    )


def ingest_lineups(
    match: dict,
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
    source: SourceRef | None = None,
    refresh: bool = False,
) -> dict:
    """Ingest one match's lineups, checking them against its match record."""

    if manifest_path is None:
        manifest_path = output_dir / "manifest.json"

    result = bronze.ingest_object(
        lineups_target(match, output_dir, manifest_path),
        storage,
        source or SourceRef(),
        refresh,
    )

    return {"match_id": match["match_id"], **result}


def load_lineups(path: Path) -> list[dict]:
    """Read and validate a stored lineups file."""

    return parse_lineups(path.read_bytes())
