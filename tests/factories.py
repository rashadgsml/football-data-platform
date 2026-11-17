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


def make_competition(competition_id: int = 9, season_id: int = 281, **overrides) -> dict:
    """A competitions.json row that satisfies validation."""

    row = {
        "competition_id": competition_id,
        "season_id": season_id,
        "competition_name": "1. Bundesliga",
        "season_name": "2023/2024",
        "country_name": "Germany",
        "match_updated": "2024-09-28T20:46:38.893391",
    }
    row.update(overrides)
    return row


def competitions_bytes(*rows: dict) -> bytes:
    return json.dumps(list(rows) or [make_competition()]).encode("utf-8")


def make_match(
    match_id: int = 3895158,
    competition_id: int = 9,
    season_id: int = 281,
    home_team_id: int = 904,
    away_team_id: int = 180,
    home_score: int = 1,
    away_score: int = 1,
    **overrides,
) -> dict:
    """A matches/<c>/<s>.json record that satisfies validation."""

    match = {
        "match_id": match_id,
        "match_date": "2023-12-03",
        "match_status": "available",
        "last_updated": "2024-07-29T12:45:28.152104",
        "home_score": home_score,
        "away_score": away_score,
        "home_team": {"home_team_id": home_team_id, "home_team_name": "Home"},
        "away_team": {"away_team_id": away_team_id, "away_team_name": "Away"},
        "competition": {"competition_id": competition_id, "competition_name": "1. Bundesliga"},
        "season": {"season_id": season_id, "season_name": "2023/2024"},
    }
    match.update(overrides)
    return match


def matches_bytes(*matches: dict) -> bytes:
    return json.dumps(list(matches) or [make_match()]).encode("utf-8")


# --- internally consistent match data (match + lineups + events) -----------

HOME_ID, AWAY_ID = 904, 180
HOME_PLAYERS = list(range(101, 113))  # 11 starters + 1 unused substitute (112)
AWAY_PLAYERS = list(range(201, 213))


def make_lineup_team(team_id: int, player_ids: list[int]) -> dict:
    return {
        "team_id": team_id,
        "team_name": f"Team {team_id}",
        "lineup": [{"player_id": pid, "player_name": f"Player {pid}"} for pid in player_ids],
    }


def make_lineups(
    home_players: list[int] = HOME_PLAYERS, away_players: list[int] = AWAY_PLAYERS
) -> list[dict]:
    return [make_lineup_team(HOME_ID, home_players), make_lineup_team(AWAY_ID, away_players)]


def lineups_bytes(lineups: list[dict] | None = None) -> bytes:
    return json.dumps(lineups if lineups is not None else make_lineups()).encode("utf-8")


def _starting_xi(index: int, team_id: int, players: list[int]) -> dict:
    return make_event(
        index,
        type={"id": 35, "name": "Starting XI"},
        team={"id": team_id, "name": f"Team {team_id}"},
        tactics={"lineup": [{"player": {"id": pid, "name": f"Player {pid}"}} for pid in players[:11]]},
    )


def _shot(index: int, team_id: int, player_id: int, outcome: str, period: int = 1) -> dict:
    return make_event(
        index,
        period=period,
        type={"id": 16, "name": "Shot"},
        team={"id": team_id, "name": f"Team {team_id}"},
        player={"id": player_id, "name": f"Player {player_id}"},
        shot={"outcome": {"id": 97, "name": outcome}},
    )


def make_match_events(
    home_goals: int = 1,
    away_goals: int = 1,
    shootout_goals: int = 0,
    own_goals_by_home: int = 0,
) -> list[dict]:
    """Events consistent with ``make_match(home_score=..., away_score=...)``.

    ``own_goals_by_home`` own goals are recorded against the home team, so they
    count for the away side; ``home_goals``/``away_goals`` are shots only.
    """

    events = [_starting_xi(1, HOME_ID, HOME_PLAYERS), _starting_xi(2, AWAY_ID, AWAY_PLAYERS)]
    events.append(_shot(len(events) + 1, HOME_ID, 101, "Saved"))  # not a goal

    for _ in range(home_goals):
        events.append(_shot(len(events) + 1, HOME_ID, 101, "Goal"))
    for _ in range(away_goals):
        events.append(_shot(len(events) + 1, AWAY_ID, 201, "Goal"))
    for _ in range(own_goals_by_home):
        events.append(
            make_event(
                len(events) + 1,
                type={"id": 20, "name": "Own Goal Against"},
                team={"id": HOME_ID, "name": "Home"},
                player={"id": 102, "name": "Player 102"},
            )
        )
    for _ in range(shootout_goals):
        events.append(_shot(len(events) + 1, HOME_ID, 101, "Goal", period=5))

    return events


def consistent_match(
    home_score: int = 1, away_score: int = 1, match_id: int = 3895158, **overrides
) -> dict:
    return make_match(
        match_id,
        home_team_id=HOME_ID,
        away_team_id=AWAY_ID,
        home_score=home_score,
        away_score=away_score,
        **overrides,
    )
