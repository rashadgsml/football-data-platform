"""Cross-dataset consistency checks for one match.

Structural validation says a file is well formed; reconciliation says it
belongs to the match it claims to describe. Events and lineups carry no match
id of their own, so without these checks a file downloaded for the wrong match
(or a corrupted one that is still valid JSON) would be stored unnoticed.

Every check is a pure function over already-parsed JSON that returns a list of
human-readable problems; an empty list means consistent. ``require_consistent``
turns problems into one ``ReconciliationError``.

Only facts StatsBomb itself publishes are compared (team ids, final score,
starting elevens, rosters). Nothing is derived or estimated.
"""

from collections import Counter

PENALTY_SHOOTOUT_PERIOD = 5
MAX_REPORTED_PROBLEMS = 10


class ReconciliationError(ValueError):
    """Raised when datasets that describe the same match disagree."""

    def __init__(self, subject: str, problems: list[str]) -> None:
        self.problems = problems
        shown = "; ".join(problems[:MAX_REPORTED_PROBLEMS])
        extra = len(problems) - MAX_REPORTED_PROBLEMS
        suffix = f" (and {extra} more)" if extra > 0 else ""
        super().__init__(f"{subject} is inconsistent: {shown}{suffix}.")


def require_consistent(subject: str, problems: list[str]) -> None:
    if problems:
        raise ReconciliationError(subject, problems)


def _teams(match: dict) -> tuple[int, int]:
    return match["home_team"]["home_team_id"], match["away_team"]["away_team_id"]


def goals_from_events(events: list[dict], home_id: int, away_id: int) -> dict[int, int]:
    """Goals credited to each team during the match proper.

    A goal is a ``Shot`` whose outcome is "Goal"; an ``Own Goal Against`` event
    is recorded for the team that scored it and counts for the opponent.
    Penalty shootout kicks (period 5) are not goals of the match.
    """

    goals = Counter({home_id: 0, away_id: 0})
    opponent = {home_id: away_id, away_id: home_id}

    for event in events:
        if event.get("period") == PENALTY_SHOOTOUT_PERIOD:
            continue

        kind = event["type"]["name"]
        team = event["team"]["id"]

        if kind == "Shot" and event.get("shot", {}).get("outcome", {}).get("name") == "Goal":
            goals[team] += 1
        elif kind == "Own Goal Against" and team in opponent:
            goals[opponent[team]] += 1

    return {home_id: goals[home_id], away_id: goals[away_id]}


def check_events_against_match(events: list[dict], match: dict) -> list[str]:
    """Compare an events file with its match record."""

    home_id, away_id = _teams(match)
    expected_teams = {home_id, away_id}
    problems = []

    event_teams = {event["team"]["id"] for event in events}
    if event_teams != expected_teams:
        problems.append(f"event teams {sorted(event_teams)} differ from match teams {sorted(expected_teams)}")

    starters = Counter(
        event["team"]["id"] for event in events if event["type"]["name"] == "Starting XI"
    )
    if starters != Counter(expected_teams):
        problems.append(f"expected one Starting XI per team, found {dict(starters)}")

    goals = goals_from_events(events, home_id, away_id)
    if (goals[home_id], goals[away_id]) != (match["home_score"], match["away_score"]):
        problems.append(
            f"goals in events {goals[home_id]}-{goals[away_id]} differ from "
            f"match score {match['home_score']}-{match['away_score']}"
        )

    return problems


def check_lineups_against_match(lineups: list[dict], match: dict) -> list[str]:
    """Compare a lineups file with its match record."""

    expected_teams = set(_teams(match))
    lineup_teams = {team["team_id"] for team in lineups}

    if lineup_teams != expected_teams:
        return [
            f"lineup teams {sorted(lineup_teams)} differ from match teams {sorted(expected_teams)}"
        ]

    return []


def check_events_against_lineups(events: list[dict], lineups: list[dict]) -> list[str]:
    """Check that everyone who appears in the events is on their team's roster.

    The lineups list the whole match-day squad, so substitutes who never played
    are fine; a player in the events who is not on the roster is not.
    """

    roster = {
        team["team_id"]: {player["player_id"] for player in team["lineup"]} for team in lineups
    }
    unknown: set[tuple[int, int]] = set()

    def check(player_id: int, team_id: int) -> None:
        if player_id not in roster.get(team_id, set()):
            unknown.add((player_id, team_id))

    for event in events:
        team_id = event["team"]["id"]

        if isinstance(event.get("player"), dict):
            check(event["player"]["id"], team_id)

        if event["type"]["name"] == "Starting XI":
            for entry in event.get("tactics", {}).get("lineup", []):
                check(entry["player"]["id"], team_id)

        if event["type"]["name"] == "Substitution":
            replacement = event.get("substitution", {}).get("replacement")
            if isinstance(replacement, dict):
                check(replacement["id"], team_id)

    return [
        f"player {player_id} appears for team {team_id} but is not on its lineup"
        for player_id, team_id in sorted(unknown)
    ]
