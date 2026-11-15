import copy

import pytest
from factories import (
    AWAY_ID,
    HOME_ID,
    HOME_PLAYERS,
    consistent_match,
    make_event,
    make_lineups,
    make_match_events,
)

from football_platform.reconcile import (
    ReconciliationError,
    check_events_against_lineups,
    check_events_against_match,
    check_lineups_against_match,
    goals_from_events,
    require_consistent,
)

MATCH = consistent_match(1, 1)


def test_consistent_data_has_no_problems():
    events, lineups = make_match_events(1, 1), make_lineups()

    assert check_events_against_match(events, MATCH) == []
    assert check_lineups_against_match(lineups, MATCH) == []
    assert check_events_against_lineups(events, lineups) == []


# --- events vs match --------------------------------------------------------


def test_goals_are_counted_per_team_and_non_goal_shots_are_ignored():
    assert goals_from_events(make_match_events(3, 0), HOME_ID, AWAY_ID) == {HOME_ID: 3, AWAY_ID: 0}


def test_own_goals_count_for_the_opponent():
    events = make_match_events(home_goals=0, away_goals=0, own_goals_by_home=2)

    assert goals_from_events(events, HOME_ID, AWAY_ID) == {HOME_ID: 0, AWAY_ID: 2}
    assert check_events_against_match(events, consistent_match(0, 2)) == []


def test_penalty_shootout_kicks_are_not_match_goals():
    events = make_match_events(1, 1, shootout_goals=4)

    assert check_events_against_match(events, MATCH) == []


def test_score_mismatch_is_reported_with_both_scores():
    problems = check_events_against_match(make_match_events(2, 1), MATCH)

    assert problems == ["goals in events 2-1 differ from match score 1-1"]


def test_home_and_away_swapped_is_detected():
    problems = check_events_against_match(make_match_events(1, 0), consistent_match(0, 1))

    assert any("differ from match score" in p for p in problems)


def test_events_of_a_different_team_are_detected():
    events = make_match_events(1, 1) + [make_event(99, team={"id": 777, "name": "Other"})]

    problems = check_events_against_match(events, MATCH)

    assert any("event teams" in p for p in problems)


@pytest.mark.parametrize("keep", [0, 1])
def test_missing_starting_xi_is_detected(keep):
    xi = [e for e in make_match_events() if e["type"]["name"] == "Starting XI"]
    events = [e for e in make_match_events() if e["type"]["name"] != "Starting XI"] + xi[:keep]

    assert any("Starting XI" in p for p in check_events_against_match(events, MATCH))


def test_duplicate_starting_xi_is_detected():
    events = make_match_events()
    events.append(copy.deepcopy(events[0]))

    assert any("Starting XI" in p for p in check_events_against_match(events, MATCH))


# --- lineups vs match -------------------------------------------------------


def test_lineup_teams_must_match_the_match_record():
    lineups = make_lineups()
    lineups[1]["team_id"] = 12345

    problems = check_lineups_against_match(lineups, MATCH)

    assert len(problems) == 1 and "lineup teams" in problems[0]


# --- events vs lineups ------------------------------------------------------


def test_unused_substitutes_are_allowed():
    events = make_match_events()

    assert 112 in HOME_PLAYERS and not any(e.get("player", {}).get("id") == 112 for e in events)
    assert check_events_against_lineups(events, make_lineups()) == []


def test_player_missing_from_the_roster_is_detected():
    lineups = make_lineups(home_players=[p for p in HOME_PLAYERS if p != 101])

    problems = check_events_against_lineups(make_match_events(1, 1), lineups)

    assert "player 101 appears for team 904 but is not on its lineup" in problems


def test_player_on_the_wrong_team_is_detected():
    lineups = make_lineups()  # 101 is on the home roster only
    events = make_match_events(1, 1)
    events.append(make_event(50, team={"id": AWAY_ID, "name": "Away"}, player={"id": 101, "name": "x"}))

    assert any("player 101 appears for team 180" in p for p in check_events_against_lineups(events, lineups))


def test_starting_xi_and_substitution_players_are_checked():
    events = make_match_events()
    events.append(
        make_event(
            60,
            type={"id": 19, "name": "Substitution"},
            team={"id": HOME_ID, "name": "Home"},
            substitution={"replacement": {"id": 999, "name": "Ghost"}},
        )
    )
    lineups = make_lineups(home_players=HOME_PLAYERS[1:])  # 101 missing from roster

    problems = check_events_against_lineups(events, lineups)

    assert any("player 999" in p for p in problems)
    assert any("player 101" in p for p in problems)


# --- error type -------------------------------------------------------------


def test_require_consistent_is_silent_without_problems():
    require_consistent("Events", [])


def test_error_is_a_value_error_that_names_the_subject_and_problems():
    with pytest.raises(ValueError, match="Events for match 1 is inconsistent: a; b") as raised:
        require_consistent("Events for match 1", ["a", "b"])

    assert isinstance(raised.value, ReconciliationError)
    assert raised.value.problems == ["a", "b"]


def test_long_problem_lists_are_truncated_in_the_message():
    with pytest.raises(ReconciliationError, match=r"\(and 5 more\)") as raised:
        require_consistent("Events", [f"p{n}" for n in range(15)])

    assert len(raised.value.problems) == 15
