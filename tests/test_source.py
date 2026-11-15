from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest
from factories import COMMIT

from football_platform import source
from football_platform.datasets.events import fetch_match_events
from football_platform.source import SourceRef, events_url

URLOPEN = "football_platform.source.urlopen"


def response(body: bytes) -> MagicMock:
    mock = MagicMock()
    mock.__enter__.return_value.read.return_value = body
    return mock


def test_events_url_is_pinned_to_the_commit():
    assert events_url(COMMIT, 3895158) == (
        f"https://raw.githubusercontent.com/statsbomb/open-data/{COMMIT}"
        "/data/events/3895158.json"
    )


@pytest.mark.parametrize("bad", ["master", "", "A" * 40, "a" * 39, "g" * 40])
def test_events_url_rejects_anything_but_a_commit_sha(bad):
    with pytest.raises(ValueError, match="commit"):
        events_url(bad, 1)


def test_fetch_requests_the_pinned_url():
    with patch(URLOPEN, return_value=response(b"data")) as urlopen:
        fetch_match_events(7, COMMIT, sleep=lambda _: None)

    assert urlopen.call_args.args[0].full_url == events_url(COMMIT, 7)


def test_resolve_source_commit_reads_the_sha_from_github(monkeypatch):
    # The conftest fixture replaces resolution; undo it to test the real function.
    monkeypatch.undo()

    with patch(URLOPEN, return_value=response(f"{COMMIT}\n".encode())) as urlopen:
        assert source.resolve_source_commit() == COMMIT

    request = urlopen.call_args.args[0]
    assert request.full_url == source.LATEST_COMMIT_URL
    assert request.get_header("Accept") == "application/vnd.github.sha"


def test_resolve_source_commit_rejects_unexpected_content(monkeypatch):
    monkeypatch.undo()

    with (
        patch(URLOPEN, return_value=response(b"<html>not a sha</html>")),
        pytest.raises(ValueError, match="valid commit SHA"),
    ):
        source.resolve_source_commit()


def test_resolve_source_commit_does_not_retry_rate_limits(monkeypatch):
    monkeypatch.undo()
    error = HTTPError("https://api.github.com", 403, "rate limit exceeded", None, None)

    with (
        patch(URLOPEN, side_effect=error) as urlopen,
        pytest.raises(RuntimeError, match="latest StatsBomb commit.*403"),
    ):
        source.resolve_source_commit(sleep=lambda _: None)

    assert urlopen.call_count == 1


def test_source_ref_resolves_once(upstream_commit):
    ref = SourceRef()

    assert ref.resolved is None
    assert ref.commit() == COMMIT
    assert ref.commit() == COMMIT
    assert upstream_commit.calls == 1
    assert ref.resolved == COMMIT


def test_source_ref_with_explicit_commit_never_resolves(upstream_commit):
    assert SourceRef(COMMIT).commit() == COMMIT
    assert upstream_commit.calls == 0


def test_source_ref_rejects_invalid_commit():
    with pytest.raises(ValueError, match="commit"):
        SourceRef("master")
