from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import pytest

from football_platform.ingestion import fetch_match_events

URLOPEN = "football_platform.ingestion.urlopen"


def response(body: bytes = b"[]") -> MagicMock:
    mock = MagicMock()
    mock.__enter__.return_value.read.return_value = body
    return mock


def http_error(code: int) -> HTTPError:
    return HTTPError("https://example.test", code, "error", None, None)


def read_timeout() -> MagicMock:
    mock = MagicMock()
    mock.__enter__.return_value.read.side_effect = TimeoutError("timed out")
    return mock


def test_fetch_returns_content_on_first_success():
    sleeps = []

    with patch(URLOPEN, return_value=response(b"data")) as urlopen:
        assert fetch_match_events(1, sleep=sleeps.append) == b"data"

    assert urlopen.call_count == 1
    assert sleeps == []


def test_fetch_retries_http_5xx_then_succeeds():
    sleeps = []

    with patch(URLOPEN, side_effect=[http_error(503), http_error(500), response(b"ok")]) as urlopen:
        assert fetch_match_events(1, sleep=sleeps.append) == b"ok"

    assert urlopen.call_count == 3
    assert sleeps == [1.0, 2.0]  # exponential backoff


def test_fetch_retries_connection_errors():
    with patch(URLOPEN, side_effect=[URLError("dns"), ConnectionResetError(), response(b"ok")]):
        assert fetch_match_events(1, sleep=lambda _: None) == b"ok"


def test_fetch_retries_read_timeouts():
    with patch(URLOPEN, side_effect=[read_timeout(), response(b"ok")]) as urlopen:
        assert fetch_match_events(1, sleep=lambda _: None) == b"ok"

    assert urlopen.call_count == 2


def test_fetch_gives_up_after_bounded_attempts():
    sleeps = []

    with (
        patch(URLOPEN, side_effect=lambda *a, **k: read_timeout()) as urlopen,
        pytest.raises(RuntimeError, match="after 3 attempts.*timed out"),
    ):
        fetch_match_events(1, sleep=sleeps.append)

    assert urlopen.call_count == 3
    assert len(sleeps) == 2  # no sleep after the final attempt


def test_fetch_respects_custom_attempt_limit():
    with (
        patch(URLOPEN, side_effect=http_error(502)) as urlopen,
        pytest.raises(RuntimeError, match="after 5 attempts"),
    ):
        fetch_match_events(1, max_attempts=5, sleep=lambda _: None)

    assert urlopen.call_count == 5


@pytest.mark.parametrize("code", [400, 403, 404])
def test_fetch_does_not_retry_client_errors(code):
    sleeps = []

    with (
        patch(URLOPEN, side_effect=http_error(code)) as urlopen,
        pytest.raises(RuntimeError, match="Could not download match 1"),
    ):
        fetch_match_events(1, sleep=sleeps.append)

    assert urlopen.call_count == 1
    assert sleeps == []


def test_fetch_rejects_invalid_arguments():
    with pytest.raises(ValueError, match="positive"):
        fetch_match_events(0)

    with pytest.raises(ValueError, match="max_attempts"):
        fetch_match_events(1, max_attempts=0)
