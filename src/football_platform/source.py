"""Access to the StatsBomb Open Data repository, pinned to a commit.

Downloading from a branch name such as ``master`` is not reproducible: the
same URL returns different bytes after upstream changes. A run therefore
resolves the branch to an immutable commit SHA once and downloads every file
from that commit, so the manifest can say exactly which upstream version the
bytes came from.
"""

import http.client
import re
import time
from collections.abc import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

RAW_BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data"
LATEST_COMMIT_URL = "https://api.github.com/repos/statsbomb/open-data/commits/master"

# Used only to describe data ingested before commits were recorded.
UNPINNED_BASE_URL = f"{RAW_BASE_URL}/master/data/events"

FETCH_TIMEOUT_SECONDS = 30
MAX_FETCH_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 1.0

COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")

# Network failures worth retrying. HTTPError is a URLError subclass and is
# handled separately so that only 5xx responses are retried.
TRANSIENT_ERRORS = (
    URLError,
    TimeoutError,
    ConnectionError,
    http.client.HTTPException,
)


def is_commit_sha(value: object) -> bool:
    return isinstance(value, str) and COMMIT_PATTERN.fullmatch(value) is not None


def events_url(commit: str, match_id: int) -> str:
    """URL of a match's event file at a specific upstream commit."""

    if not is_commit_sha(commit):
        raise ValueError("commit must be a 40-character lowercase hex SHA.")

    return f"{RAW_BASE_URL}/{commit}/data/events/{match_id}.json"


def get_with_retries(
    url: str,
    description: str,
    headers: dict[str, str] | None = None,
    max_attempts: int = MAX_FETCH_ATTEMPTS,
    backoff_seconds: float = RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """GET a URL, retrying transient failures with exponential backoff.

    Connection errors, timeouts, truncated reads and HTTP 5xx are retried, up
    to ``max_attempts`` attempts in total. Other HTTP errors, such as 404 or a
    403 rate limit, fail immediately because retrying cannot fix them.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    request = Request(url, headers=headers or {})
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
                return response.read()
        except HTTPError as error:
            if error.code < 500:
                raise RuntimeError(f"Could not download {description}: {error}") from error
            last_error = error
        except TRANSIENT_ERRORS as error:
            last_error = error

        if attempt < max_attempts:
            sleep(backoff_seconds * 2 ** (attempt - 1))

    raise RuntimeError(
        f"Could not download {description} after {max_attempts} attempts: {last_error}"
    ) from last_error


def resolve_source_commit(
    max_attempts: int = MAX_FETCH_ATTEMPTS,
    backoff_seconds: float = RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Return the commit SHA that the upstream ``master`` branch points to now.

    Uses the unauthenticated GitHub API (60 requests per hour per IP), which is
    ample for one call per run.
    """

    body = get_with_retries(
        LATEST_COMMIT_URL,
        "the latest StatsBomb commit",
        headers={
            "Accept": "application/vnd.github.sha",
            "User-Agent": "football-data-platform",
        },
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        sleep=sleep,
    )
    commit = body.decode("utf-8", errors="replace").strip()

    if not is_commit_sha(commit):
        raise ValueError("GitHub did not return a valid commit SHA.")

    return commit


class SourceRef:
    """The upstream commit used by one run, resolved lazily and only once.

    Laziness matters: a run where every file is already cached never needs the
    network, so it must not spend an API request either.
    """

    def __init__(self, commit: str | None = None) -> None:
        if commit is not None and not is_commit_sha(commit):
            raise ValueError("commit must be a 40-character lowercase hex SHA.")
        self._commit = commit

    @property
    def resolved(self) -> str | None:
        return self._commit

    def commit(self) -> str:
        if self._commit is None:
            self._commit = resolve_source_commit()
        return self._commit
