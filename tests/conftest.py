import pytest
from factories import COMMIT


@pytest.fixture(autouse=True)
def upstream_commit(monkeypatch):
    """Never touch the GitHub API: resolve every run to a fixed commit.

    ``upstream_commit.calls`` counts how often resolution was requested, and
    ``upstream_commit.value`` can be changed to simulate upstream moving on.
    """

    class Commit:
        value = COMMIT
        calls = 0

    def resolve():
        Commit.calls += 1
        return Commit.value

    monkeypatch.setattr("football_platform.source.resolve_source_commit", resolve)
    return Commit
