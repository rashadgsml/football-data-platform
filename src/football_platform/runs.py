"""Run records: one small JSON object per ingestion run.

A run record answers "what did that run do?" without reading logs: which
upstream commit it used, what happened to each match, and how it ended. Records
are write-once (the run_id is unique) and are never modified afterwards.
"""

import json
import re
import secrets
from datetime import UTC, datetime

RUN_PREFIX = "statsbomb/runs"
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+")


def new_run_id() -> str:
    """Sortable, unique run identifier such as ``20261115T101500Z-3fa9c1``."""

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{secrets.token_hex(3)}"


def run_record_key(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("run_id may only contain letters, digits, '_' and '-'.")

    return f"{RUN_PREFIX}/{run_id}.json"


SCHEMA_VERSION = 2


def outcome(result: dict) -> dict:
    """Summarise what happened to one stored object in a run."""

    return {
        "status": "cached" if result["skipped"] else "downloaded",
        "records": result.get("event_count", result.get("record_count")),
        "sha256": result["sha256"],
        "s3_key": result.get("s3_key"),
        "s3_uploaded": result["s3_uploaded"],
        **result.get("extra", {}),
    }


def build_run_record(
    run_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    source_commit: str | None,
    scope: dict | None = None,
    datasets: dict | None = None,
    matches: list[dict] | None = None,
    skipped_matches: list[dict] | None = None,
    stale_match_ids: list[int] | None = None,
    failure: dict | None = None,
) -> dict:
    """Describe a run.

    ``source_commit`` is None if nothing was downloaded. ``datasets`` holds the
    outcome of season-level files (competitions, matches); each entry of
    ``matches`` holds the outcomes of that match's lineups and events.
    ``failure`` names the dataset and unit that raised, for a failed run.
    """

    record = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "source": "StatsBomb Open Data",
        "source_commit": source_commit,
        "scope": scope,
        "datasets": datasets or {},
        "matches": matches or [],
        "skipped_matches": skipped_matches or [],
        "stale_match_ids": stale_match_ids or [],
    }

    if status == "failed":
        record["failure"] = failure

    return record


def serialize_run_record(record: dict) -> bytes:
    return json.dumps(record, indent=2, sort_keys=True).encode("utf-8")
