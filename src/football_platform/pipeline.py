"""Run orchestration: one run, one upstream commit, one run record.

``ingest_season`` is the metadata-driven entry point: it ingests competitions
and a season's match list, then lineups and events for every available match,
reconciling each against the match record. ``ingest_matches`` is the lower
level, events-only entry point for explicit match ids.
"""

import logging
from pathlib import Path

from football_platform import bronze
from football_platform.datasets.competitions import (
    ingest_competitions,
    load_competitions,
)
from football_platform.datasets.events import ingest_match
from football_platform.datasets.lineups import ingest_lineups, load_lineups
from football_platform.datasets.matches import ingest_matches_for_season, load_matches
from football_platform.runs import (
    build_run_record,
    new_run_id,
    outcome,
    run_record_key,
    serialize_run_record,
)
from football_platform.source import SourceRef
from football_platform.storage import S3Storage

logger = logging.getLogger(__name__)

AVAILABLE = "available"


def _write_run_record(storage: S3Storage, record: dict) -> None:
    storage.upload_bytes(run_record_key(record["run_id"]), serialize_run_record(record))


def _finish(
    storage: S3Storage | None,
    run_id: str,
    started_at: str,
    source: SourceRef,
    error: Exception | None = None,
    failure: dict | None = None,
    **sections,
) -> dict:
    """Build the run record and, with S3 configured, store it.

    A failed run's record is best effort: if writing it fails, the original
    error is kept and the record failure is attached as a note.
    """

    record = build_run_record(
        run_id=run_id,
        started_at=started_at,
        finished_at=bronze.utc_now(),
        status="failed" if error else "succeeded",
        source_commit=source.resolved,
        failure=failure,
        **sections,
    )

    if storage is None:
        return record

    if error is None:
        _write_run_record(storage, record)
        return record

    try:
        _write_run_record(storage, record)
    except Exception as record_error:  # noqa: BLE001 - must not mask the real failure
        error.add_note(f"Could not write run record {run_id}: {record_error}")

    return record


def ingest_matches(
    match_ids: list[int],
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
    run_id: str | None = None,
    refresh: bool = False,
) -> list[dict]:
    """Ingest events for explicit match ids as one run (no metadata checks).

    All matches share one upstream commit. When S3 is configured the run is
    recorded at ``statsbomb/runs/<run_id>.json``, including failed runs; the
    original exception is always re-raised.
    """

    if len(match_ids) != len(set(match_ids)):
        raise ValueError("Match IDs must be unique.")

    run_id = run_id or new_run_id()
    run_record_key(run_id)  # reject an unusable run_id before doing any work

    source = SourceRef()
    started_at = bronze.utc_now()
    results: list[dict] = []
    entries: list[dict] = []
    current: int | None = None

    try:
        for current in match_ids:
            result = ingest_match(
                current, output_dir, manifest_path, storage, source, refresh
            )
            results.append(result)
            entries.append({"match_id": current, "events": outcome(result)})
    except Exception as error:
        failure = {"dataset": "events", "unit": str(current), "error": str(error)}
        _finish(storage, run_id, started_at, source, error, failure, matches=entries)
        raise

    _finish(storage, run_id, started_at, source, matches=entries)
    return results


def _select_matches(
    matches: list[dict],
    competition_id: int,
    season_id: int,
    match_ids: list[int] | None,
    limit: int | None,
) -> list[dict]:
    selected = matches

    if match_ids is not None:
        if len(match_ids) != len(set(match_ids)):
            raise ValueError("Match IDs must be unique.")

        unknown = sorted(set(match_ids) - {match["match_id"] for match in matches})
        if unknown:
            raise ValueError(
                f"Match IDs {unknown} are not in competition {competition_id} season {season_id}."
            )

        wanted = set(match_ids)
        selected = [match for match in matches if match["match_id"] in wanted]

    return selected if limit is None else selected[:limit]


def ingest_season(
    competition_id: int,
    season_id: int,
    data_root: Path,
    storage: S3Storage | None = None,
    run_id: str | None = None,
    refresh: bool = False,
    match_ids: list[int] | None = None,
    limit: int | None = None,
) -> dict:
    """Ingest a whole competition season, driven by its match list.

    Order: competitions, the season's matches, then per available match its
    lineups followed by its events, each checked against the match record (and
    the events also against the lineups). All files come from one upstream
    commit. The first failure stops the run; the run record then names the
    dataset and unit that failed and the original error is re-raised.

    ``match_ids`` and ``limit`` restrict which matches are processed. Matches
    whose status is not "available" are skipped and reported. A match is
    reported stale when its metadata has changed upstream since its stored
    files were downloaded; staleness is only reported, never refreshed
    automatically.

    Returns the run record.
    """

    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1.")

    run_id = run_id or new_run_id()
    run_record_key(run_id)  # reject an unusable run_id before doing any work

    source = SourceRef()
    started_at = bronze.utc_now()
    scope = {"competition_id": competition_id, "season_id": season_id}
    datasets: dict = {}
    entries: list[dict] = []
    skipped: list[dict] = []
    stale: list[int] = []
    dataset, unit = "competitions", "all"

    logger.info("Run %s: ingesting competition %s season %s", run_id, competition_id, season_id)

    try:
        result = ingest_competitions(
            data_root / "competitions", storage=storage, source=source, refresh=refresh
        )
        datasets["competitions"] = outcome(result)
        competitions = load_competitions(data_root / "competitions" / "competitions.json")

        dataset, unit = "matches", f"{competition_id}/{season_id}"
        result = ingest_matches_for_season(
            competition_id,
            season_id,
            competitions,
            data_root / "matches",
            storage=storage,
            source=source,
            refresh=refresh,
        )
        datasets["matches"] = outcome(result)
        matches = load_matches(
            data_root / "matches" / f"{competition_id}_{season_id}.json",
            competition_id,
            season_id,
        )

        for match in _select_matches(matches, competition_id, season_id, match_ids, limit):
            match_id = match["match_id"]

            if match["match_status"] != AVAILABLE:
                logger.warning(
                    "Match %s skipped: status is %r, not %r",
                    match_id, match["match_status"], AVAILABLE,
                )
                skipped.append({"match_id": match_id, "reason": f"match_status={match['match_status']}"})
                continue

            dataset, unit = "lineups", str(match_id)
            lineups_result = ingest_lineups(
                match, data_root / "lineups", storage=storage, source=source, refresh=refresh
            )
            lineups = load_lineups(data_root / "lineups" / f"{match_id}.json")

            dataset = "events"
            events_result = ingest_match(
                match_id,
                data_root / "events",
                storage=storage,
                source=source,
                refresh=refresh,
                match=match,
                lineups=lineups,
            )

            recorded = [
                found
                for found in (
                    lineups_result["extra"]["match_last_updated"],
                    events_result["extra"]["match_last_updated"],
                )
                if found is not None
            ]
            is_stale = any(found != match["last_updated"] for found in recorded)
            if is_stale:
                logger.warning(
                    "Match %s is stale: metadata changed upstream since it was downloaded "
                    "(use --refresh to update)", match_id,
                )
                stale.append(match_id)

            entries.append(
                {
                    "match_id": match_id,
                    "lineups": outcome(lineups_result),
                    "events": outcome(events_result),
                    "stale": is_stale,
                }
            )
    except Exception as error:
        logger.error("Run %s failed in %s %s: %s", run_id, dataset, unit, error)
        failure = {"dataset": dataset, "unit": unit, "error": str(error)}
        _finish(
            storage, run_id, started_at, source, error, failure,
            scope=scope, datasets=datasets, matches=entries,
            skipped_matches=skipped, stale_match_ids=stale,
        )
        raise

    record = _finish(
        storage, run_id, started_at, source,
        scope=scope, datasets=datasets, matches=entries,
        skipped_matches=skipped, stale_match_ids=stale,
    )
    logger.info(
        "Run %s succeeded: %d matches, %d skipped, %d stale",
        run_id, len(entries), len(skipped), len(stale),
    )
    return record
