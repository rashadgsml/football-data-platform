"""Run orchestration: one run, one upstream commit, one run record."""

from pathlib import Path

from football_platform import bronze
from football_platform.datasets.events import ingest_match
from football_platform.runs import (
    build_run_record,
    new_run_id,
    run_record_key,
    serialize_run_record,
)
from football_platform.source import SourceRef
from football_platform.storage import S3Storage


def _write_run_record(
    storage: S3Storage,
    run_id: str,
    started_at: str,
    status: str,
    source: SourceRef,
    results: list[dict],
    failed_match_id: int | None = None,
    error: str | None = None,
) -> None:
    record = build_run_record(
        run_id=run_id,
        started_at=started_at,
        finished_at=bronze.utc_now(),
        status=status,
        source_commit=source.resolved,
        results=results,
        failed_match_id=failed_match_id,
        error=error,
    )
    storage.upload_bytes(run_record_key(run_id), serialize_run_record(record))


def ingest_matches(
    match_ids: list[int],
    output_dir: Path,
    manifest_path: Path | None = None,
    storage: S3Storage | None = None,
    run_id: str | None = None,
    refresh: bool = False,
) -> list[dict]:
    """Ingest multiple matches sequentially as one run.

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
    current: int | None = None

    try:
        for current in match_ids:
            results.append(
                ingest_match(current, output_dir, manifest_path, storage, source, refresh)
            )
    except Exception as error:
        if storage is not None:
            try:
                _write_run_record(
                    storage, run_id, started_at, "failed", source, results,
                    failed_match_id=current, error=str(error),
                )
            except Exception as record_error:  # noqa: BLE001 - must not mask the real failure
                error.add_note(f"Could not write run record {run_id}: {record_error}")
        raise

    if storage is not None:
        _write_run_record(storage, run_id, started_at, "succeeded", source, results)

    return results
