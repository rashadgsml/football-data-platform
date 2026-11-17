"""Shared Bronze ingestion mechanics for one source file.

Every StatsBomb dataset (events, lineups, matches, competitions) is ingested the
same way: reuse a valid local copy or download from a pinned upstream commit,
validate, store at a write-once content-addressed S3 key, and record a manifest.
What differs per dataset is described by a ``BronzeTarget``: where the file
lives, how to download it, and how to validate it. The dataset modules under
``football_platform.datasets`` own those rules explicitly; this module only
runs the common steps.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from football_platform.checksums import calculate_sha256
from football_platform.manifest import (
    find_entry,
    load_manifest,
    parse_entry,
    save_manifest,
    serialize_entry,
)
from football_platform.source import SourceRef
from football_platform.storage import S3Storage

logger = logging.getLogger(__name__)


def utc_now() -> str:
    """Return the current UTC time as an ISO 8601 string."""

    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class BronzeTarget:
    """Everything that is specific to one source file.

    ``validate`` must raise on invalid content and otherwise return summary
    fields (for example ``{"event_count": 3866}``) that are copied into the
    result and the manifest entry.

    ``extra`` holds provenance recorded at download time, such as the match's
    upstream ``last_updated``. It is written only when content is downloaded
    and carried over unchanged otherwise, so a later run can tell that the
    metadata has moved on since the stored copy was fetched.
    """

    unit: str  # identity within the dataset, e.g. "3895158"
    label: str  # for messages, e.g. "match 3895158"
    identity: dict  # entry fields that identify it, e.g. {"match_id": 3895158}
    local_path: Path
    manifest_path: Path
    manifest_key: str
    object_key: Callable[[str], str]  # sha256 -> write-once S3 key
    source_url: Callable[[str], str]  # upstream commit -> URL
    unpinned_url: str  # describes data ingested before commits were tracked
    fetch: Callable[[str], bytes]  # upstream commit -> content
    validate: Callable[[bytes], dict]
    extra: dict = field(default_factory=dict)


def save_raw(content: bytes, path: Path, validate: Callable[[bytes], dict]) -> dict:
    """Validate and atomically save original content locally.

    Validation happens before anything is written, so invalid content leaves
    no file (and no directory) behind.
    """

    summary = validate(content)

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".tmp")

    try:
        temporary_path.write_bytes(content)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)

    return {"file_path": str(path), **summary, "sha256": calculate_sha256(content)}


def _load_cached(
    path: Path, previous: dict | None, validate: Callable[[bytes], dict]
) -> dict | None:
    """Return a summary of a usable local file, or None if it must be replaced.

    A cached file is unusable if it cannot be read, fails validation, or does
    not match the checksum recorded when it was ingested.
    """

    try:
        content = path.read_bytes()
        summary = validate(content)
    except (OSError, ValueError, TypeError):
        return None

    sha256 = calculate_sha256(content)
    recorded = previous.get("sha256") if previous else None

    if recorded and recorded != sha256:
        return None

    return {
        "content": content,
        "result": {"file_path": str(path), **summary, "sha256": sha256},
    }


def sync_object(storage: S3Storage, key: str, content: bytes, sha256: str) -> bool:
    """Upload content only if S3 differs, then verify. Return True if uploaded.

    Comparison and verification use SHA-256 of the object bytes, never ETags.
    """

    existing = storage.download_bytes_if_exists(key)

    if existing is not None and calculate_sha256(existing) == sha256:
        return False

    storage.upload_bytes(key, content)

    uploaded = storage.download_bytes(key)
    if calculate_sha256(uploaded) != sha256:
        raise RuntimeError(
            f"Upload verification failed for {key}: "
            "downloaded content does not match the SHA-256 of the source."
        )

    return True


def build_entry(
    target: BronzeTarget,
    result: dict,
    previous: dict | None,
    commit: str | None,
    uploaded: bool,
    now: str,
) -> dict:
    """Build the manifest entry, preserving timestamps when nothing happened.

    ``commit`` is the upstream commit used for a download in this run, or None
    if the content came from the local cache. ``retrieved_at`` is when the
    content was last downloaded from the source and ``s3_uploaded_at`` when it
    was last uploaded to S3; neither changes merely because an existing file or
    object was checked. When the content changed, the previous version is
    appended to ``history`` and never dropped.
    """

    downloaded = commit is not None

    if downloaded:
        retrieved_at, basis = now, "download"
        source_commit = commit
        source_url = target.source_url(commit)
    elif previous and previous.get("retrieved_at"):
        retrieved_at = previous["retrieved_at"]
        basis = previous.get("retrieved_at_basis", "download")
        # Recorded before commits were tracked: unknown, not guessed.
        source_commit = previous.get("source_commit")
        source_url = previous.get("source_url", target.unpinned_url)
    else:
        # Local file adopted without a record: file mtime is the best evidence.
        modified = Path(result["file_path"]).stat().st_mtime
        retrieved_at = datetime.fromtimestamp(modified, UTC).isoformat()
        basis = "file_mtime"
        source_commit = None
        source_url = target.unpinned_url

    history = list(previous.get("history", [])) if previous else []
    same_content = bool(previous) and previous.get("sha256") == result["sha256"]

    if previous and not same_content:
        history.append(
            {
                "sha256": previous.get("sha256"),
                "s3_key": previous.get("s3_key"),
                "source_commit": previous.get("source_commit"),
                "retrieved_at": previous.get("retrieved_at"),
                "superseded_at": now,
            }
        )

    summary = {
        name: value
        for name, value in result.items()
        if name not in {"file_path", "sha256", "s3_key", "s3_uri"}
    }

    entry = {
        **target.identity,
        "source": "StatsBomb Open Data",
        "source_url": source_url,
        "source_commit": source_commit,
        "file_path": result["file_path"],
        **summary,
        "sha256": result["sha256"],
        "retrieved_at": retrieved_at,
        "retrieved_at_basis": basis,
        "history": history,
    }

    if downloaded:
        entry.update(target.extra)
    elif previous:
        # Never backfilled: for older entries we cannot know what was current
        # when the content was downloaded.
        entry.update({name: previous[name] for name in target.extra if name in previous})

    if "s3_key" in result:
        entry["s3_key"] = result["s3_key"]
        entry["s3_uri"] = result["s3_uri"]
        # None means the upload time is unknown (object predates this field).
        entry["s3_uploaded_at"] = (
            now if uploaded else previous.get("s3_uploaded_at") if same_content else None
        )
    elif same_content:
        # A run without S3 must not erase what an earlier run recorded.
        for name in ("s3_key", "s3_uri", "s3_uploaded_at"):
            if name in previous:
                entry[name] = previous[name]

    return entry


def ingest_object(
    target: BronzeTarget,
    storage: S3Storage | None,
    source: SourceRef,
    refresh: bool = False,
) -> dict:
    """Ingest one source file locally and optionally sync it to S3.

    Existing manifests are read and validated before anything is written or
    uploaded. A valid local file is reused without downloading (and without
    resolving the upstream commit) unless ``refresh`` is set. If S3 is
    configured the content is stored at a write-once, content-addressed key,
    uploaded only when that object is missing or differs, and verified by
    SHA-256 after upload.

    Returns the summary fields plus ``file_path``, ``sha256``, ``skipped``
    (True when nothing was downloaded), ``s3_uploaded`` and ``source_commit``,
    ``extra`` (the recorded provenance fields), and ``s3_key`` / ``s3_uri``
    when S3 is configured.
    """

    now = utc_now()

    # Validate manifests first: nothing is written until both are readable.
    local_entry = find_entry(load_manifest(target.manifest_path), target.unit)
    s3_entry = None

    if storage is not None:
        raw_manifest = storage.download_bytes_if_exists(target.manifest_key)
        if raw_manifest is not None:
            s3_entry = parse_entry(raw_manifest, target.unit, target.label)

    previous = s3_entry or local_entry

    cached = (
        _load_cached(target.local_path, previous, target.validate)
        if target.local_path.exists() and not refresh
        else None
    )

    if cached is not None:
        content = cached["content"]
        result = cached["result"]
        commit = None
    else:
        commit = source.commit()
        content = target.fetch(commit)
        result = save_raw(content, target.local_path, target.validate)

    uploaded = False

    if storage is not None:
        storage.create_bucket()

        object_key = target.object_key(result["sha256"])
        uploaded = sync_object(storage, object_key, content, result["sha256"])

        result["s3_key"] = object_key
        result["s3_uri"] = f"s3://{storage.bucket}/{object_key}"

    entry = build_entry(target, result, previous, commit, uploaded, now)

    # Rewrite manifests only when their content actually changed.
    if storage is not None and entry != s3_entry:
        storage.upload_bytes(target.manifest_key, serialize_entry(entry))

    if entry != local_entry:
        save_manifest(entry, target.manifest_path)

    result["skipped"] = commit is None
    result["s3_uploaded"] = uploaded
    result["source_commit"] = entry["source_commit"]
    result["extra"] = {name: entry.get(name) for name in target.extra}

    logger.info(
        "%s %s: %s%s",
        target.identity.get("dataset", "events"),
        target.unit,
        "cached" if commit is None else "downloaded",
        ", uploaded to S3" if uploaded else "",
    )
    return result
