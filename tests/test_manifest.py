import json

from football_platform.manifest import save_manifest


def test_save_manifest_creates_file(tmp_path):
    manifest_path = tmp_path / "data" / "manifest.json"

    entry = {
        "match_id": 3895158,
        "event_count": 3866,
        "sha256": "abc123",
    }

    save_manifest(entry, manifest_path)

    assert manifest_path.exists()

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert saved_manifest == [entry]


def test_save_manifest_appends_entries(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    first_entry = {"match_id": 3895158}
    second_entry = {"match_id": 3895159}

    save_manifest(first_entry, manifest_path)
    save_manifest(second_entry, manifest_path)

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert saved_manifest == [first_entry, second_entry]