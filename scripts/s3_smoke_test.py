"""Manual smoke test for the configured S3-compatible storage.

Writes, reads back and lists a small object under the ``test/`` prefix of the
bucket named in ``.env``. This is not a pytest test and never runs on import.

Usage:
    python scripts/s3_smoke_test.py
"""

from football_platform.storage import S3Storage


def main() -> None:
    storage = S3Storage()

    storage.create_bucket()

    key = "test/integration_test.txt"
    original = b"Football Data Engineering Platform"

    storage.upload_bytes(key, original)
    downloaded = storage.download_bytes(key)

    assert downloaded == original

    print("Uploaded and downloaded:", key)
    print("Objects:", storage.list_objects("test/"))
    print("S3 integration successful")


if __name__ == "__main__":
    main()
