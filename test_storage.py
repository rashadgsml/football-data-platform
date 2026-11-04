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
