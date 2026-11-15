from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from football_platform.storage import S3Storage


@pytest.fixture
def storage(monkeypatch):
    for name, value in {
        "S3_ENDPOINT_URL": "http://localhost:0",
        "S3_ACCESS_KEY": "test-access",
        "S3_SECRET_KEY": "test-secret",
        "S3_REGION": "us-east-1",
        "S3_BUCKET": "test-bucket",
    }.items():
        monkeypatch.setenv(name, value)

    with patch("football_platform.storage.boto3.client") as client:
        instance = S3Storage()
        instance.client = client.return_value
        yield instance


def client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "GetObject",
    )


def test_download_bytes_if_exists_returns_content(storage):
    body = MagicMock()
    body.read.return_value = b"data"
    storage.client.get_object.return_value = {"Body": body}

    assert storage.download_bytes_if_exists("key") == b"data"


@pytest.mark.parametrize(
    ("code", "status"),
    [("NoSuchKey", 404), ("NoSuchBucket", 404), ("404", 404), ("NoSuchKey", 400)],
)
def test_download_bytes_if_exists_returns_none_when_missing(storage, code, status):
    storage.client.get_object.side_effect = client_error(code, status)

    assert storage.download_bytes_if_exists("key") is None


@pytest.mark.parametrize(("code", "status"), [("AccessDenied", 403), ("InternalError", 500)])
def test_download_bytes_if_exists_raises_other_errors(storage, code, status):
    storage.client.get_object.side_effect = client_error(code, status)

    with pytest.raises(ClientError):
        storage.download_bytes_if_exists("key")
