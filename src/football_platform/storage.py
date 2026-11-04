from io import BytesIO

import boto3
from botocore.config import Config

from football_platform.config import load_settings


class S3Storage:
    """S3-compatible object storage operations."""

    def __init__(self) -> None:
        settings = load_settings()

        self.bucket = settings.s3_bucket

        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4"),
        )

    def create_bucket(self) -> None:
        """Create the configured bucket if it does not exist."""

        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            status_code = error.response.get(
                "ResponseMetadata", {}
            ).get("HTTPStatusCode")

            if status_code != 404:
                raise

            self.client.create_bucket(Bucket=self.bucket)

    def upload_bytes(self, key: str, content: bytes) -> None:
        """Upload bytes to the configured bucket."""

        self.client.upload_fileobj(
            BytesIO(content),
            self.bucket,
            key,
        )

    def download_bytes(self, key: str) -> bytes:
        """Download an object and return its contents."""

        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key,
        )

        return response["Body"].read()

    def list_objects(self, prefix: str = "") -> list[str]:
        """List object keys under a prefix."""

        paginator = self.client.get_paginator("list_objects_v2")

        keys = []

        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
        ):
            keys.extend(
                item["Key"] for item in page.get("Contents", [])
            )

        return keys
