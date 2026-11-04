import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str


def load_settings() -> Settings:
    required = {
        "S3_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
        "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY"),
        "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY"),
        "S3_REGION": os.getenv("S3_REGION"),
        "S3_BUCKET": os.getenv("S3_BUCKET"),
    }

    missing = [key for key, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"Missing environment variables: {', '.join(missing)}"
        )

    return Settings(
        s3_endpoint_url=required["S3_ENDPOINT_URL"],
        s3_access_key=required["S3_ACCESS_KEY"],
        s3_secret_key=required["S3_SECRET_KEY"],
        s3_region=required["S3_REGION"],
        s3_bucket=required["S3_BUCKET"],
    )
