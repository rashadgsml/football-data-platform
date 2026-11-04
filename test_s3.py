import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8333",
    aws_access_key_id="football_access_key",
    aws_secret_access_key="football_secret_key_2026",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

bucket = "football-raw"
key = "test/hello.txt"
content = b"Football Data Engineering Platform"

# Create bucket
s3.create_bucket(Bucket=bucket)
print("Bucket created")

# Upload
s3.put_object(Bucket=bucket, Key=key, Body=content)
print("File uploaded")

# List objects
response = s3.list_objects_v2(Bucket=bucket)
print("Objects:", [obj["Key"] for obj in response.get("Contents", [])])

# Download and verify
downloaded = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
assert downloaded == content

print("Upload and download verification successful")
