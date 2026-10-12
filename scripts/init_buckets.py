"""Create medallion buckets in MinIO (idempotent; waits until MinIO is ready)."""
import os
import time

import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://s3:9000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

for attempt in range(60):
    try:
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        break
    except Exception as e:
        print(f"waiting for minio ({attempt + 1}/60): {e.__class__.__name__}")
        time.sleep(2)
else:
    raise SystemExit("MinIO did not become ready")

for bucket in ("bronze", "silver", "gold"):
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"created bucket: {bucket}")
    else:
        print(f"bucket exists: {bucket}")
print("buckets ready")