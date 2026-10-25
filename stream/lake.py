"""Data lake access (MinIO, S3 API): the client, object names, and writing gzipped JSON-lines objects."""
import gzip
import os
import re
import socket
import time

import boto3
from botocore.config import Config

BUCKET = os.environ.get("LAKE_BUCKET", "lake")
COUNT_IN_NAME = re.compile(r"-n(\d+)\.jsonl\.gz$")


def client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def part_name(count: int) -> str:
    """Object name for one flush. The event count is in the name so status checks never download files."""
    return f"part-{int(time.time() * 1000)}-{socket.gethostname()}-n{count}.jsonl.gz"


def count_from_name(key: str):
    match = COUNT_IN_NAME.search(key)
    return int(match.group(1)) if match else None


def put_lines(s3, key: str, lines) -> None:
    """Upload lines as one gzipped JSON-lines object. S3 objects appear only when complete."""
    body = gzip.compress(b"\n".join(lines) + b"\n")
    # Stored as a plain .gz file (no Content-Encoding header), so readers like DuckDB get the bytes as-is.
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/gzip")
