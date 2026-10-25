"""Fail (exit 1) if the stream has stopped writing to the lake, so the Airflow task turns red.

Checks the newest object in the raw zone (raw-sink) and in the clean zone (clean-sink). Each sink writes
every FLUSH_SECONDS (5 min), so anything older than --max-age-minutes means a stopped service, the
laptop was asleep, or Wikimedia's stream is down.

Run (inside Airflow, with dbt's Python): /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/check_freshness.py
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

BUCKET = os.environ.get("LAKE_BUCKET", "lake")


def newest(s3, prefix: str, since: datetime):
    """Newest LastModified under prefix, only listing today's and yesterday's partitions (fast)."""
    latest = None
    for day in (since.date(), (since + timedelta(days=1)).date(), datetime.now(timezone.utc).date()):
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{prefix}dt={day:%Y-%m-%d}/"):
            for obj in page.get("Contents", []):
                if latest is None or obj["LastModified"] > latest:
                    latest = obj["LastModified"]
    return latest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age-minutes", type=int, default=15)
    args = parser.parse_args()

    s3 = boto3.client("s3", endpoint_url=f"http://{os.environ.get('S3_ENDPOINT_HOST', 'minio:9000')}",
                      aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                      aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
                      region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
    now = datetime.now(timezone.utc)
    stale = []
    for zone in ("raw/edits/", "clean/edits/"):
        latest = newest(s3, zone, now - timedelta(days=1))
        age = (now - latest).total_seconds() / 60 if latest else None
        print(f"{zone:<14} newest file: {latest or 'none'}" + (f" ({age:.0f} min ago)" if age is not None else ""))
        if age is None or age > args.max_age_minutes:
            stale.append(zone)
    if stale:
        sys.exit(f"STALE: no new files in {', '.join(stale)} for over {args.max_age_minutes} min "
                 "(collector stopped? check `docker compose ps` and the producer/sink logs)")
    print("fresh")


if __name__ == "__main__":
    main()
