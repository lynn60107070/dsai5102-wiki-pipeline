"""Compact the clean zone: merge each finished hour's small Parquet files into one, without duplicates.

The clean sinks write a file per hour every few minutes per replica, and (being at-least-once) can
repeat events after a restart. For every hour at least --min-age-hours old that has more than one file:
  1. DuckDB reads the hour's files, keeps one row per event_id, and writes a single new file
  2. only then are the old files deleted
A crash between 1 and 2 leaves duplicates (removed by the next run and by dbt), never a gap.

Run (inside Airflow, with dbt's Python, which has DuckDB + boto3):
  /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/compact_clean_zone.py [--min-age-hours 2]
"""
import argparse
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
import duckdb
from botocore.config import Config

BUCKET = os.environ.get("LAKE_BUCKET", "lake")
PREFIX = "clean/edits/"
ENDPOINT = os.environ.get("S3_ENDPOINT_HOST", "minio:9000")


def s3_client():
    return boto3.client("s3", endpoint_url=f"http://{ENDPOINT}", region_name="us-east-1",
                        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
                        config=Config(s3={"addressing_style": "path"}))


def duckdb_session():
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute(f"""CREATE SECRET lake (TYPE s3, KEY_ID '{os.environ["MINIO_ROOT_USER"]}',
        SECRET '{os.environ["MINIO_ROOT_PASSWORD"]}', ENDPOINT '{ENDPOINT}', URL_STYLE 'path', USE_SSL false)""")
    return con


def files_by_hour(s3) -> dict:
    hours = defaultdict(list)
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                hours[obj["Key"].rsplit("/", 1)[0]].append(obj["Key"])
    return hours


def hour_start(prefix: str) -> datetime:
    day, hour = prefix.split("/")[2][3:], prefix.split("/")[3][5:]
    return datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-age-hours", type=int, default=2,
                        help="only touch hours that ended at least this long ago (the sinks may still write)")
    args = parser.parse_args()

    s3, con = s3_client(), duckdb_session()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.min_age_hours + 1)
    total_before = total_after = 0
    for prefix, keys in sorted(files_by_hour(s3).items()):
        if len(keys) < 2 or hour_start(prefix) > cutoff:
            continue
        paths = [f"s3://{BUCKET}/{key}" for key in keys]
        con.execute("""CREATE OR REPLACE TEMP TABLE hour AS
                       SELECT * FROM read_parquet(?)
                       QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_ts) = 1""", [paths])
        before = con.execute("SELECT count(*) FROM read_parquet(?)", [paths]).fetchone()[0]
        after = con.execute("SELECT count(*) FROM hour").fetchone()[0]
        new_key = f"{prefix}/part-compacted-{int(time.time() * 1000)}-n{after}.parquet"
        con.execute(f"COPY hour TO 's3://{BUCKET}/{new_key}' (FORMAT parquet, COMPRESSION zstd)")
        for start in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[start:start + 1000]]})
        total_before, total_after = total_before + before, total_after + after
        print(f"{prefix}: {len(keys)} files -> 1, {before:,} rows -> {after:,} ({before - after:,} duplicates removed)")
    print(f"done: {total_before:,} rows -> {total_after:,} ({total_before - total_after:,} duplicates removed)")


if __name__ == "__main__":
    main()
