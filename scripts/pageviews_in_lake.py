"""Print `yes` if the lake holds pageview files (both kinds the dbt models read), otherwise `no`.

warehouse_hourly runs this before dbt: on a fresh clone the lake has only the bundled edit sample and
pageviews_hourly starts paused, so dbt would fail on `stg_pageviews` ("No files found"). With `no`, dbt
builds without the pageview models; as soon as pageviews_hourly has uploaded an hour, they are back on.
A lake it can't reach raises an error (the task fails) rather than printing `no`, so an outage never
silently switches the pageview models off.

Run (inside Airflow, with dbt's Python): /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/pageviews_in_lake.py
"""
import os

import boto3
from botocore.config import Config

BUCKET = os.environ.get("LAKE_BUCKET", "lake")
PREFIX = "raw/pageviews/"
NEEDED = ("articles.parquet", "wiki_totals.parquet")  # read by stg_pageviews and stg_wiki_views


def main():
    s3 = boto3.client("s3", endpoint_url=f"http://{os.environ.get('S3_ENDPOINT_HOST', 'minio:9000')}",
                      aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                      aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
                      region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
    found = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name in NEEDED:
                found.add(name)
        if len(found) == len(NEEDED):
            break
    print("yes" if len(found) == len(NEEDED) else "no")


if __name__ == "__main__":
    main()
