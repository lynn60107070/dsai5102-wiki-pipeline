"""Hourly batch: one Wikipedia pageview file -> two filtered Parquet files -> data lake (MinIO).

One run per UTC hour since 25 Oct 2026 05:00 (the first hour of edit data), so catchup backfills the history.

    wait_for_file -> fetch -> upload -> cleanup

wait_for_file  waits until Wikimedia has published the hour's file (it appears with some delay)
fetch          ingest.pageviews.fetch_hour (Sumaiya, S2): download, keep Wikipedia rows, write
               articles.parquet (>= 10 views) + wiki_totals.parquet (docs/design.md section 7)
upload         both files to s3://lake/raw/pageviews/dt=YYYY-MM-DD/hour=HH/ via the `minio_lake` connection;
               updates the Dataset s3://lake/raw/pageviews, which triggers warehouse_hourly
cleanup        removes the local copies, even if an earlier step failed

Re-running an hour overwrites the same two objects, so retries and backfills are safe (idempotent).

Hour alignment: a file named ...-HH0000.gz holds the hour ENDING at HH (verified against the Pageview
API, see ingest/pageviews.py), so the run for interval [HH:00, HH+1:00) fetches the file named HH+1.
The DAG is paused when first created; unpause it in the UI once S2 is merged (and set
`pageviews_enabled: true` in dbt/dbt_project.yml so the warehouse picks the files up).
"""
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import PokeReturnValue

LAKE_CONN = "minio_lake"
BUCKET = "lake"
PAGEVIEWS = Dataset("s3://lake/raw/pageviews")  # warehouse_hourly runs when this is updated
STAGING = Path("/tmp/pageviews")  # tasks share the scheduler container (LocalExecutor)


def _staging_dir(start: datetime) -> Path:
    return STAGING / f"{start:%Y%m%dT%H}"


def _file_hour(start: datetime) -> datetime:
    """Which pageview file (by the hour in its name) belongs to the run starting at `start`."""
    return start + timedelta(hours=1)


@dag(
    dag_id="pageviews_hourly",
    description="Hourly pageview file -> filtered Parquet -> lake",
    schedule="@hourly",
    start_date=datetime(2026, 10, 25, 5, tzinfo=timezone.utc),  # first hour of edit data (05:58 UTC)
    catchup=True,
    max_active_runs=1,  # one hour at a time: downloads dominate anyway, and it halves peak memory
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["batch", "pageviews"],
)
def pageviews_hourly():

    @task.sensor(poke_interval=600, timeout=6 * 3600, mode="reschedule")
    def wait_for_file(data_interval_start=None) -> PokeReturnValue:
        from ingest.pageviews import file_url

        hour = _file_hour(data_interval_start)
        url = file_url(hour.date(), hour.hour)
        response = requests.head(url, timeout=30, allow_redirects=True,
                                 headers={"User-Agent": os.environ.get("WIKI_USER_AGENT", "")})
        return PokeReturnValue(is_done=response.status_code == 200, xcom_value=url)

    @task
    def fetch(data_interval_start=None) -> dict:
        from ingest.pageviews import fetch_hour

        hour = _file_hour(data_interval_start)
        paths = fetch_hour(hour.date(), hour.hour, _staging_dir(data_interval_start))
        return {name: str(path) for name, path in paths.items()}

    @task(outlets=[PAGEVIEWS])
    def upload(paths: dict, data_interval_start=None) -> list:
        hook = S3Hook(aws_conn_id=LAKE_CONN)
        prefix = f"raw/pageviews/dt={data_interval_start:%Y-%m-%d}/hour={data_interval_start:%H}"
        keys = []
        for name, path in paths.items():
            key = f"{prefix}/{name}.parquet"
            hook.load_file(filename=path, key=key, bucket_name=BUCKET, replace=True)
            keys.append(key)
        return keys

    @task(trigger_rule="all_done")
    def cleanup(data_interval_start=None):
        shutil.rmtree(_staging_dir(data_interval_start), ignore_errors=True)

    paths = fetch()
    wait_for_file() >> paths
    upload(paths) >> cleanup()


pageviews_hourly()
