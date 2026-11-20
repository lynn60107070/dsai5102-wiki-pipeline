"""Hourly warehouse refresh: lake -> dbt (DuckDB) -> Postgres `serving` -> Metabase.

    check_freshness            fails (red) if the stream stopped writing to the lake; alerting only,
                               it does not block the refresh
    compact_clean_zone ─► dbt_build ─► export_marts

compact_clean_zone  merges each finished hour of the clean zone into one Parquet file, without duplicates
dbt_build           seeds, models and all data tests. It is the quality gate: if a test fails, the task
                    fails and export_marts does not run, so the dashboard keeps the last good data.
                    The pageview models are skipped while the lake has no pageview files (fresh clone)
export_marts        copies every marts.* table into Postgres `serving` (read by Metabase)

Schedule: every hour at :20 (after the sinks' flushes), AND whenever pageviews_hourly lands new files
(Airflow Dataset `s3://lake/raw/pageviews`). One run at a time, because DuckDB allows a single writer;
don't run dbt by hand while this DAG is running.
"""
from datetime import datetime, timedelta, timezone

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

PAGEVIEWS = Dataset("s3://lake/raw/pageviews")
PYTHON = "/opt/airflow/dbt-venv/bin/python"
SCRIPTS = "/opt/airflow/project/scripts"


@dag(
    dag_id="warehouse_hourly",
    description="Lake -> dbt (DuckDB) -> Postgres serving, hourly and on new pageviews",
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("20 * * * *", timezone="UTC"),
        datasets=[PAGEVIEWS],
    ),
    start_date=datetime(2026, 10, 25, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # safe to run from the start: it only reads the lake

    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["warehouse", "dbt"],
)
def warehouse_hourly():
    BashOperator(
        task_id="check_freshness",
        bash_command=f"{PYTHON} {SCRIPTS}/check_freshness.py --max-age-minutes 15",
        # Retries give a fresh install time for the sinks' first flush (every 5 min); without them the
        # first run after `docker compose up` was always red. A real outage is still red 10 min later.
        retries=2,
    )

    compact = BashOperator(
        task_id="compact_clean_zone",
        bash_command=f"{PYTHON} {SCRIPTS}/compact_clean_zone.py --min-age-hours 2",
    )
    # Without pageview files in the lake (fresh clone: edit sample only, pageviews_hourly paused) the
    # pageview models are switched off for this run instead of failing it; see scripts/pageviews_in_lake.py.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "set -euo pipefail; cd $DBT_PROJECT_DIR; "
            f"pageviews=$({PYTHON} {SCRIPTS}/pageviews_in_lake.py); "
            'if [ "$pageviews" = yes ]; then /opt/airflow/dbt-venv/bin/dbt build --no-use-colors; '
            'else echo "no pageview files in the lake yet: building without the pageview models"; '
            "/opt/airflow/dbt-venv/bin/dbt build --no-use-colors --vars '{pageviews_enabled: false}'; fi"
        ),
        append_env=True,
    )
    export = BashOperator(
        task_id="export_marts",
        bash_command=f"{PYTHON} {SCRIPTS}/export_marts.py",
    )

    compact >> dbt_build >> export


warehouse_hourly()
