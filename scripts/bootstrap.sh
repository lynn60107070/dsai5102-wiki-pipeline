#!/usr/bin/env bash
# One-off setup at the end of `docker compose up` (service `bootstrap`). Safe to run again:
#   1. sample data into the lake, only if the lake is empty (fresh clone)
#   2. unpause warehouse_hourly and run it once through Airflow (so it can never clash with a
#      scheduled run over DuckDB's single writer), then wait for the export to finish
#   3. Metabase: admin account, connection to `serving`, charts, dashboard
set -euo pipefail
SCRIPTS=/opt/airflow/project/scripts
PY=/opt/airflow/dbt-venv/bin/python
quiet() { "$@" 2>/dev/null | grep -vE "INFO -|plugins.py|Warning|warn" || true; }

"$PY" "$SCRIPTS/load_sample.py"

# Wait until the scheduler has registered the DAG in Airflow's database AND it is unpaused. (Checking
# `airflow dags list` is not enough: it reads the DAG files, so it can see the DAG before the database
# row exists; an unpause then does nothing and a triggered run stays queued forever.)
echo "waiting for Airflow to register warehouse_hourly..."
for _ in $(seq 1 60); do
    airflow dags unpause warehouse_hourly >/dev/null 2>&1 || true
    airflow dags details warehouse_hourly -o plain 2>/dev/null | grep -qE "^is_paused +False" && break
    sleep 5
done
airflow dags details warehouse_hourly -o plain 2>/dev/null | grep -qE "^is_paused +False" \
    || { echo "warehouse_hourly was not registered/unpaused after 5 min: check the Airflow UI"; exit 1; }

run_id="bootstrap__$(date -u +%Y%m%dT%H%M%S)"
quiet airflow dags trigger warehouse_hourly -r "$run_id"
echo "building the warehouse (Airflow run $run_id)..."
for _ in $(seq 1 60); do
    state=$(airflow tasks states-for-dag-run warehouse_hourly "$run_id" -o plain 2>/dev/null \
            | awk '$3 == "export_marts" {print $4}')
    case "$state" in
        success) echo "warehouse built and exported"; break ;;
        failed|upstream_failed) echo "warehouse build failed: see run $run_id in the Airflow UI"; exit 1 ;;
    esac
    sleep 10
done
[ "${state:-}" = "success" ] || { echo "warehouse build still running after 10 min: check the Airflow UI"; exit 1; }

python "$SCRIPTS/metabase_setup.py"
echo "ready: dashboard http://localhost:3000 · Airflow http://localhost:8080 · MinIO http://localhost:9001"
