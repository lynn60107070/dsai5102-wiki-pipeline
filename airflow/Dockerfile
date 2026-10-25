# Official Airflow image + dbt in its own virtual environment.
# dbt and Airflow pin different versions of shared libraries, so installing dbt into Airflow's own
# environment would break one of them; the DAG calls dbt from /opt/airflow/dbt-venv instead.
FROM apache/airflow:2.11.2-python3.12

COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && /opt/airflow/dbt-venv/bin/python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs; INSTALL postgres')" \
 && mkdir -p /opt/airflow/warehouse
# The mkdir runs as the airflow user, so the `warehouse` volume mounted there is writable by Airflow.
# DuckDB extensions are downloaded at build time, so tasks never need to fetch them at run time.
