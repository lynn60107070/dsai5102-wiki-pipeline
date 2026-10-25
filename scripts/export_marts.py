"""Copy every table in the warehouse's `marts` schema into the Postgres `serving` database.

DuckDB computes, Postgres serves: DuckDB allows only one writer, so Metabase never opens the warehouse
file; it reads these copies instead, through a read-only user. Each table is replaced inside one
transaction, so Metabase never sees a half-copied table.

Run (inside Airflow, with dbt's Python): /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/export_marts.py
"""
import os

import duckdb

WAREHOUSE = os.environ.get("WAREHOUSE_PATH", "/opt/airflow/warehouse/wiki.duckdb")
SERVING = (f"host={os.environ.get('POSTGRES_HOST', 'postgres')} port=5432 dbname=serving "
           f"user=airflow password={os.environ['POSTGRES_PASSWORD']}")


def main():
    # An in-memory session that attaches the warehouse read-only and Postgres read-write, so the export
    # can never change the warehouse.
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{WAREHOUSE}' AS wh (READ_ONLY)")
    con.execute(f"ATTACH '{SERVING}' AS serving (TYPE postgres)")
    tables = [row[0] for row in con.execute(
        "select table_name from information_schema.tables "
        "where table_catalog = 'wh' and table_schema = 'marts' order by 1").fetchall()]
    for table in tables:
        con.execute("BEGIN")
        con.execute(f"DROP TABLE IF EXISTS serving.public.{table}")
        con.execute(f"CREATE TABLE serving.public.{table} AS SELECT * FROM wh.marts.{table}")
        con.execute("COMMIT")
        rows = con.execute(f"SELECT count(*) FROM serving.public.{table}").fetchone()[0]
        print(f"exported marts.{table} -> serving.public.{table} ({rows:,} rows)")


if __name__ == "__main__":
    main()
