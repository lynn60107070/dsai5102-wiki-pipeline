#!/bin/bash
# Runs once, when the Postgres volume is first created.
#   serving   the dashboard tables (written by scripts/export_marts.py)
#   metabase  Metabase's own application database (its questions and dashboards)
#   metabase_ro  read-only login Metabase uses for `serving`: it can SELECT and nothing else
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE serving;
    CREATE DATABASE metabase;
    CREATE ROLE metabase_ro LOGIN PASSWORD '${METABASE_DB_PASSWORD}';
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname serving <<-EOSQL
    REVOKE ALL ON DATABASE serving FROM PUBLIC;
    GRANT CONNECT ON DATABASE serving TO metabase_ro;
    GRANT USAGE ON SCHEMA public TO metabase_ro;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_ro;
    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA public GRANT SELECT ON TABLES TO metabase_ro;

    -- Filled by the live-stats stream service; created here too, so the dashboard's live tile shows
    -- "no results" instead of an error if Metabase is set up before the stream has written anything.
    CREATE TABLE IF NOT EXISTS live_edits_per_minute (
        minute_ts   timestamptz NOT NULL,
        wiki_id     text        NOT NULL,
        bot_edits   integer     NOT NULL,
        human_edits integer     NOT NULL,
        PRIMARY KEY (minute_ts, wiki_id)
    );
EOSQL
