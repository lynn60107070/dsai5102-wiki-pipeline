# Architecture explanation (Version 1)

Companion to `wikipedia_pipeline_architecture_v1.drawio` (exports: `.svg`, `.png`, `.pdf`). Everything here
was checked against the code on `main` at commit `429ce89` (11 Nov 2026). Where the code and older notes
disagree, this file follows the code.

## Overall architecture

The pipeline turns Wikimedia's public data into a dashboard for one question: where editing pressure
concentrates, how much of it comes from bots, and whether spikes in reader attention are followed by edit
surges. Three paths bring data in, and they differ in how often they run:

| Path | Source | Runs | Run by |
|---|---|---|---|
| Real-time stream | EventStreams `recentchange` | Continuously | Docker Compose services |
| Hourly batch | Pageview dump files | Every hour | Airflow DAG `pageviews_hourly` |
| Reference | Sitematrix API | By hand, rarely | `python -m ingest.wiki_names` |

The stream and batch paths meet in the MinIO data lake. DuckDB, driven by dbt, models them into a star schema.
The finished tables are copied to PostgreSQL, and Metabase reads them there. All of it runs on one machine in
Docker Compose (14 services); the diagram shows the services that handle data, not all 14.

## Streaming pipeline

1. `producer` reads the `recentchange` feed over HTTPS (server-sent events). It keeps edits and page creations
   only (about 20 events/s), drops Wikimedia's canary test events, and writes to Kafka topic `wiki.raw`, keyed
   by wiki. A second connection backfills any gap with `?since=<time>`, so a slow replay never delays live data.
2. `raw-sink` (consumer group `raw-archive`) writes `wiki.raw` unchanged to `raw/edits/` every 5 minutes, or sooner once 50,000 events are buffered.
3. `processor` (group `processor`) reads the same topic. It validates each event, drops the user name and edit
   summary (keeping an MD5 editor key and the editor type), flags reverts, and routes the event to `edits.bot` or
   `edits.human`. It also counts edits per wiki and minute into `edits.per_minute`. Invalid events go to
   `wiki.deadletter`, which has no consumer and is inspected by hand.
4. `clean-sink` (2 replicas sharing group `clean-archive`) writes the bot and human topics to `clean/edits/` as
   Parquet on the same rule (every 5 minutes or 50,000 events).
5. `live-stats` (group `live-stats`) writes the per-minute counts straight into PostgreSQL. This path skips the
   lake and the warehouse, which is why the live tile can be near real time: a minute closes 2 minutes after it
   ends and appears about 3 minutes after it ends.

Every consumer commits its offsets only after its output is written (at-least-once delivery), so a crash can
repeat events but not lose them. Airflow does not run or monitor any of these services directly; its only link
to the stream is `check_freshness`, which looks at how new the lake's files are.

## Batch pipeline

`pageviews_hourly` runs once per UTC hour, one hour at a time, with catchup:

`wait_for_file` → `fetch` → `upload` → `cleanup`

- `wait_for_file` sends a HEAD request until Wikimedia has published the file.
- `fetch` downloads it, keeps Wikipedia rows and writes two Parquet files: `articles.parquet` (articles with at
  least 10 views) and `wiki_totals.parquet` (all views, summed per wiki).
- `upload` puts both in `raw/pageviews/` and updates the Airflow Dataset `s3://lake/raw/pageviews`.
- `cleanup` removes the local copies, even if an earlier task failed.

Kafka is not involved in this path.

`warehouse_hourly` refreshes the warehouse. It runs hourly at :20 **or** when that Dataset is updated, with one
run at a time because DuckDB allows a single writer:

`compact_clean_zone` → `dbt_build` → `export_marts`, plus an independent `check_freshness`.

`check_freshness` only alerts: it turns red if the stream stops writing, and never blocks the refresh. Both DAGs
run on the Airflow scheduler with the LocalExecutor.

## Reference data

`ingest/wiki_names.py` reads the Sitematrix API and writes `dbt/seeds/dim_wiki_names.csv`, which is committed to
Git. Every `dbt build` loads that file as a seed. This path does not go through Airflow, Kafka or MinIO.

## Data lake zones

MinIO holds one bucket, `lake`, partitioned by `dt=YYYY-MM-DD/hour=HH`.

| Zone | Path | Format | Written by | Read by dbt? |
|---|---|---|---|---|
| Raw | `raw/edits/` | Gzipped JSON lines, as received from Kafka | `raw-sink` | No (archive) |
| Raw | `raw/pageviews/` | Parquet, filtered before upload | `upload` task | Yes |
| Clean | `clean/edits/` | Parquet, validated, no user names | `clean-sink`, rewritten by `compact_clean_zone` | Yes |

Two details matter when reading the diagram:

- **The clean zone is not built from the raw zone.** Both are filled separately from Kafka, so no arrow joins them.
- **`raw/pageviews/` is not "as published".** Pageviews are filtered and converted to Parquet before they reach the
  lake. Only `raw/edits/` is an untouched copy, and it still holds user names.

`compact_clean_zone` merges each finished hour into one file and removes duplicates by `event_id`. On a fresh
install, the `bootstrap` service also loads a 6-hour sample into `clean/edits/`, but only if that zone is empty.

## Analytical modelling

`dbt_build` runs `dbt build` inside the Airflow scheduler container, using its own Python environment. dbt-duckdb
drives DuckDB in-process, with the warehouse stored as one file, `wiki.duckdb`. DuckDB reads the lake over S3 with
its `httpfs` extension. There are three layers and no intermediate layer:

| Layer | Models | Built as |
|---|---|---|
| `staging` | `stg_edits`, `stg_pageviews`, `stg_wiki_views` | Views over the lake |
| `core` | `fact_edit`, `fact_pageviews_hourly`, `fact_wiki_views_hourly`; `dim_time`, `dim_wiki`, `dim_article`, `dim_editor_type`; seed `dim_wiki_names` | Tables; facts and `dim_article` are incremental |
| `marts` | `mart_edits_hourly`, `mart_wiki_activity`, `mart_data_completeness`, `mart_attention_vs_edits` | Tables |

`dbt build` runs the data tests too: 34 generic tests defined in the model YAML files. If one fails, the task
fails and `export_marts` does not run, so the dashboard keeps the last good data.

## Dashboard serving

- **Warehouse tables:** `export_marts` opens an in-memory DuckDB session with the warehouse attached read-only and
  PostgreSQL attached through DuckDB's `postgres` extension. It replaces each mart in the `serving` database inside
  one transaction, so Metabase never sees a half-copied table.
- **Live table:** `live-stats` adds rows to `live_edits_per_minute` in the same database and keeps the last 48 hours.
- **Metabase** reads `serving` as `metabase_ro`, which can only SELECT. Its dashboard, *Wikipedia edits*, currently
  has two tiles: the live tile (`live_edits_per_minute`) and edits per hour, bots vs humans (`mart_edits_hourly`).

DuckDB is the warehouse; PostgreSQL only serves copies. The same PostgreSQL server also holds the `airflow` and
`metabase` application databases, which contain no pipeline data, so the diagram draws no data flow to them.

## Where each box lives

| Diagram box | Implementation |
|---|---|
| `producer`, `processor`, `raw-sink`, `clean-sink`, `live-stats` | `stream/producer.py`, `processor.py` + `clean.py`, `raw_sink.py`, `clean_sink.py`, `live_stats.py` |
| Kafka topics | `kafka-init` command in `docker-compose.yml` |
| MinIO paths | `stream/lake.py`, `stream/raw_sink.py`, `stream/clean_sink.py`, `airflow/dags/pageviews_hourly.py` |
| Airflow DAGs and tasks | `airflow/dags/pageviews_hourly.py`, `airflow/dags/warehouse_hourly.py` |
| `fetch` logic | `ingest/pageviews.py` |
| Sitematrix script and seed | `ingest/wiki_names.py`, `dbt/seeds/dim_wiki_names.csv` |
| dbt layers | `dbt/models/staging`, `dbt/models/core`, `dbt/models/marts`, `dbt/profiles.yml` |
| `compact_clean_zone`, `check_freshness`, `export_marts` | `scripts/compact_clean_zone.py`, `scripts/check_freshness.py`, `scripts/export_marts.py` |
| PostgreSQL databases and role | `postgres/init/01_serving.sh` |
| Metabase connection and tiles | `scripts/metabase_setup.py` |

## Editing the diagram

Open the `.drawio` file in draw.io (desktop or diagrams.net), edit it, then use **File → Export as** for SVG, PNG
(300% zoom) and PDF, keeping the same file names. Labels are plain text rather than HTML, so the SVG also works
outside a browser.
