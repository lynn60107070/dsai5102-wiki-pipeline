# Design note (draft v0, to be agreed at CP1, task L3)

Sumaiya draws the architecture diagram (S6) from this file. Anything marked **(open)** is still to decide.

## 1. Problem statement

Wikipedia's volunteer patrollers have limited time: **where does editing pressure concentrate, how much
of it comes from bots, and do spikes in reader attention (pageviews) cause edit surges?**

Audience: patrollers and wiki administrators deciding where to focus review effort.

## 2. Sources

| Source | Type | How often | Volume | Joins on |
|---|---|---|---|---|
| Wikimedia EventStreams `recentchange` | Stream (server-sent events) | Continuous, ~29 events/s (~14/s after keeping edits + new pages) | ~1.2M events/day | wiki, article, time |
| Pageview dumps (`dumps.wikimedia.org/other/pageviews`) | Batch files | Hourly, ~57 MB compressed | 6.4M rows/hour (5.25M Wikipedia); we keep ~193k (see §7) | wiki, article, hour |
| Sitematrix API (`action=sitematrix`) | Reference (batch, once) | Rarely changes | ~1,000 wikis | wiki |

All times are UTC.

## 3. Data flow

```
EventStreams ──► producer ──► wiki.raw ──► processor (L7) ──► edits.bot / edits.human / edits.per_minute
                                 │                        └─► wiki.deadletter (bad events)
                                 └──► raw-sink (consumer group, 2 replicas in L7) ──► lake raw/edits/

Pageview dumps ──► Airflow DAG (L5/L9): download → filter (S2) → lake raw/pageviews/
                                          → dbt build (DuckDB) → export marts → Postgres ──► Metabase
Sitematrix ──► dbt seed dim_wiki (S3)
```

Stream services run all the time as Compose services. Airflow only runs the scheduled batch work.

## 4. Kafka topics

| Topic | Partitions | Key | Written by | Read by (consumer group) | Retention |
|---|---|---|---|---|---|
| `wiki.raw` | 3 | wiki | producer | raw-sink (`raw-archive`), processor (`processor`) | 3 days |
| `edits.bot`, `edits.human` | 3 each | wiki | processor | clean-sink ×2 (`clean-archive`) | 3 days |
| `edits.per_minute` | 1 | wiki | processor | live-stats (`live-stats`) | 1 day |
| `wiki.deadletter` | 1 | none | processor | (inspected by hand) | 7 days |

All live since 25 Oct. Topics are created explicitly by `kafka-init` (auto-creation is off).

**Processor** (`stream/processor.py`, logic in `stream/clean.py`, unit-tested): validates each event
(dead-letter with the reason if it fails), drops the user name and edit summary (keeps an MD5 key and
the editor type), flags reverts, routes to `edits.bot` / `edits.human`, and counts edits per wiki and
minute in event time. The watermark is the slowest partition's newest event, so a partition racing
ahead during catch-up can't make the others look late (first version counted 205,814 events as late
during the backlog; fixed and tested). Minutes close 2 minutes after they end.

**Delivery guarantees:** every consumer commits offsets only after its output is written
(at-least-once). Measured on 25 Oct: after restarting the sinks mid-backlog, the clean zone held
381,066 duplicates and 0 missing events; `scripts/compact_clean_zone.py` merged each hour into one file
and removed all of them. Stream services now get 60 s to shut down (`stop_grace_period`) so they can
flush and commit. **Known limitation:** the per-minute counts live in memory, so the live tile dips
for the 2–3 minutes around a processor restart (the warehouse is unaffected).

## 5. Lake layout (raw → clean, Hive-style partitions)

```
raw/edits/dt=YYYY-MM-DD/hour=HH/part-*-n<events>.jsonl.gz   ← live in MinIO (bucket `lake`) since 25 Oct
raw/pageviews/dt=YYYY-MM-DD/hour=HH/articles.parquet      ← S2 + L5 (articles with ≥ 10 views)
raw/pageviews/dt=YYYY-MM-DD/hour=HH/wiki_totals.parquet   ← S2 + L5 (all views, summed per wiki)
clean/edits/dt=YYYY-MM-DD/hour=HH/part-*-n<events>.parquet   ← clean-sink ×2 (no user names), live since 25 Oct;
                                                               compacted to one file per hour, duplicates removed
```

The raw zone is kept exactly as received (ELT, schema-on-read, lecture 8): gzipped JSON lines, one object
per sink flush, with the event count in the object name. Parquet only appears in the clean zone. DuckDB
reads the raw zone straight from MinIO (`read_json('s3://lake/raw/edits/*/*/*.jsonl.gz', hive_partitioning=true)`,
tested 25 Oct: 0.7 s for ~70k events). Duplicates from at-least-once delivery are removed in dbt by event id.

**Why this MinIO image:** MinIO stopped publishing free official images in 2025 (Docker Hub and quay.io
both refuse the pull). We use `bitnamilegacy/minio:2025.7.23-debian-12-r5`, the last free MinIO build,
archived by Bitnami, pinned and multi-arch. Alternatives checked: SeaweedFS, RustFS, the `pgsty/minio`
community rebuild. MinIO won because it matches the course demo and is the best-tested S3 target for DuckDB.

## 5b. Orchestration (Airflow)

`apache/airflow:2.11.2-python3.12`: the newest Airflow 2 release, the same major version as the course
lab (2.10.2), so the UI and DAG concepts match what was taught. LocalExecutor instead of the lab's
CeleryExecutor: one machine needs no Redis or Celery workers (about half the memory). Postgres 16 holds
Airflow's metadata; MinIO is an Airflow connection (`minio_lake`, defined by environment variable, so no
secrets in the repo or the UI). The official image already includes the Amazon provider, pyarrow and
requests, so no custom image is needed until dbt arrives (L6).

DAG `pageviews_hourly`: `wait_for_file` (sensor, reschedule mode) → `fetch` (S2) → `upload` → `cleanup`
(runs even after a failure). Hourly from 25 Oct with catchup, at most 2 runs at once, 2 retries.
Idempotent: a re-run overwrites the same two objects. `upload` updates the Airflow Dataset
`s3://lake/raw/pageviews`.

DAG `warehouse_hourly` (L9): `compact_clean_zone` → `dbt_build` → `export_marts`, plus an independent
`check_freshness` that turns red when the stream stops writing (monitoring, not a blocker).
- **Data-aware scheduling:** `DatasetOrTimeSchedule` = every hour at :20 **or** whenever the pageviews
  Dataset is updated, so the two DAGs coordinate without either one triggering the other directly.
- **Quality gate:** `dbt build` runs all data tests; if one fails, the task fails and `export_marts`
  never runs, so Metabase keeps showing the last good data instead of wrong numbers.
- **One run at a time** (`max_active_runs=1`), because DuckDB allows a single writer. No catchup: the
  warehouse is rebuilt from the lake each time, so only the latest run matters.
- Measured 25 Oct: a full run takes ~20 s (dbt build ~9 s).

The stream services are **not** in Airflow: they run continuously as Compose services; Airflow only
schedules batch work.

## 5c. Warehouse, serving and dashboard (thin slice, 25 Oct)

- **dbt runs inside the Airflow image**, in its own virtual environment (`/opt/airflow/dbt-venv`,
  dbt-core 1.12.6 + dbt-duckdb 1.11.0 + DuckDB 2.0.0): dbt and Airflow pin different versions of shared
  libraries. Airflow will run `dbt build` as a DAG task (L9).
- **Warehouse:** one DuckDB file on the `warehouse` volume. Only dbt writes to it.
- **Schema contract at the stream:** the processor validates every event and the clean sink writes
  Parquet with a fixed schema, so a changed field in the stream cannot silently change the warehouse.
  (The L6 thin slice read the raw JSON with an explicit column list; since L8, dbt reads the clean
  Parquet zone instead: no JSON parsing, and the raw zone stays as the untouched archive.)
- **Privacy:** user names are never stored in the warehouse, only an MD5 hash (to count distinct
  editors) and the editor type (bot / registered / anonymous).
- **DuckDB computes, Postgres serves:** `scripts/export_marts.py` copies every `marts.*` table into the
  Postgres database `serving` (in-memory DuckDB session: warehouse attached read-only, Postgres
  read-write; each table replaced inside one transaction).
- **Least privilege:** Metabase reads `serving` as `metabase_ro`, which can only SELECT (tested: DELETE
  is denied). Metabase's own settings live in the Postgres database `metabase`, not in a local file.
- **Reproducible dashboard setup:** `scripts/metabase_setup.py` creates the admin account, the
  connection and the first chart through Metabase's API, and is safe to run again.

## 6. Star schema (warehouse, DuckDB + dbt), built 25 Oct

Layers: `staging` (views over the lake, no copies) → `core` (facts + dimensions) → `marts` (one per
dashboard tile, copied to Postgres). Natural keys instead of surrogate integers: they are stable across
full rebuilds and readable in the dashboard; `article_key` is `md5(wiki_id:title)` because titles are
long and only unique within a wiki.

| Table | Grain | Keys → dimension | Built as |
|---|---|---|---|
| `fact_edit` | one edit or page creation (`event_id`) | `hour_ts` → dim_time, `wiki_id` → dim_wiki, `article_key` → dim_article, `editor_type` → dim_editor_type | incremental (last day reloaded, `delete+insert` by `event_id`, so duplicates can't survive) |
| `fact_pageviews_hourly` | hour × wiki × article × access (≥ 10 views) | `wiki_id`, `article_key` | incremental, switched on by `pageviews_enabled` |
| `fact_wiki_views_hourly` | hour × wiki × access (all views) | `wiki_id` | incremental, switched on by `pageviews_enabled` |
| `dim_time` | one UTC hour, continuous from the first hour of data | `hour_ts` | table; flags `is_weekend`, `in_analysis_window` |
| `dim_wiki` | one wiki | `wiki_id` | parsed from the server name (project, language), enriched by the S3 seed `dim_wiki_names` |
| `dim_article` | one article | `article_key` | from edits (+ pageviews when enabled) |
| `dim_editor_type` | bot / registered / anonymous | `editor_type` | static |

| Mart | Grain | Tile |
|---|---|---|
| `mart_edits_hourly` | hour × wiki × editor type | edits over time, bots vs humans |
| `mart_wiki_activity` | day × wiki (counts only; shares computed after summing) | bot share by wiki, top languages, reverts |
| `mart_data_completeness` | finished hour | gaps (`is_gap`) and partial hours (< 50% of median) |
| `mart_attention_vs_edits` | article-hour with ≥ 1,000 views | attention spikes (≥ 5× previous-24 h average, full baseline only) vs edits in the same + next 2 hours |

**Tests: 38** with pageviews off, **49** with them on (unique and not-null keys, accepted values, and a
relationship test from every fact key to its dimension). Measured 25 Oct: full build 19 s, incremental
`fact_edit` 1.3 s instead of 6.5 s; 630,405 edits, 0 duplicates; 396 wikis, 482,975 articles.
The pageview models were tested on a fixture following the S2 contract (throwaway warehouse, deleted
afterwards); it caught one bug: without a full 24-hour baseline everything looked like a spike.

**Finding:** Wikidata + Commons are ~48% of all edits, so `dim_wiki.project_family` / `is_wikipedia`
matter for every language question.

## 7. Decision: which pageviews we keep (decided 25 Oct, measured on the 24 Oct 12:00 UTC file)

| Keep article rows with… | Rows / hour | Share of views | Edited articles (one stream hour) found |
|---|---|---|---|
| any views (all Wikipedia) | 5.25M | 100% | 2,741 / 12,543 |
| ≥ 2 views | 1.85M | 78% | 1,845 |
| ≥ 5 views | 498k | 55% | 1,048 |
| **≥ 10 views** | **193k** | **43%** | 626 |

Keeping everything is ~126M rows/day, too much for a laptop warehouse. Most edited articles have almost no
readers, so the attention question is answered **from the attention side**: find the articles readers
flock to, then check whether their edits surge. So per hour we keep (1) article rows with **≥ 10 views**
(~4.6M rows/day, every real spike is in there; below 10 counts as "low attention") and (2) **per-wiki
totals of all views**, so attention by language loses nothing. Both are written by `ingest/pageviews.py`.

Domain codes: no suffix = desktop Wikipedia, `.m` = mobile Wikipedia. Exceptions: `commons`, `meta`,
`species`, `incubator`, `wikidata`, `foundation` are not Wikipedias even without a suffix, and `commons.m`
means Wikimedia Commons, not mobile.
