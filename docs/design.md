# Design log

> Kept as written during the build. Where it differs, the report (`paper/`) and the README are current.

## 1. Problem statement

Wikipedia's volunteer patrollers have limited time: **where does editing pressure concentrate, how much
of it comes from bots, and are spikes in reader attention (pageviews) followed by edit surges?**

Audience: patrollers and wiki administrators deciding where to focus review effort.

## 2. Sources

| Source | Type | How often | Volume | Joins on |
|---|---|---|---|---|
| Wikimedia EventStreams `recentchange` | Stream (server-sent events) | Continuous, ~20 kept events/s (edits + new pages) | ~1.7M kept events/day | wiki, article, time |
| Pageview dumps (`dumps.wikimedia.org/other/pageviews`) | Batch files | Hourly, ~57 MB compressed | 6.4M rows/hour (5.25M Wikipedia); we keep ~193k (see §7) | wiki, article, hour |
| Sitematrix API (`action=sitematrix`) | Reference (batch, once) | Rarely changes | ~1,000 wikis | wiki |

All times are UTC.

## 3. Data flow

```
EventStreams ──► producer ──► wiki.raw ──► processor (L7) ──► edits.bot / edits.human / edits.per_minute
                                 │                        └─► wiki.deadletter (bad events)
                                 └──► raw-sink (consumer group `raw-archive`) ──► lake raw/edits/

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
minute in event time. **Dead-letter in practice (26 Oct):** 32 events out of ~2 million, all edits on
ruwiki that arrived with no `user` field at all (the user name was hidden/suppressed by Wikimedia), so
the editor type can't be determined; they are kept in `wiki.deadletter` with the reason instead of
entering the warehouse unclassified. The watermark is the slowest partition's newest event, so a partition racing
ahead during catch-up can't make the others look late (first version counted 205,814 events as late
during the backlog; fixed and tested). Minutes close 2 minutes after they end.

**Delivery guarantees:** every consumer commits offsets only after its output is written
(at-least-once). Measured on 25 Oct: after restarting the sinks mid-backlog, the clean zone held
381,066 duplicates and 0 missing events; `scripts/compact_clean_zone.py` merged each hour into one file
and removed all of them. Stream services now get 60 s to shut down (`stop_grace_period`) so they can
flush and commit. **Known limitation:** the per-minute counts live in memory, so the live tile dips
for the 2–3 minutes around a processor restart (the warehouse is unaffected).

**Real outage, 25 Oct 16:02–20:17 UTC (laptop asleep in a bag, no network): detected and fully
recovered.** On wake the producer resumed from its saved `Last-Event-ID` (this worked on 25 Oct; from 27 Oct Wikimedia ignored the header, so replays now use `?since=`, see below), and Wikimedia replayed the
missed ~4 hours at ~14× the normal rate (32–37k events/min for ~12 minutes); the sinks then caught up from
Kafka. Every hour 16:00–20:00 has normal volume (83–89k edits), so no data was lost. `check_freshness`
correctly failed in the 3 warehouse runs that executed right after waking (raw zone 258 minutes stale),
while compaction, dbt (all tests) and the export still succeeded in those runs.

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
  connection, the dashboard charts and its filters through Metabase's API, and is safe to run again
  (charts and filters defined in its CARDS and FILTERS are put back on every run).

## 5e. Resource limits (incident 26 Oct)

Turning on the pageview models made a dbt build read much more data. DuckDB defaults to 80% of RAM
(6 GB here) and all 20 CPU threads; on top of the 4.7 GB stack inside Docker's 8 GB it pushed the VM
into swap (1.8 of 2 GB used). Everything froze for ~7 minutes: dbt stalled, Docker's internal DNS timed
out, Airflow lost its database, and the 4 stream consumers missed their Kafka heartbeats, were
evicted from their groups and crashed on `CommitFailedError`. The restart policy brought them back and
at-least-once delivery meant **no data was lost** (no gaps in the status table). Fixes:
- DuckDB capped: `memory_limit 1500MB`, `threads 4` in dbt (spills to disk beyond that); 1 GB / 2
  threads in the helper scripts. A full build now takes ~55 s instead of ~7 s while the backfill runs.
  (`temp_directory` must not be set in the dbt profile: dbt re-applies settings on every connection
  and DuckDB refuses to change it once used; that broke one build.)
- Consumers: 30 s Kafka session timeout (default 10 s), and `safe_commit` logs a failed commit after a
  rebalance instead of crashing (unit-tested).
- Memory trimmed: Kafka heap 512 MB (default 1 GB), Airflow webserver 2 workers (default 4), pageview
  backfill one hour at a time. Stack: 4.7 GB → 3.8 GB.

## 5h. Backfill that actually works (28 Oct)

Two laptop sleeps fell inside the analysis window (28 Oct 12:11–12:40 and 13:06–14:18 UTC). Investigating
why they weren't recovered found the real root causes of every "backfill ended early" since 27 Oct:
1. **Wikimedia ignored the resume position sent as a `Last-Event-ID` header** and simply started from
   live, so a backfill immediately "reached the end" (102 events recovered for a 30-minute gap). The
   `?since=<time>` query parameter works and is fast (34,948 events covering 18 minutes in 12 s).
2. **Canary test events** carry arbitrary timestamps; a replay *starts* with some stamped hours ahead.
3. A sleep that the connection survives isn't a "drop", so no gap was queued; and a gap's end was
   recorded at the first failed reconnect, not when data resumed.
Fix: positions and gaps are **times** (`?since=`), canaries are ignored, and a gap is any **jump of more
than 3 s between consecutive events** in the live stream (normally milliseconds), queued with the exact
start and end. Result: the 12:10–12:41 gap recovered 27,171 events in 16 s.

## 5g. Producer: live + background backfill (27 Oct)

**Incident:** the laptop was offline 12:22–14:37 UTC. On reconnect the producer resumed from its saved
position as before, but Wikimedia served the resumed ("historical") connection at ~5% speed (~400
events/min instead of ~3,000), even when the saved position was only seconds old, while fresh live
connections were fast. With one connection, a slow resume holds back live data too, so the gap kept
growing. (On 25 Oct the same kind of resume ran at ~14× speed; it depends on Wikimedia's side.)

**Fix:** two connections. The live connection always starts from *now*; when it (re)connects after
having had a position, the stretch from that position to the reconnect time is queued as a gap
(`/state/pending_gaps.json`, so it survives restarts), and a background thread replays gaps from the
saved id until it reaches the reconnect time. Live data can no longer be held back; backfills run at
whatever speed Wikimedia serves them. Overlaps only create duplicates (removed by compaction and dbt).
Tested on deploy: live at full speed immediately, the restart gap backfilled in 6 s (4,521 events);
the 12:20–14:57 gap was queued by hand and backfilled in the background.

## 5f. Disk incident (27 Oct): DuckDB spill grew Docker's disk to 421 GB

With the 1.5 GB memory cap from §5e and dbt running **4 models in parallel in one DuckDB process**,
the marts shared that budget: each query got a fraction of it and spilled to disk. Some builds wrote
tens of GB of temp files (`wiki.duckdb.tmp` reached 47 GB, one model took 510 s). DuckDB deletes them
afterwards, but Docker Desktop's virtual disk (`docker_data.vhdx`) never shrinks on its own, so over a
day it grew to **421 GB** and the laptop had 15 GB free. Measured: alone, each mart runs in 0.5–1.7 s.
Fixes:
- **dbt `threads: 1`**: models run one at a time with the full memory budget. Full build 30 s, no spill.
- **`max_temp_directory_size: 8GB`**: a runaway query now fails (the quality gate keeps the last good
  dashboard) instead of filling the disk. DuckDB's default is 90% of the free disk. Tested: it fired.
- `mart_attention_vs_edits` computes the 24 h window only for articles that ever reach 1,000 views
  (3,340 of millions); verified identical output to the old logic on the same data (16,201 rows, 0
  differences), 1.7 s instead of 6.4 s alone.
- `dim_article` is incremental (adds only new articles) instead of regrouping all history every hour.
- The warehouse DAG was paused during the fix; the stream kept collecting (no gaps). Reclaiming the
  disk space required trimming, then compacting the vhdx with Docker stopped (README → Troubleshooting):
  424.7 GB → 31.6 GB, C: free 11.7 GB → 406.6 GB. Docker was off ~5 min; the producer replayed the gap (no gaps).

## 5d. Reproducibility: one command (L10, 25 Oct)

- **`.env` is optional.** Every variable in `docker-compose.yml` has a working default (the
  `.env.example` values); `.env` only overrides them. A fresh clone needs only `docker compose up -d --build`.
- **`bootstrap` service** (Airflow image, one-off, last to start, safe on every `up`):
  1. `scripts/load_sample.py`: loads `sample/` (6 real hours of the clean zone, 4 Nov 06:00–12:00 UTC, 379,942 edits, plus 30 hours of pageviews,
     no user names) **only if the lake's clean zone is empty**, so sample rows can never mix with real data;
  2. unpauses `warehouse_hourly` and triggers it **through Airflow** (not by running dbt directly), so it
     can't collide with a scheduled run over DuckDB's single writer; waits for `export_marts`;
  3. `scripts/metabase_setup.py`: admin account, connection, charts, dashboard.
- `scripts/make_sample.py` regenerates the sample from compacted hours (planned: a day inside the
  analysis window, once it exists).
- **Fresh-clone test (25 Oct)**, isolated project with other ports, no `.env`: found two real problems,
  both fixed and re-tested: (1) sample file paths were over Windows' 260-character limit, so the clone
  failed on checkout (paths shortened from ~85 to 62 characters); (2) the live tile errored when Metabase
  was set up before the stream had created its table (now created by `postgres/init`).

## 6. Star schema (warehouse, DuckDB + dbt), built 25 Oct

Layers: `staging` (views over the lake, no copies) → `core` (facts + dimensions) → `marts` (one per
dashboard tile, copied to Postgres). Natural keys instead of surrogate integers: they are stable across
full rebuilds and readable in the dashboard; `article_key` is `md5(wiki_id:title)` because titles are
long and only unique within a wiki.

| Table | Grain | Keys → dimension | Built as |
|---|---|---|---|
| `fact_edit` | one edit or page creation (`event_id`) | `hour_ts` → dim_time, `wiki_id` → dim_wiki, `article_key` → dim_article, `editor_type` → dim_editor_type | incremental (last 3 days reloaded, `delete+insert` by `event_id`, so duplicates can't survive) |
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
| `mart_attention_vs_edits` | article-hour with ≥ 1,000 views | attention spikes (≥ 5× previous-24 h average, full baseline only) vs edits in the same + next 2 hours; `is_content_page` marks articles |

**Tests: 35** data tests, 28 with pageviews off (`dbt build` reports PASS=51 and 39, counting models and seeds; unique and not-null keys, accepted values, and a
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

**Which hour a file holds (verified 26 Oct):** `pageviews-20261024-120000.gz` matches the official
Pageview API's **11:00–12:00** numbers exactly for English Wikipedia (desktop 2,077,400; desktop +
mobile 6,708,289), so a file named HH holds the hour *ending* at HH, and `pageviews_hourly` fetches
file H+1 for hour H. The dumps count **user traffic only** (they equal the API's `user` agent, not
`all-agents`), and `.m` = mobile web + mobile apps. Licence: CC0 (dumps readme). One file: 6.4M rows
read, 5.25M Wikipedia, 193k kept, 754 wiki totals, ~2.2 MB of Parquet, 17 s on a laptop.

**Wiki names (S3):** `ingest/wiki_names.py` reads the sitematrix API (1,073 wikis: 949 language wikis
+ 124 special ones like Commons) into the seed `dim_wiki_names`; `dim_wiki` prefers it over the values
parsed from server names (e.g. `zh-min-nan` → language code `nan`, "Abstract Wikipedia" → not a
language Wikipedia).

**Non-article pages (23 Nov):** the pageview dumps count every page, so main pages, `Special:Search` and
`Wikipedia:` project pages sat in `mart_attention_vs_edits` with huge views and almost no edits. They were
1,746 of the 2,326 non-spike hours above 20,000 views and made spikes look 3.1× as edited (finding 5
reversed once they were left out). `ingest/page_namespaces.py` asks each Wikipedia's siteinfo API for its
namespace names (local, canonical, aliases) and main page → seed `wiki_non_article_titles` (21,246 titles,
364 Wikipedias); the mart's `is_content_page` is false when a title is a main page or starts with
`<namespace>:`. Titles like *Ramayana: Part 1* stay articles, since "Ramayana" is no namespace.

Domain codes: no suffix = desktop Wikipedia, `.m` = mobile Wikipedia. Exceptions: `commons`, `meta`,
`species`, `incubator`, `wikidata`, `foundation` are not Wikipedias even without a suffix, and `commons.m`
means Wikimedia Commons, not mobile.
