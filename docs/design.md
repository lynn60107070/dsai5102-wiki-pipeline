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

| Topic | Partitions | Key | Written by | Status |
|---|---|---|---|---|
| `wiki.raw` | 3 | wiki | producer | **live** |
| `edits.bot`, `edits.human` | 3 | wiki | processor | L7 |
| `edits.per_minute` | 1 | wiki | processor | L7 |
| `wiki.deadletter` | 1 | — | processor | L7 |

## 5. Lake layout (raw → clean, Hive-style partitions)

```
raw/edits/dt=YYYY-MM-DD/hour=HH/part-*-n<events>.jsonl.gz   ← live in MinIO (bucket `lake`) since 25 Oct
raw/pageviews/dt=YYYY-MM-DD/hour=HH/articles.parquet      ← S2 + L5 (articles with ≥ 10 views)
raw/pageviews/dt=YYYY-MM-DD/hour=HH/wiki_totals.parquet   ← S2 + L5 (all views, summed per wiki)
clean/edits/dt=YYYY-MM-DD/*.parquet                 ← L4/L9 (merged, one file per day)
```

The raw zone is kept exactly as received (ELT, schema-on-read, lecture 8): gzipped JSON lines, one object
per sink flush, with the event count in the object name. Parquet only appears in the clean zone. DuckDB
reads the raw zone straight from MinIO (`read_json('s3://lake/raw/edits/*/*/*.jsonl.gz', hive_partitioning=true)`,
tested 25 Oct: 0.7 s for ~70k events). Duplicates from at-least-once delivery are removed in dbt by event id.

**Why this MinIO image:** MinIO stopped publishing free official images in 2025 (Docker Hub and quay.io
both refuse the pull). We use `bitnamilegacy/minio:2025.7.23-debian-12-r5`, the last free MinIO build,
archived by Bitnami, pinned and multi-arch. Alternatives checked: SeaweedFS, RustFS, the `pgsty/minio`
community rebuild. MinIO won because it matches the course demo and is the best-tested S3 target for DuckDB.

## 6. Star schema (warehouse, DuckDB + dbt)

| Table | Grain | Key columns |
|---|---|---|
| `fact_edit` | one edit or page creation | edit_id, hour_key, wiki_id, article_key, editor_type, is_bot, is_minor, is_new_page, bytes_changed, is_revert (comment heuristic) |
| `fact_pageviews_hourly` | wiki × article × hour × access (articles with ≥ 10 views in that hour) | hour_key, wiki_id, article_key, access, views |
| `fact_wiki_views_hourly` | wiki × hour × access (all views) | hour_key, wiki_id, access, views, articles_viewed |
| `dim_wiki` | one wiki | wiki_id, language, project family, is_arabic (seed, S3) |
| `dim_article` | one wiki + title | article_key, wiki_id, title, namespace, first_seen |
| `dim_time` | one UTC hour | hour_key, date, hour, weekday, is_weekend |
| `dim_editor_type` | bot / registered / anonymous or temporary account | editor_type |

**Marts** (one per dashboard tile, exported to Postgres): `mart_edits_hourly`, `mart_bot_share`,
`mart_attention_vs_edits`, `mart_data_completeness`.

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
