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
| Pageview dumps (`dumps.wikimedia.org/other/pageviews`) | Batch files | Hourly, ~60 MB compressed | All wikis, all articles | wiki, article, hour |
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
raw/edits/dt=YYYY-MM-DD/hour=HH/part-*.jsonl.gz     ← live now (Docker volume raw_data; MinIO in L4)
raw/pageviews/dt=YYYY-MM-DD/hour=HH/*.parquet       ← S2 + L5
clean/edits/dt=YYYY-MM-DD/*.parquet                 ← L4/L9 (merged, one file per day)
```

The raw zone is kept exactly as received (ELT, schema-on-read, lecture 8). Duplicates from at-least-once
delivery are removed in dbt by event id.

## 6. Star schema (warehouse, DuckDB + dbt)

| Table | Grain | Key columns |
|---|---|---|
| `fact_edit` | one edit or page creation | edit_id, hour_key, wiki_id, article_key, editor_type, is_bot, is_minor, is_new_page, bytes_changed, is_revert (comment heuristic) |
| `fact_pageviews_hourly` | wiki × article × hour × access | hour_key, wiki_id, article_key, access, views |
| `dim_wiki` | one wiki | wiki_id, language, project family, is_arabic (seed, S3) |
| `dim_article` | one wiki + title | article_key, wiki_id, title, namespace, first_seen |
| `dim_time` | one UTC hour | hour_key, date, hour, weekday, is_weekend |
| `dim_editor_type` | bot / registered / anonymous or temporary account | editor_type |

**Marts** (one per dashboard tile, exported to Postgres): `mart_edits_hourly`, `mart_bot_share`,
`mart_attention_vs_edits`, `mart_data_completeness`.

**(open)** Which wikis to keep in `fact_pageviews_hourly`: all Wikipedias, or only those with edits in the stream.
