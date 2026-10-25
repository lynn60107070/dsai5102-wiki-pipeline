{{ config(enabled=var("pageviews_enabled")) }}
-- Total views per wiki and hour (all articles, no threshold), written by ingest/pageviews.py (S2).

select
    hour_ts,
    dt,
    wiki                                as wiki_id,
    access,
    views,
    articles_viewed
from read_parquet('{{ var("pageviews_path") }}/*/*/wiki_totals.parquet', hive_partitioning = true)
