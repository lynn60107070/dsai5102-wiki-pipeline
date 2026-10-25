{{ config(enabled=var("pageviews_enabled")) }}
-- Article-level pageviews (articles with >= 10 views in the hour), written by ingest/pageviews.py (S2).

select
    hour_ts,
    dt,
    wiki                                as wiki_id,
    access,
    article                             as title,
    md5(wiki || ':' || replace(article, '_', ' '))  as article_key,
    views
from read_parquet('{{ var("pageviews_path") }}/*/*/articles.parquet', hive_partitioning = true)
