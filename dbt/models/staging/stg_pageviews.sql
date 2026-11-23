{{ config(enabled=var("pageviews_enabled")) }}
-- Article-level pageviews (articles with >= 10 views in the hour), written by ingest/pageviews.py.
-- The dumps use the title '-' for requests whose article couldn't be determined: not a page, so it is
-- dropped (it made up 75 of the window's 24,400 "spikes", e.g. 188k views an hour on Chinese Wikipedia).

select
    hour_ts,
    dt,
    wiki                                as wiki_id,
    access,
    article                             as title,
    md5(wiki || ':' || replace(article, '_', ' '))  as article_key,
    views
from read_parquet('{{ var("pageviews_path") }}/*/*/articles.parquet', hive_partitioning = true)
where article <> '-'
