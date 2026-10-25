{{
    config(
        enabled = var("pageviews_enabled"),
        materialized = 'incremental',
        unique_key = ['hour_ts', 'wiki_id', 'access'],
        incremental_strategy = 'delete+insert'
    )
}}
-- Fact: total views per wiki, hour and access type, over all articles (grain: hour_ts x wiki_id x access).

select hour_ts, wiki_id, access, sum(views) as views, sum(articles_viewed) as articles_viewed
from {{ ref('stg_wiki_views') }}
where dt >= date '{{ var("data_start") }}'
{% if is_incremental() %}
  and dt >= (select max(hour_ts)::date - {{ var("lookback_days") }} from {{ this }})
{% endif %}
group by all
