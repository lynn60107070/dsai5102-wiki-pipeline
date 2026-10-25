{{
    config(
        enabled = var("pageviews_enabled"),
        materialized = 'incremental',
        unique_key = ['hour_ts', 'wiki_id', 'article_key', 'access'],
        incremental_strategy = 'delete+insert'
    )
}}
-- Fact: views per article, hour and access type, for articles with >= 10 views in that hour
-- (grain: hour_ts x wiki_id x article_key x access; see docs/design.md section 7 for the threshold).

select hour_ts, wiki_id, article_key, access, sum(views) as views
from {{ ref('stg_pageviews') }}
where dt >= date '{{ var("data_start") }}'
{% if is_incremental() %}
  and dt >= (select max(hour_ts)::date - {{ var("lookback_days") }} from {{ this }})
{% endif %}
group by all
