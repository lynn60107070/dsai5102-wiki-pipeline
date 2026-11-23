{{
    config(
        materialized = 'incremental',
        unique_key = 'article_key',
        incremental_strategy = 'append'
    )
}}
-- Dimension: one row per article (grain: article_key = md5(wiki_id:title), titles with spaces).
-- Articles come from the edits and, when enabled, from the pageviews (articles read but never edited).
--
-- Incremental: each run only reads the last `lookback_days` of partitions and appends articles not seen
-- before, so first_seen_ts never changes and a run doesn't regroup the whole history (the full rebuild
-- grew with every pageview hour, ~4.6M rows a day). After backfilling data older than the lookback, run
-- `dbt build --full-refresh -s dim_article+` once.

{% set since %}
    (select max(first_seen_ts)::date - {{ var("lookback_days") }} from {{ this }})
{% endset %}

with from_edits as (

    select article_key, wiki_id, title, namespace, event_ts as seen_ts
    from {{ ref('stg_edits') }}
    where {{ before_run_start() }}
    {% if is_incremental() %} and dt >= {{ since }} {% endif %}

),

{% if var("pageviews_enabled") %}
from_pageviews as (

    select article_key, wiki_id, replace(title, '_', ' ') as title, null::integer as namespace,
           hour_ts as seen_ts
    from {{ ref('stg_pageviews') }}
    {% if is_incremental() %} where dt >= {{ since }} {% endif %}

),
{% endif %}

all_articles as (

    select * from from_edits
    {% if var("pageviews_enabled") %}
    union all
    select * from from_pageviews
    {% endif %}

),

articles as (

    select
        article_key,
        any_value(wiki_id)      as wiki_id,
        any_value(title)        as title,
        max(namespace)          as namespace,
        min(seen_ts)            as first_seen_ts
    from all_articles
    group by article_key

)

select * from articles
{% if is_incremental() %}
where article_key not in (select article_key from {{ this }})
{% endif %}
