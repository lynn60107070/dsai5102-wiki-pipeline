-- Dimension: one row per article (grain: article_key = md5(wiki_id:title), titles with spaces).
-- Articles come from the edits and, when enabled, from the pageviews (articles read but never edited).

with from_edits as (

    select article_key, wiki_id, title, namespace, event_ts as seen_ts
    from {{ ref('stg_edits') }}

),

{% if var("pageviews_enabled") %}
from_pageviews as (

    select article_key, wiki_id, replace(title, '_', ' ') as title, null::integer as namespace,
           hour_ts as seen_ts
    from {{ ref('stg_pageviews') }}

),
{% endif %}

all_articles as (

    select * from from_edits
    {% if var("pageviews_enabled") %}
    union all
    select * from from_pageviews
    {% endif %}

)

select
    article_key,
    any_value(wiki_id)      as wiki_id,
    any_value(title)        as title,
    max(namespace)          as namespace,
    min(seen_ts)            as first_seen_ts
from all_articles
group by article_key
