-- Data completeness per UTC hour (tile: is the data trustworthy?). Grain: hour_ts.
-- Every hour of the time spine appears, so hours with no data show up as gaps instead of disappearing.
-- An hour is "partial" when it has under half the median hourly volume (the collector was down for
-- part of it). The current, unfinished hour is left out.

with edits_per_hour as (

    select hour_ts, count(*) as edits
    from {{ ref('fact_edit') }}
    group by hour_ts

),

{% if var("pageviews_enabled") %}
pageview_hours as (

    select distinct hour_ts from {{ ref('fact_wiki_views_hourly') }}

),
{% endif %}

hours as (

    select
        t.hour_ts,
        t.date,
        t.in_analysis_window,
        coalesce(e.edits, 0) as edits
        {% if var("pageviews_enabled") %}
        , p.hour_ts is not null as has_pageviews
        {% endif %}
    from {{ ref('dim_time') }} as t
    left join edits_per_hour as e using (hour_ts)
    {% if var("pageviews_enabled") %}
    left join pageview_hours as p using (hour_ts)
    {% endif %}
    where t.hour_ts < (select max(hour_ts) from {{ ref('dim_time') }})

)

select
    *,
    edits = 0                                                   as is_gap,
    edits > 0 and edits < 0.5 * median(edits) over ()           as is_partial,
    round(edits / nullif(median(edits) over (), 0), 2)          as share_of_median
from hours
