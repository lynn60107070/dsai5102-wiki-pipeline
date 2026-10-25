-- Dimension: one row per UTC hour from the first hour of data up to the latest edit (grain: hour_ts).
-- Generated as a continuous spine, so hours with no data still exist (that is how gaps are found).
-- It starts at the first hour that has any data (the collector started 25 Oct 05:58 UTC), not at
-- midnight, so the hours before the project existed don't count as gaps.

with first_hour as (

    select min(hour_ts) as hour_ts from {{ ref('fact_edit') }}
    {% if var("pageviews_enabled") %}
    union all
    select min(hour_ts) from {{ ref('fact_wiki_views_hourly') }}
    {% endif %}

),

spine as (

    select unnest(generate_series(
        (select min(hour_ts) from first_hour),
        (select max(hour_ts) from {{ ref('fact_edit') }}),
        interval 1 hour
    )) as hour_ts

)

select
    hour_ts,
    hour_ts::date                                                    as date,
    hour(hour_ts)                                                    as hour_of_day,
    dayname(hour_ts)                                                 as day_name,
    isodow(hour_ts)                                                  as day_of_week,
    isodow(hour_ts) in (6, 7)                                        as is_weekend,
    hour_ts >= timestamptz '{{ var("analysis_start") }} 00:00:00+00'
      and hour_ts < timestamptz '{{ var("analysis_end") }} 00:00:00+00' as in_analysis_window
from spine
