{{ config(enabled=var("pageviews_enabled")) }}
-- Reader attention vs edits per article-hour (tile: do attention spikes cause edit surges?).
-- Grain: hour_ts x article_key, for article-hours with >= 1,000 views (kept small enough to serve).
--   prev_24h_avg_views  average views per hour over the previous 24 hours; hours under the 10-view
--                       threshold count as 0, so a sudden jump from obscurity shows as a large ratio
--   has_full_baseline   the pageview data covers the whole previous 24 hours (false on the first day)
--   is_spike            full baseline, >= 1,000 views and at least 5x the previous 24-hour average
--   edits_*             edits to the same article in the same hour and in the 2 hours after
--
-- Performance: the 24-hour window and the edit join only run for "candidate" articles, i.e. ones that
-- reach >= 1,000 views in at least one hour (a few thousand, out of millions in fact_pageviews_hourly).
-- Only candidates can produce output rows, so the result is the same as running over everything; the
-- first version did run over everything and spilled up to ~50 GB to disk per build (27 Oct).

with views_all as (

    select hour_ts, wiki_id, article_key, sum(views) as views
    from {{ ref('fact_pageviews_hourly') }}
    group by all

),

candidates as (

    select distinct article_key from views_all where views >= 1000

),

views as (

    select v.* from views_all as v
    where v.article_key in (select article_key from candidates)

),

with_baseline as (

    select
        *,
        coalesce(sum(views) over (
            partition by article_key order by hour_ts
            range between interval 24 hours preceding and interval 1 hour preceding
        ), 0) / 24.0                                            as prev_24h_avg_views
    from views

),

edits as (

    select article_key, hour_ts,
           count(*)                                    as edits,
           count(*) filter (where editor_type <> 'bot') as human_edits
    from {{ ref('fact_edit') }}
    where article_key in (select article_key from candidates)
    group by all

),

first_hour as (

    select min(hour_ts) as hour_ts from views_all

)

select
    v.hour_ts,
    v.wiki_id,
    v.article_key,
    a.title,
    v.views,
    round(v.prev_24h_avg_views, 1)                                              as prev_24h_avg_views,
    v.hour_ts >= (select hour_ts from first_hour) + interval 24 hours          as has_full_baseline,
    v.hour_ts >= (select hour_ts from first_hour) + interval 24 hours
      and v.views >= 1000 and v.views >= 5 * v.prev_24h_avg_views              as is_spike,
    coalesce(sum(e.edits) filter (where e.hour_ts = v.hour_ts), 0)              as edits_same_hour,
    coalesce(sum(e.edits) filter (where e.hour_ts > v.hour_ts), 0)              as edits_next_2h,
    coalesce(sum(e.human_edits) filter (where e.hour_ts >= v.hour_ts), 0)       as human_edits_0_to_2h
from with_baseline as v
join {{ ref('dim_article') }} as a using (article_key)
left join edits as e
       on e.article_key = v.article_key
      and e.hour_ts between v.hour_ts and v.hour_ts + interval 2 hours
where v.views >= 1000
group by all
