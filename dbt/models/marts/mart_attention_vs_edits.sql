{{ config(enabled=var("pageviews_enabled")) }}
-- Reader attention vs edits per article-hour (tile: do attention spikes cause edit surges?).
-- Grain: hour_ts x article_key, for article-hours with >= 1,000 views (kept small enough to serve).
--   prev_24h_avg_views  average views per hour over the previous 24 hours; hours under the 10-view
--                       threshold count as 0, so a sudden jump from obscurity shows as a large ratio
--   has_full_baseline   the pageview data covers the whole previous 24 hours (false on the first day)
--   is_spike            full baseline, >= 1,000 views and at least 5x the previous 24-hour average
--   edits_*             edits to the same article in the same hour and in the 2 hours after
--   is_content_page     false for main pages and pages outside the article namespace (Special:Search,
--                       Wikipedia:..., File:...), per the seed wiki_non_article_titles: they get huge
--                       view counts but are almost never edited, so views-vs-edits comparisons use only
--                       content pages (finding 5)
--
-- Performance: the 24-hour window and the edit join only run for "candidate" articles, i.e. ones that
-- reach >= 1,000 views in at least one hour (a few thousand, out of millions in fact_pageviews_hourly).
-- Only candidates can produce output rows, so the result is the same as running over everything; the
-- first version did run over everything and spilled up to ~50 GB to disk per build (27 Oct).
-- Candidates are found without summing all ~80M fact rows: an article-hour has at most two rows
-- (desktop, mobile), so reaching 1,000 views needs one row with >= 500. Summing every row first came
-- close to the 8 GB spill cap and hit it on 23 Nov.

with maybe as (

    select distinct article_key from {{ ref('fact_pageviews_hourly') }} where views >= 500

),

views as (

    select hour_ts, wiki_id, article_key, sum(views) as views
    from {{ ref('fact_pageviews_hourly') }}
    where article_key in (select article_key from maybe)
    group by all

),

candidates as (

    select distinct article_key from views where views >= 1000

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

non_article_titles as (

    select distinct wiki_id, kind, lower(name) as name from {{ ref('wiki_non_article_titles') }}

),

-- Plain equality joins (namespace names contain no ':'), so this stays a cheap hash join.
content as (

    select a.article_key,
           -- only Wikipedias are in the seed; other wikis in the dumps (e.g. tenwiki) count as non-content
           a.wiki_id in (select wiki_id from non_article_titles)
             and ns.name is null and mp.name is null as is_content_page
    from {{ ref('dim_article') }} as a
    left join non_article_titles as ns
           on ns.kind = 'namespace' and ns.wiki_id = a.wiki_id
          and contains(a.title, ':') and ns.name = lower(split_part(a.title, ':', 1))
    left join non_article_titles as mp
           on mp.kind = 'main_page' and mp.wiki_id = a.wiki_id and mp.name = lower(a.title)
    where a.article_key in (select article_key from candidates)

),

first_hour as (

    select min(hour_ts) as hour_ts from {{ ref('fact_pageviews_hourly') }}

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
    coalesce(sum(e.human_edits) filter (where e.hour_ts >= v.hour_ts), 0)       as human_edits_0_to_2h,
    c.is_content_page
from with_baseline as v
join {{ ref('dim_article') }} as a using (article_key)
join content as c using (article_key)
left join edits as e
       on e.article_key = v.article_key
      and e.hour_ts between v.hour_ts and v.hour_ts + interval 2 hours
where v.views >= 1000
group by all
