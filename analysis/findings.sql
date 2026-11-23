-- The numbers behind the five findings in the report (section 6), computed from the marts that the
-- warehouse DAG exports to Postgres. Run against the serving database:
--   docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d serving' < analysis/findings.sql
--   PowerShell: Get-Content analysis/findings.sql | docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d serving'
-- Window: 28 Oct - 11 Nov 2026, UTC (in_analysis_window, or the same bounds on hour_ts).
set timezone = 'UTC';

-- Finding 1: bot share, all wikis and Wikipedias, and its lowest and highest day ---------------------
select round(100.0 * sum(bot_edits) / sum(edits), 1)                                   as bot_share_all,
       round(100.0 * sum(bot_edits) filter (where is_wikipedia)
                   / sum(edits) filter (where is_wikipedia), 1)                        as bot_share_wikipedias
from mart_wiki_activity where in_analysis_window;

(select date, round(100.0 * sum(bot_edits) / sum(edits), 1) as bot_share, sum(bot_edits) as bot_edits
 from mart_wiki_activity where in_analysis_window group by date order by bot_share limit 1)
union all
(select date, round(100.0 * sum(bot_edits) / sum(edits), 1), sum(bot_edits)
 from mart_wiki_activity where in_analysis_window group by date order by 2 desc limit 1);

-- Finding 1 (cont.): the highest bot day against a typical (median) day ("about 60% more")
with d as (select date, sum(bot_edits) as bot_edits from mart_wiki_activity where in_analysis_window group by date)
select max(bot_edits)                                                                          as highest_day,
       percentile_cont(0.5) within group (order by bot_edits)                                  as median_day,
       round((max(bot_edits) / percentile_cont(0.5) within group (order by bot_edits))::numeric, 2) as ratio
from d;

-- Finding 2: the busiest Wikipedias, rank by edits against rank by human edits -----------------------
with w as (
    select language_name, sum(edits) as edits, sum(edits - bot_edits) as human_edits,
           round(100.0 * sum(bot_edits) / sum(edits), 1) as bot_share
    from mart_wiki_activity where in_analysis_window and is_wikipedia group by language_name
)
select language_name, edits, bot_share,
       rank() over (order by edits desc)       as rank_by_edits,
       rank() over (order by human_edits desc) as rank_by_human_edits
from w order by edits desc limit 7;

-- Finding 2 (cont.): bot share of the Wikipedias named in the text
select language_name, round(100.0 * sum(bot_edits) / sum(edits), 1) as bot_share
from mart_wiki_activity
where in_analysis_window and is_wikipedia
  and language_name in ('English', 'German', 'Spanish', 'Arabic', 'Chechen')
group by language_name order by language_name;

-- Finding 3: concentration of human Wikipedia edits; Wikidata + Commons against all Wikipedias -------
with h as (
    select language_name, sum(edits - bot_edits) as human_edits
    from mart_wiki_activity where in_analysis_window and is_wikipedia group by language_name
), r as (
    select *, row_number() over (order by human_edits desc) as rk, sum(human_edits) over () as total from h
)
select round(100.0 * sum(human_edits) filter (where rk = 1)  / max(total), 1) as english_share,
       round(100.0 * sum(human_edits) filter (where rk <= 3) / max(total), 1) as top3_share,
       round(100.0 * sum(human_edits) filter (where rk <= 10) / max(total), 1) as top10_share,
       count(*)                                                                 as wikipedias
from r;

select round(100.0 * sum(edits) filter (where wiki_id in ('wikidatawiki', 'commonswiki')) / sum(edits), 1)
           as wikidata_commons_share,
       round(100.0 * sum(edits) filter (where is_wikipedia) / sum(edits), 1) as wikipedias_share
from mart_wiki_activity where in_analysis_window;

-- Finding 4: average edits per hour of day on Wikipedias, humans and bots ----------------------------
with by_hour as (
    select hour_of_day,
           sum(edits) filter (where is_human) / 15.0     as human_per_hour,
           sum(edits) filter (where not is_human) / 15.0 as bot_per_hour
    from mart_edits_hourly where in_analysis_window and is_wikipedia group by hour_of_day
)
select round(max(human_per_hour))                                                 as human_peak,
       (array_agg(hour_of_day order by human_per_hour desc))[1]                   as human_peak_hour,
       round(min(human_per_hour))                                                 as human_low,
       (array_agg(hour_of_day order by human_per_hour))[1]                        as human_low_hour,
       round((max(human_per_hour) / min(human_per_hour))::numeric, 1)             as human_peak_to_low,
       round((100 * sum(human_per_hour) filter (where hour_of_day between 13 and 18)
                  / sum(human_per_hour))::numeric, 1)                             as human_share_13_18,
       round((max(bot_per_hour) / min(bot_per_hour))::numeric, 1)                 as bot_peak_to_low
from by_hour;

-- Finding 5: attention spikes and the edits that follow ----------------------------------------------
-- Spike: >= 1,000 views in the hour and >= 5x the previous 24-hour average (is_spike in the mart).
-- Comparison group: article-hours in the same view range that are not spikes ("steadily popular").
-- Articles only (is_content_page): main pages, Special:Search, Wikipedia: pages etc. get huge views and
-- almost no edits; left in, they made up 1,746 of the 2,326 non-spike hours above 20,000 views and made
-- spikes look 3.1x as edited (fixed 23 Nov, seed wiki_non_article_titles).
with a as (
    select * from mart_attention_vs_edits
    where hour_ts >= '2026-10-28' and hour_ts < '2026-11-12' and is_content_page
)
select count(*) filter (where is_spike)                                               as spikes,
       round(100.0 * avg((edits_same_hour + edits_next_2h = 0)::int) filter (where is_spike), 1)
                                                                                      as pct_spikes_no_edits,
       round(avg(edits_next_2h) filter (where is_spike)::numeric, 2)                 as spike_edits_next_2h,
       round(avg(edits_next_2h) filter (where not is_spike)::numeric, 2)             as other_edits_next_2h
from a;

-- Finding 5 (cont.): share with an edit in the next 2 hours, by views in the hour (the dashboard tile)
select case when views >= 20000 then '3. 20,000+' when views >= 5000 then '2. 5,000-20,000'
            else '1. 1,000-5,000' end                                                as views_in_hour,
       count(*) filter (where is_spike)                                               as spikes,
       count(*) filter (where not is_spike)                                           as others,
       round(100.0 * avg((edits_next_2h > 0)::int) filter (where is_spike), 1)       as pct_spikes_with_edit,
       round(100.0 * avg((edits_next_2h > 0)::int) filter (where not is_spike), 1)   as pct_others_with_edit,
       round(avg(edits_next_2h) filter (where is_spike)::numeric, 2)                 as spike_edits_next_2h,
       round(avg(edits_next_2h) filter (where not is_spike)::numeric, 2)             as other_edits_next_2h,
       count(distinct article_key) filter (where not is_spike)                       as other_articles
from mart_attention_vs_edits
where hour_ts >= '2026-10-28' and hour_ts < '2026-11-12' and has_full_baseline and is_content_page
group by 1 order by 1;

-- Finding 5 (cont.): spikes per wiki, and the largest surge (2026 Krakow shooting, 7 Nov)
select wiki_id, count(*) as spikes
from mart_attention_vs_edits
where is_spike and is_content_page and hour_ts >= '2026-10-28' and hour_ts < '2026-11-12'
group by wiki_id order by spikes desc limit 3;

select hour_ts, views, prev_24h_avg_views, is_spike, edits_same_hour, edits_next_2h
from mart_attention_vs_edits
where title like '2026 Krak%w shooting' and hour_ts < '2026-11-08'
order by hour_ts;
