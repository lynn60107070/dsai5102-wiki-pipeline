-- Activity per wiki and UTC day (tiles: bot share by wiki, top languages, who reverts).
-- Grain: date x wiki_id. Sum the counts over any date range, then divide: bot share =
-- sum(bot_edits) / sum(edits). Shares are not stored, because shares can't be added up across days.

select
    t.date,
    bool_or(t.in_analysis_window)                                       as in_analysis_window,
    f.wiki_id,
    w.project_family,
    w.is_wikipedia,
    w.language_code,
    w.language_name,
    w.is_arabic,
    count(*)                                                            as edits,
    count(*) filter (where f.editor_type = 'bot')                       as bot_edits,
    count(*) filter (where f.editor_type = 'registered')                as registered_edits,
    count(*) filter (where f.editor_type = 'anonymous')                 as anonymous_edits,
    count(*) filter (where f.is_new_page)                               as new_pages,
    count(*) filter (where f.is_revert)                                 as reverts,
    count(*) filter (where f.is_revert and f.editor_type <> 'bot')      as human_reverts,
    count(distinct f.article_key)                                       as articles_edited,
    count(distinct f.editor_key)                                        as editors
from {{ ref('fact_edit') }} as f
join {{ ref('dim_time') }} as t using (hour_ts)
join {{ ref('dim_wiki') }} as w using (wiki_id)
group by t.date, f.wiki_id, w.project_family, w.is_wikipedia, w.language_code, w.language_name, w.is_arabic
