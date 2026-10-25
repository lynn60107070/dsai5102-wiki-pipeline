-- Edits per UTC hour, wiki and editor type (tile: edits over time, bots vs humans).
-- Grain: hour_ts x wiki_id x editor_type. Project and language columns let the dashboard filter.

select
    f.hour_ts,
    t.date,
    t.hour_of_day,
    t.in_analysis_window,
    f.wiki_id,
    w.project_family,
    w.is_wikipedia,
    w.language_name,
    f.editor_type,
    e.is_human,
    count(*)                                    as edits,
    count(*) filter (where f.is_new_page)       as new_pages,
    count(*) filter (where f.is_revert)         as reverts,
    count(distinct f.editor_key)                as editors,
    sum(f.bytes_changed)                        as bytes_changed
from {{ ref('fact_edit') }} as f
join {{ ref('dim_time') }} as t using (hour_ts)
join {{ ref('dim_wiki') }} as w using (wiki_id)
join {{ ref('dim_editor_type') }} as e using (editor_type)
group by all
