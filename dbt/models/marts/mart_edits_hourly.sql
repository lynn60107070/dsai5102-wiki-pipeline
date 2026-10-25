-- Edits per UTC hour, wiki and editor type: the base for "edits over time, bots vs humans".

select
    event_hour                                   as hour_ts,
    wiki_id,
    editor_type,
    count(*)                                     as edits,
    count(*) filter (where is_new_page)          as new_pages,
    count(*) filter (where is_revert)            as reverts,
    count(distinct editor_key)                   as editors,
    sum(bytes_changed)                           as bytes_changed
from {{ ref('stg_edits') }}
group by all
