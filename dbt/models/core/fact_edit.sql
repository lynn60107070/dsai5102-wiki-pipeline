{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'delete+insert'
    )
}}
-- Fact: one row per edit or page creation (grain: event_id).
-- Foreign keys: hour_ts -> dim_time, wiki_id -> dim_wiki, article_key -> dim_article,
--               editor_type -> dim_editor_type.
-- Incremental: each run reloads only the last `lookback_days` of date partitions and replaces rows by
-- event_id, so late, re-delivered or re-compacted events never create duplicates.

select
    event_id,
    event_ts,
    hour_ts,
    wiki_id,
    article_key,
    editor_type,
    editor_key,
    is_bot,
    is_new_page,
    is_minor,
    is_revert,
    bytes_changed,
    namespace,
    revision_id,
    recent_change_id
from {{ ref('stg_edits') }}
where dt >= date '{{ var("data_start") }}'
  and {{ before_run_start() }}
{% if is_incremental() %}
  and dt >= (select max(event_ts)::date - {{ var("lookback_days") }} from {{ this }})
{% endif %}
qualify row_number() over (partition by event_id order by event_ts) = 1
