-- Clean zone of the lake, as written by the stream processor + clean sinks: one row per edit or page
-- creation, already validated and without user names. Still may contain duplicates (at-least-once
-- delivery); fact_edit keeps one row per event_id.
-- `dt` is the Hive partition (UTC date), so filters on it only read the matching folders.

select
    event_id,
    recent_change_id,
    revision_id,
    event_ts,
    date_trunc('hour', event_ts)                            as hour_ts,
    dt,
    wiki_id,
    server_name,
    namespace,
    title,
    md5(wiki_id || ':' || title)                            as article_key,
    is_new_page,
    is_bot,
    is_minor,
    editor_type,
    editor_key,
    bytes_changed,
    is_revert
from read_parquet('s3://{{ var("lake_bucket") }}/clean/edits/*/*/*.parquet', hive_partitioning = true)
